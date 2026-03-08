import { WebSocketResponse, WsPush, WsResponse } from "@t3tools/contracts";
import { Cause, Schema } from "effect";

type PushListener = (data: unknown) => void;

interface PendingRequest {
  resolve: (result: unknown) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
}

const REQUEST_TIMEOUT_MS = 60_000;
const RECONNECT_DELAYS_MS = [500, 1_000, 2_000, 4_000, 8_000];
const decodeWsResponseFromJson = Schema.decodeUnknownExit(Schema.fromJsonString(WsResponse));
const isWsPushEnvelope = Schema.is(WsPush);
const isWebSocketResponseEnvelope = Schema.is(WebSocketResponse);

interface WsRequestEnvelope {
  id: string;
  body: {
    _tag: string;
    [key: string]: unknown;
  };
}

export class WsTransport {
  private ws: WebSocket | null = null;
  private nextId = 1;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly listeners = new Map<string, Set<PushListener>>();
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private disposed = false;
  private url: string | null = null;
  private readonly explicitUrl?: string;

  constructor(url?: string) {
    this.explicitUrl = url;
    this.connect();
  }

  async request<T = unknown>(method: string, params?: unknown): Promise<T> {
    if (typeof method !== "string" || method.length === 0) {
      throw new Error("Request method is required");
    }
    const id = String(this.nextId++);
    const body = params != null ? { ...params, _tag: method } : { _tag: method };
    const message: WsRequestEnvelope = { id, body };

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        console.error("[ws] request timed out", { method, id, url: this.url });
        reject(new Error(`Request timed out: ${method}`));
      }, REQUEST_TIMEOUT_MS);

      this.pending.set(id, {
        resolve: resolve as (result: unknown) => void,
        reject,
        timeout,
      });

      this.send(message);
    });
  }

  subscribe(channel: string, listener: PushListener): () => void {
    let channelListeners = this.listeners.get(channel);
    if (!channelListeners) {
      channelListeners = new Set();
      this.listeners.set(channel, channelListeners);
    }
    channelListeners.add(listener);

    return () => {
      channelListeners!.delete(listener);
      if (channelListeners!.size === 0) {
        this.listeners.delete(channel);
      }
    };
  }

  dispose() {
    this.disposed = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timeout);
      pending.reject(new Error("Transport disposed"));
    }
    this.pending.clear();
    this.ws?.close();
    this.ws = null;
  }

  private connect() {
    if (this.disposed) return;
    const url = this.resolveConnectUrl();
    if (!url) {
      console.warn("[ws] waiting for desktop bridge websocket url", {
        protocol: window.location.protocol,
      });
      this.scheduleReconnect();
      return;
    }
    this.url = url;

    const ws = new WebSocket(url);

    ws.addEventListener("open", () => {
      this.ws = ws;
      this.reconnectAttempt = 0;
      console.info("[ws] connected", { url: this.url });
    });

    ws.addEventListener("message", (event) => {
      this.handleMessage(event.data);
    });

    ws.addEventListener("close", () => {
      this.ws = null;
      console.warn("[ws] closed", { url: this.url });
      this.scheduleReconnect();
    });

    ws.addEventListener("error", (event) => {
      console.error("[ws] error", { url: this.url, event });
      // close event will fire after error
    });
  }

  private handleMessage(raw: unknown) {
    const exit = decodeWsResponseFromJson(raw);
    if (exit._tag === "Failure") {
      console.warn("Dropped inbound WebSocket envelope", {
        reason: "decode-failed",
        raw,
        issue: Cause.pretty(exit.cause),
      });
      return;
    }
    const message = exit.value;

    // Push event
    if (isWsPushEnvelope(message)) {
      const channelListeners = this.listeners.get(message.channel);
      if (channelListeners) {
        for (const listener of channelListeners) {
          try {
            listener(message.data);
          } catch {
            // Swallow listener errors
          }
        }
      }
      return;
    }

    // Response to a request
    if (!isWebSocketResponseEnvelope(message)) {
      return;
    }

    const pending = this.pending.get(message.id);
    if (!pending) return;

    clearTimeout(pending.timeout);
    this.pending.delete(message.id);

    if (message.error) {
      pending.reject(new Error(message.error.message));
    } else {
      pending.resolve(message.result);
    }
  }

  private send(message: WsRequestEnvelope) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
      return;
    }

    console.warn("[ws] waiting for socket to open before sending request", {
      url: this.url,
      method: message.body._tag,
      id: message.id,
      readyState: this.ws?.readyState ?? null,
    });

    // If not connected, wait for connection
    const waitForOpen = () => {
      const check = setInterval(() => {
        if (this.disposed) {
          clearInterval(check);
          return;
        }
        if (this.ws?.readyState === WebSocket.OPEN) {
          clearInterval(check);
          this.ws.send(JSON.stringify(message));
        }
      }, 50);

      // Give up after timeout (the pending request will time out on its own)
      setTimeout(() => clearInterval(check), REQUEST_TIMEOUT_MS);
    };
    waitForOpen();
  }

  private scheduleReconnect() {
    if (this.disposed) return;

    const delay =
      RECONNECT_DELAYS_MS[Math.min(this.reconnectAttempt, RECONNECT_DELAYS_MS.length - 1)] ??
      RECONNECT_DELAYS_MS[0]!;

    this.reconnectAttempt++;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  private resolveConnectUrl(): string | null {
    if (this.explicitUrl && this.explicitUrl.length > 0) {
      console.info("[ws] using explicit websocket url", { url: this.explicitUrl });
      return this.explicitUrl;
    }

    const bridgeUrl = window.desktopBridge?.getWsUrl?.();
    if (typeof bridgeUrl === "string" && bridgeUrl.length > 0) {
      console.info("[ws] using desktop bridge websocket url", { url: bridgeUrl });
      return bridgeUrl;
    }

    const envUrl = import.meta.env.VITE_WS_URL as string | undefined;
    if (envUrl && envUrl.length > 0) {
      console.info("[ws] using VITE_WS_URL websocket url", { url: envUrl });
      return envUrl;
    }

    if (window.location.protocol === "t3:") {
      console.warn("[ws] no websocket url available yet for desktop scheme", {
        protocol: window.location.protocol,
        hasDesktopBridge: Boolean(window.desktopBridge),
      });
      return null;
    }

    const fallbackUrl = `ws://${window.location.hostname}:${window.location.port}`;
    console.warn("[ws] using same-origin websocket fallback", {
      url: fallbackUrl,
      protocol: window.location.protocol,
    });
    return fallbackUrl;
  }
}
