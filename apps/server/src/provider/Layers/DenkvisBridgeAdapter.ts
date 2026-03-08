import { Effect, Layer, Queue, Stream } from "effect";
import { WebSocket } from "ws";

import type {
  ApprovalRequestId,
  ProviderApprovalDecision,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderSessionStartInput,
  ProviderTurnStartResult,
  ProviderUserInputAnswers,
  ThreadId,
} from "@t3tools/contracts";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import {
  CodexAdapter,
  type CodexAdapterShape,
} from "../Services/CodexAdapter.ts";
import type { ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";

const PROVIDER = "codex" as const;

interface DenkvisBridgeSessionPayload {
  readonly provider: string;
  readonly status: "connecting" | "ready" | "running" | "error" | "closed";
  readonly runtimeMode: "approval-required" | "full-access";
  readonly cwd?: string;
  readonly model?: string;
  readonly threadId: ThreadId;
  readonly resumeCursor?: unknown;
  readonly activeTurnId?: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly lastError?: string;
}

interface DenkvisBridgeTurnPayload {
  readonly threadId: ThreadId;
  readonly turnId: string;
  readonly resumeCursor?: unknown;
}

interface DenkvisBridgeThreadPayload {
  readonly threadId: ThreadId;
  readonly turns: ReadonlyArray<{
    readonly id: string;
    readonly items: ReadonlyArray<unknown>;
  }>;
}

interface DenkvisBridgeAdapterOptions {
  readonly baseUrl?: string;
  readonly token?: string;
}

function bridgeBaseUrl(options?: DenkvisBridgeAdapterOptions): string {
  const value = options?.baseUrl ?? process.env.DENKVIS_T3_BRIDGE_URL;
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    throw new Error("DENKVIS_T3_BRIDGE_URL is required");
  }
  return trimmed.replace(/\/+$/, "");
}

function bridgeToken(options?: DenkvisBridgeAdapterOptions): string {
  const value = options?.token ?? process.env.DENKVIS_T3_BRIDGE_TOKEN;
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    throw new Error("DENKVIS_T3_BRIDGE_TOKEN is required");
  }
  return trimmed;
}

function toProcessError(
  threadId: ThreadId,
  operation: string,
  cause: unknown,
): ProviderAdapterError {
  return new ProviderAdapterProcessError({
    provider: PROVIDER,
    threadId,
    detail: cause instanceof Error ? cause.message : `${operation} failed`,
    cause,
  });
}

function toRequestError(
  threadId: ThreadId,
  method: string,
  cause: unknown,
): ProviderAdapterError {
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: cause instanceof Error ? cause.message : `${method} failed`,
    cause,
  });
}

async function bridgeRequest<T>(
  baseUrl: string,
  token: string,
  path: string,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });

  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const message =
      payload && typeof payload === "object" && "error" in payload
        ? String(
            (payload as { error?: unknown }).error ?? "bridge request failed",
          )
        : `bridge request failed with status ${response.status}`;
    throw new Error(message);
  }
  return payload as T;
}

function toProviderSession(
  payload: DenkvisBridgeSessionPayload,
): ProviderSession {
  return {
    provider: PROVIDER,
    status: payload.status,
    runtimeMode: payload.runtimeMode,
    ...(payload.cwd ? { cwd: payload.cwd } : {}),
    ...(payload.model ? { model: payload.model } : {}),
    threadId: payload.threadId,
    ...(payload.resumeCursor !== undefined
      ? { resumeCursor: payload.resumeCursor }
      : {}),
    ...(payload.activeTurnId
      ? { activeTurnId: payload.activeTurnId as never }
      : {}),
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt,
    ...(payload.lastError ? { lastError: payload.lastError } : {}),
  };
}

function normalizeRuntimeEventProvider(
  event: ProviderRuntimeEvent | { type?: string },
): ProviderRuntimeEvent | { type?: string } {
  if (!event || typeof event !== "object" || !("provider" in event)) {
    return event;
  }
  return {
    ...event,
    provider: PROVIDER,
  } as ProviderRuntimeEvent;
}

function toThreadSnapshot(
  payload: DenkvisBridgeThreadPayload,
): ProviderThreadSnapshot {
  return {
    threadId: payload.threadId,
    turns: payload.turns.map((turn) => ({
      id: turn.id as never,
      items: turn.items,
    })),
  };
}

const makeAdapter = (options?: DenkvisBridgeAdapterOptions) =>
  Effect.gen(function* () {
    const baseUrl = bridgeBaseUrl(options);
    const token = bridgeToken(options);
    const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const serverConfig = yield* Effect.service(ServerConfig);

    const ws = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          new Promise<WebSocket>((resolve, reject) => {
            const wsBase = baseUrl.replace(/^http/i, "ws");
            const connection = new WebSocket(
              `${wsBase}/v1/events?token=${encodeURIComponent(token)}`,
            );
            let settled = false;
            connection.once("open", () => {
              settled = true;
              resolve(connection);
            });
            connection.once("error", (error) => {
              if (settled) {
                return;
              }
              settled = true;
              reject(error);
            });
          }),
        catch: (cause) =>
          toProcessError("bridge" as ThreadId, "events/connect", cause),
      }),
      (connection) =>
        Effect.sync(() => {
          try {
            connection.close();
          } catch {
            // best-effort close
          }
        }).pipe(Effect.zipRight(Queue.shutdown(runtimeEventQueue))),
    );

    ws.on("message", (raw) => {
      try {
        const text = typeof raw === "string" ? raw : raw.toString("utf8");
        const parsed = JSON.parse(text) as
          | ProviderRuntimeEvent
          | { type?: string };
        if ((parsed as { type?: string }).type === "bridge.ready") {
          return;
        }
        void Effect.runPromise(
          Queue.offer(
            runtimeEventQueue,
            normalizeRuntimeEventProvider(parsed) as ProviderRuntimeEvent,
          ),
        );
      } catch {
        // Ignore malformed bridge events and keep the connection alive.
      }
    });

    const startSession: CodexAdapterShape["startSession"] = (
      input: ProviderSessionStartInput,
    ) => {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return Effect.fail(
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          }),
        );
      }

      return Effect.tryPromise({
        try: async () =>
          toProviderSession(
            await bridgeRequest<DenkvisBridgeSessionPayload>(
              baseUrl,
              token,
              "/v1/sessions/start",
              {
                method: "POST",
                body: JSON.stringify({
                  threadId: input.threadId,
                  ...(input.cwd !== undefined ? { cwd: input.cwd } : {}),
                  ...(input.model !== undefined ? { model: input.model } : {}),
                  ...(input.resumeCursor !== undefined
                    ? { resumeCursor: input.resumeCursor }
                    : {}),
                  runtimeMode: input.runtimeMode,
                  ...(input.approvalPolicy !== undefined
                    ? { approvalPolicy: input.approvalPolicy }
                    : {}),
                  ...(input.sandboxMode !== undefined
                    ? { sandboxMode: input.sandboxMode }
                    : {}),
                  ...(input.serviceTier !== undefined
                    ? { serviceTier: input.serviceTier }
                    : {}),
                  ...(input.modelOptions !== undefined
                    ? { modelOptions: input.modelOptions }
                    : {}),
                  ...(input.providerOptions !== undefined
                    ? { providerOptions: input.providerOptions }
                    : {}),
                }),
              },
            ),
          ),
        catch: (cause) => toProcessError(input.threadId, "startSession", cause),
      });
    };

    const sendTurn: CodexAdapterShape["sendTurn"] = (
      input: ProviderSendTurnInput,
    ) => {
      return Effect.tryPromise({
        try: async () => {
          const attachments = (input.attachments ?? []).map((attachment) => {
            const attachmentPath = resolveAttachmentPath({
              stateDir: serverConfig.stateDir,
              attachment,
            });
            if (!attachmentPath) {
              throw new Error(
                `Failed to resolve attachment '${attachment.id}'.`,
              );
            }
            return {
              ...attachment,
              path: attachmentPath,
            };
          });
          const payload = await bridgeRequest<DenkvisBridgeTurnPayload>(
            baseUrl,
            token,
            "/v1/turns/start",
            {
              method: "POST",
              body: JSON.stringify({
                threadId: input.threadId,
                ...(input.input !== undefined ? { input: input.input } : {}),
                ...(input.attachments !== undefined ? { attachments } : {}),
                ...(input.model !== undefined ? { model: input.model } : {}),
                ...(input.serviceTier !== undefined
                  ? { serviceTier: input.serviceTier }
                  : {}),
                ...(input.modelOptions !== undefined
                  ? { modelOptions: input.modelOptions }
                  : {}),
                ...(input.interactionMode !== undefined
                  ? { interactionMode: input.interactionMode }
                  : {}),
              }),
            },
          );
          return {
            threadId: payload.threadId,
            turnId: payload.turnId as never,
            ...(payload.resumeCursor !== undefined
              ? { resumeCursor: payload.resumeCursor }
              : {}),
          } satisfies ProviderTurnStartResult;
        },
        catch: (cause) => toRequestError(input.threadId, "turn/start", cause),
      });
    };

    const interruptTurn: CodexAdapterShape["interruptTurn"] = (threadId) =>
      Effect.tryPromise({
        try: () =>
          bridgeRequest<Record<string, never>>(
            baseUrl,
            token,
            "/v1/turns/interrupt",
            {
              method: "POST",
              body: JSON.stringify({ threadId }),
            },
          ),
        catch: (cause) => toRequestError(threadId, "turn/interrupt", cause),
      }).pipe(Effect.asVoid);

    const respondToRequest: CodexAdapterShape["respondToRequest"] = (
      threadId: ThreadId,
      requestId: ApprovalRequestId,
      decision: ProviderApprovalDecision,
    ) =>
      Effect.tryPromise({
        try: () =>
          bridgeRequest<Record<string, never>>(
            baseUrl,
            token,
            "/v1/requests/respond",
            {
              method: "POST",
              body: JSON.stringify({
                threadId,
                requestId,
                decision,
              }),
            },
          ),
        catch: (cause) => toRequestError(threadId, "request/respond", cause),
      }).pipe(Effect.asVoid);

    const respondToUserInput: CodexAdapterShape["respondToUserInput"] = (
      threadId: ThreadId,
      requestId: ApprovalRequestId,
      answers: ProviderUserInputAnswers,
    ) =>
      Effect.tryPromise({
        try: () =>
          bridgeRequest<Record<string, never>>(
            baseUrl,
            token,
            "/v1/user-input/respond",
            {
              method: "POST",
              body: JSON.stringify({
                threadId,
                requestId,
                answers,
              }),
            },
          ),
        catch: (cause) => toRequestError(threadId, "user-input/respond", cause),
      }).pipe(Effect.asVoid);

    const stopSession: CodexAdapterShape["stopSession"] = (threadId) =>
      Effect.tryPromise({
        try: () =>
          bridgeRequest<Record<string, never>>(
            baseUrl,
            token,
            "/v1/sessions/stop",
            {
              method: "POST",
              body: JSON.stringify({ threadId }),
            },
          ),
        catch: (cause) => toRequestError(threadId, "session/stop", cause),
      }).pipe(Effect.asVoid);

    const listSessions: CodexAdapterShape["listSessions"] = () =>
      Effect.tryPromise({
        try: async () =>
          (
            await bridgeRequest<ReadonlyArray<DenkvisBridgeSessionPayload>>(
              baseUrl,
              token,
              "/v1/sessions",
            )
          ).map(toProviderSession),
        catch: () => [] as ReadonlyArray<ProviderSession>,
      });

    const hasSession: CodexAdapterShape["hasSession"] = (threadId) =>
      listSessions().pipe(
        Effect.map((sessions) =>
          sessions.some((session) => session.threadId === threadId),
        ),
      );

    const readThread: CodexAdapterShape["readThread"] = (threadId) =>
      Effect.tryPromise({
        try: async () =>
          toThreadSnapshot(
            await bridgeRequest<DenkvisBridgeThreadPayload>(
              baseUrl,
              token,
              `/v1/threads/read?threadId=${encodeURIComponent(threadId)}`,
            ),
          ),
        catch: (cause) => toRequestError(threadId, "thread/read", cause),
      });

    const rollbackThread: CodexAdapterShape["rollbackThread"] = (
      threadId,
      numTurns,
    ) =>
      Effect.tryPromise({
        try: async () =>
          toThreadSnapshot(
            await bridgeRequest<DenkvisBridgeThreadPayload>(
              baseUrl,
              token,
              "/v1/threads/rollback",
              {
                method: "POST",
                body: JSON.stringify({
                  threadId,
                  numTurns,
                }),
              },
            ),
          ),
        catch: (cause) => toRequestError(threadId, "thread/rollback", cause),
      });

    const stopAll: CodexAdapterShape["stopAll"] = () =>
      listSessions().pipe(
        Effect.flatMap((sessions) =>
          Effect.forEach(sessions, (session) => stopSession(session.threadId), {
            concurrency: 1,
            discard: true,
          }),
        ),
      );

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "restart-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromQueue(runtimeEventQueue),
    } satisfies CodexAdapterShape;
  });

export function makeDenkvisBridgeCodexAdapterLive(
  options?: DenkvisBridgeAdapterOptions,
) {
  return Layer.effect(CodexAdapter, makeAdapter(options));
}
