import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { describe, expect, it } from "vitest";

import {
  DESKTOP_WS_URL_FILE_NAME,
  redactBridgeWsUrlForLog,
  resolveDesktopStateDir,
  resolveDesktopWsUrlFilePath,
  resolveExternalBridgeWsUrl,
} from "./externalBridge";

describe("externalBridge", () => {
  it("reads the websocket URL from the state file", () => {
    const stateDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "t3code-desktop-state-"),
    );
    const filePath = path.join(stateDir, DESKTOP_WS_URL_FILE_NAME);
    fs.writeFileSync(filePath, "ws://127.0.0.1:4773/?token=file-secret\n");

    const resolved = resolveExternalBridgeWsUrl({
      env: {
        T3CODE_STATE_DIR: stateDir,
      },
    });

    expect(resolved).toEqual({
      source: "file",
      wsUrl: "ws://127.0.0.1:4773/?token=file-secret",
      filePath,
    });
  });

  it("returns null when the websocket file is empty", () => {
    const stateDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "t3code-desktop-state-"),
    );
    fs.writeFileSync(path.join(stateDir, DESKTOP_WS_URL_FILE_NAME), " \n");

    expect(
      resolveExternalBridgeWsUrl({
        env: {
          T3CODE_STATE_DIR: stateDir,
        },
      }),
    ).toBeNull();
  });

  it("returns null and reports non-ENOENT file read errors", () => {
    const errors: string[] = [];

    const resolved = resolveExternalBridgeWsUrl({
      env: {
        T3CODE_STATE_DIR: "/tmp/state",
      },
      readFileSync: () => {
        const error = new Error("permission denied");
        Object.assign(error, { code: "EACCES" });
        throw error;
      },
      onReadError: (error, filePath) => {
        errors.push(`${filePath}: ${error.message}`);
      },
    });

    expect(resolved).toBeNull();
    expect(errors).toEqual(["/tmp/state/desktop-ws-url: permission denied"]);
  });

  it("resolves the shared state helpers consistently", () => {
    const env = {
      T3CODE_STATE_DIR: "/tmp/custom-state",
    };

    expect(resolveDesktopStateDir(env)).toBe("/tmp/custom-state");
    expect(resolveDesktopWsUrlFilePath(env)).toBe(
      "/tmp/custom-state/desktop-ws-url",
    );
  });

  it("redacts tokens when formatting websocket URLs for logs", () => {
    expect(
      redactBridgeWsUrlForLog("ws://127.0.0.1:3773/?token=secret&foo=bar"),
    ).toBe("ws://127.0.0.1:3773/?token=%3Credacted%3E&foo=bar");
  });

  it("falls back to regex redaction for invalid URLs", () => {
    expect(redactBridgeWsUrlForLog("not-a-url?token=secret")).toContain(
      "token=<redacted>",
    );
  });
});
