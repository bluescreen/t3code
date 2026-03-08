import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";

export const DESKTOP_WS_URL_FILE_NAME = "desktop-ws-url";

export type ExternalBridgeSource = "env" | "file";

export interface ResolvedExternalBridgeWsUrl {
  readonly source: ExternalBridgeSource;
  readonly wsUrl: string;
  readonly filePath: string;
}

export interface ResolveExternalBridgeWsUrlOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly stateDir?: string;
  readonly readFileSync?: typeof FS.readFileSync;
  readonly onReadError?: (error: Error, filePath: string) => void;
}

export function resolveDesktopStateDir(env: NodeJS.ProcessEnv = process.env): string {
  return env.T3CODE_STATE_DIR?.trim() || Path.join(OS.homedir(), ".t3", "userdata");
}

export function resolveDesktopWsUrlFilePath(
  env: NodeJS.ProcessEnv = process.env,
  stateDir = resolveDesktopStateDir(env),
): string {
  return env.T3CODE_DESKTOP_WS_URL_FILE?.trim() || Path.join(stateDir, DESKTOP_WS_URL_FILE_NAME);
}

export function resolveExternalBridgeWsUrl(
  options: ResolveExternalBridgeWsUrlOptions = {},
): ResolvedExternalBridgeWsUrl | null {
  const env = options.env ?? process.env;
  const stateDir = options.stateDir ?? resolveDesktopStateDir(env);
  const filePath = resolveDesktopWsUrlFilePath(env, stateDir);
  const envWsUrl = env.T3CODE_DESKTOP_WS_URL?.trim();
  if (envWsUrl) {
    return {
      source: "env",
      wsUrl: envWsUrl,
      filePath,
    };
  }

  const readFileSync = options.readFileSync ?? FS.readFileSync;
  try {
    const fileWsUrl = readFileSync(filePath, "utf8").trim();
    if (!fileWsUrl) {
      return null;
    }
    return {
      source: "file",
      wsUrl: fileWsUrl,
      filePath,
    };
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error));
    if ("code" in err && err.code === "ENOENT") {
      return null;
    }
    options.onReadError?.(err, filePath);
    return null;
  }
}
