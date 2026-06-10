import { execFile, spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { chmod, mkdir, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { homedir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";

import { syncWorkBuddySessionIndex } from "./workbuddy-session-index";

const DEFAULT_SESSION_ID = "53aihub-workbuddy-shared";
const DEFAULT_HISTORY_SCOPE = "all";
const DEFAULT_WORKBUDDY_CLI_PATH =
  "/Applications/WorkBuddy.app/Contents/Resources/app.asar.unpacked/cli/bin/codebuddy";
const SESSION_ACTIVATION_RETRY_MS = 5_000;

export type WorkBuddySupervisorConfig = {
  hubWsUrl: string;
  hubBotId: string;
  hubSecret: string;
  hubAccessPolicy: "open" | "allowlist";
  hubAllowFrom: string;
  sendThinkingMessage: boolean;
  workbuddyHome: string;
  workspaceDir: string;
  sessionId: string;
  historyScope: "all" | "channel";
  codebuddyCliPath: string;
  channelEntryPath: string;
};

export type WorkBuddyWorkerCommand = {
  command: string;
  args: string[];
  cwd: string;
  env: NodeJS.ProcessEnv;
};

export type WorkBuddySupervisorInput = {
  config: WorkBuddySupervisorConfig;
  spawnProcess?: typeof spawn;
  discoverWorkerPorts?: (pid: number) => Promise<number[]>;
  logger?: {
    info?(message: string): void;
    warn?(message: string): void;
    error?(message: string): void;
  };
};

export function loadWorkBuddySupervisorConfig(env: NodeJS.ProcessEnv = process.env): WorkBuddySupervisorConfig {
  const workbuddyHome = readEnv(env, "HUB53AI_WORKBUDDY_HOME", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_HOME") ||
    join(homedir(), ".workbuddy");
  const channelEntryPath =
    readEnv(env, "HUB53AI_CHANNEL_ENTRY_PATH", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_CHANNEL_ENTRY_PATH") ||
    resolve(dirname(process.argv[1] || process.cwd()), "codebuddy-channel.cjs");
  const sessionId =
    readEnv(env, "HUB53AI_WORKBUDDY_SESSION_ID", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_SESSION_ID") ||
    DEFAULT_SESSION_ID;
  return {
    hubWsUrl: readEnv(env, "HUB53AI_WS_URL", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WS_URL"),
    hubBotId: readEnv(env, "HUB53AI_BOT_ID", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_BOT_ID"),
    hubSecret: readEnv(env, "HUB53AI_SECRET", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_SECRET"),
    hubAccessPolicy: parseAccessPolicy(readEnv(env, "HUB53AI_ACCESS_POLICY", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_ACCESS_POLICY")),
    hubAllowFrom: readEnv(env, "HUB53AI_ALLOW_FROM", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_ALLOW_FROM"),
    sendThinkingMessage: parseOptionalBoolean(
      readEnv(env, "HUB53AI_SEND_THINKING_MESSAGE", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_SEND_THINKING_MESSAGE"),
      true
    ),
    workbuddyHome,
    workspaceDir:
      readEnv(env, "HUB53AI_WORKBUDDY_WORKSPACE", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_WORKSPACE") ||
      join(workbuddyHome, "channels", "53aihub-workspace"),
    sessionId,
    historyScope: parseHistoryScope(
      readEnv(env, "HUB53AI_WORKBUDDY_HISTORY_SCOPE", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_HISTORY_SCOPE") ||
        DEFAULT_HISTORY_SCOPE
    ),
    codebuddyCliPath: resolveCodeBuddyCliPath(
      readEnv(env, "HUB53AI_WORKBUDDY_CLI_PATH", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_CLI_PATH")
    ),
    channelEntryPath
  };
}

export function buildWorkBuddyWorkerMcpConfig(config: WorkBuddySupervisorConfig) {
  return {
    mcpServers: {
      "53aihub-channel": {
        command: "node",
        args: [config.channelEntryPath],
        env: {
          HUB53AI_WS_URL: config.hubWsUrl,
          HUB53AI_BOT_ID: config.hubBotId,
          HUB53AI_SECRET: config.hubSecret,
          HUB53AI_ACCESS_POLICY: config.hubAccessPolicy,
          HUB53AI_ALLOW_FROM: config.hubAllowFrom,
          HUB53AI_SEND_THINKING_MESSAGE: String(config.sendThinkingMessage),
          HUB53AI_WORKBUDDY_HOME: config.workbuddyHome,
          HUB53AI_WORKBUDDY_HISTORY_SCOPE: config.historyScope,
          HUB53AI_WORKBUDDY_SESSION_ID: config.sessionId,
          HUB53AI_STATE_DIR: join(config.workbuddyHome, "channels", "53aihub")
        }
      }
    }
  };
}

export function buildWorkBuddyWorkerCommand(config: WorkBuddySupervisorConfig): WorkBuddyWorkerCommand {
  return {
    command: config.codebuddyCliPath,
    cwd: config.workspaceDir,
    env: {
      ...process.env,
      HUB53AI_WORKBUDDY_HOME: config.workbuddyHome,
      HUB53AI_WORKBUDDY_HISTORY_SCOPE: config.historyScope,
      HUB53AI_WORKBUDDY_SESSION_ID: config.sessionId
    },
    args: [
      "--serve",
      "--session-id",
      config.sessionId,
      "--channels",
      "server:53aihub-channel",
      "--dangerously-load-development-channels",
      "server:53aihub-channel",
      "--permission-mode",
      "bypassPermissions",
      "--permission-mode-before-plan",
      "bypassPermissions",
      "--tools",
      "ToolSearch,DeferExecuteTool",
      "--allowedTools",
      "ToolSearch,DeferExecuteTool,mcp__53aihub-channel__reply",
      "--mcp-config",
      resolveWorkBuddyWorkerMcpConfigPath(config),
      "--strict-mcp-config"
    ]
  };
}

export function resolveWorkBuddyWorkerMcpConfigPath(config: WorkBuddySupervisorConfig): string {
  return join(config.workbuddyHome, "channels", "53aihub-worker.mcp.json");
}

export async function writeWorkBuddyWorkerMcpConfig(config: WorkBuddySupervisorConfig): Promise<string> {
  const path = resolveWorkBuddyWorkerMcpConfigPath(config);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(buildWorkBuddyWorkerMcpConfig(config), null, 2)}\n`, { mode: 0o600 });
  await chmod(path, 0o600).catch(() => {});
  return path;
}

export function createWorkBuddySupervisor(input: WorkBuddySupervisorInput) {
  const spawnProcess = input.spawnProcess ?? spawn;
  let child: ChildProcess | undefined;
  let restartTimer: NodeJS.Timeout | undefined;
  let stopping = false;
  let lastStartedAt: string | undefined;
  let lastExitAt: string | undefined;
  let lastError: string | undefined;
  let workerPort: number | undefined;
  let lastSessionActivationAt: string | undefined;
  let activeAcpSessionId: string | undefined;
  let sessionActive = false;
  let sessionActivationTimer: NodeJS.Timeout | undefined;
  let activationInFlight = false;
  let portDiscoveryTimer: NodeJS.Timeout | undefined;

  async function start() {
    stopping = false;
    await mkdir(input.config.workspaceDir, { recursive: true });
    await writeWorkBuddyWorkerMcpConfig(input.config);
    ensureWorker();
  }

  async function stop() {
    stopping = true;
    if (restartTimer) {
      clearTimeout(restartTimer);
      restartTimer = undefined;
    }
    clearSessionActivationRetry();
    clearPortDiscoveryRetry();
    if (child && !child.killed) {
      child.kill();
    }
  }

  function restart() {
    if (child && !child.killed) {
      child.kill();
    }
    child = undefined;
    void writeWorkBuddyWorkerMcpConfig(input.config)
      .catch((error) => {
        lastError = error instanceof Error ? error.message : String(error);
      })
      .finally(ensureWorker);
  }

  function status() {
    return {
      running: Boolean(child && !child.killed),
      pid: child?.pid,
      sessionId: input.config.sessionId,
      workspaceDir: input.config.workspaceDir,
      channelEntryPath: input.config.channelEntryPath,
      codebuddyCliPath: input.config.codebuddyCliPath,
      workerPort,
      sessionActive,
      activeAcpSessionId,
      lastSessionActivationAt,
      lastStartedAt,
      lastExitAt,
      lastError
    };
  }

  function ensureWorker() {
    if (stopping || (child && !child.killed)) {
      return;
    }
    const command = buildWorkBuddyWorkerCommand(input.config);
    lastStartedAt = new Date().toISOString();
    input.logger?.info?.(`[53aihub-workbuddy] starting channel worker: ${command.command}`);
    child = spawnProcess(command.command, command.args, {
      cwd: command.cwd,
      env: command.env,
      stdio: ["ignore", "pipe", "pipe"]
    });
    child.stdout?.on("data", (chunk) => {
      const text = String(chunk);
      input.logger?.info?.(`[53aihub-workbuddy:worker] ${text.trimEnd()}`);
      const port = readServePort(text);
      if (port) {
        workerPort = port;
        lastError = undefined;
        scheduleSessionActivation(0);
      }
    });
    schedulePortDiscovery();
    child.stderr?.on("data", (chunk) => {
      input.logger?.warn?.(`[53aihub-workbuddy:worker] ${String(chunk).trimEnd()}`);
    });
    child.on("error", (error) => {
      lastError = error.message;
      input.logger?.error?.(`[53aihub-workbuddy] worker error: ${error.message}`);
    });
    child.on("exit", (code, signal) => {
      lastExitAt = new Date().toISOString();
      lastError = code === 0 ? undefined : `worker exited: ${code ?? "null"} ${signal ?? ""}`.trim();
      child = undefined;
      workerPort = undefined;
      activeAcpSessionId = undefined;
      sessionActive = false;
      clearSessionActivationRetry();
      clearPortDiscoveryRetry();
      if (!stopping) {
        restartTimer = setTimeout(ensureWorker, 2_000);
      }
    });
  }

  function schedulePortDiscovery(delayMs = 800) {
    if (stopping || workerPort || !child?.pid || portDiscoveryTimer) {
      return;
    }
    portDiscoveryTimer = setTimeout(() => {
      portDiscoveryTimer = undefined;
      const pid = child?.pid;
      if (!pid || stopping || workerPort) {
        return;
      }
      void (input.discoverWorkerPorts ?? discoverListeningPortsForPid)(pid)
        .then((ports) => {
          const port = ports[0];
          if (port) {
            workerPort = port;
            lastError = undefined;
            input.logger?.info?.(`[53aihub-workbuddy] discovered worker ACP port: ${port}`);
            scheduleSessionActivation(0);
          } else {
            schedulePortDiscovery();
          }
        })
        .catch((error) => {
          lastError = error instanceof Error ? error.message : String(error);
          schedulePortDiscovery();
        });
    }, delayMs);
  }

  function scheduleSessionActivation(delayMs = SESSION_ACTIVATION_RETRY_MS) {
    if (stopping || !workerPort || sessionActive || activationInFlight) {
      return;
    }
    clearSessionActivationRetry();
    sessionActivationTimer = setTimeout(() => {
      sessionActivationTimer = undefined;
      if (!workerPort || stopping || sessionActive) {
        return;
      }
      activationInFlight = true;
      void activateSharedSession(workerPort)
        .catch((error) => {
          lastError = error instanceof Error ? error.message : String(error);
          sessionActive = false;
          input.logger?.error?.(`[53aihub-workbuddy] failed to activate shared session: ${lastError}`);
        })
        .finally(() => {
          activationInFlight = false;
          if (!sessionActive) {
            scheduleSessionActivation();
          }
        });
    }, delayMs);
  }

  function clearSessionActivationRetry() {
    if (sessionActivationTimer) {
      clearTimeout(sessionActivationTimer);
      sessionActivationTimer = undefined;
    }
  }

  function clearPortDiscoveryRetry() {
    if (portDiscoveryTimer) {
      clearTimeout(portDiscoveryTimer);
      portDiscoveryTimer = undefined;
    }
  }

  async function activateSharedSession(port: number) {
    input.logger?.info?.(`[53aihub-workbuddy] activating shared session ${input.config.sessionId} on port ${port}`);
    const connect = await acpJson(port, "/api/v1/acp/connect", undefined);
    const connectionId = readStringProperty(connect, "connectionId");
    if (!connectionId) {
      throw new Error("ACP connect did not return connectionId");
    }
    await acpSse(port, connectionId, {
      jsonrpc: "2.0",
      id: "initialize",
      method: "initialize",
      params: {
        protocolVersion: 1,
        capabilities: {},
        clientInfo: {
          name: "53aihub-workbuddy-supervisor",
          version: "0.1.13"
        }
      }
    });
    await acpSse(port, connectionId, {
      jsonrpc: "2.0",
      id: "new-session",
      method: "session/new",
      params: {
        workingDirectory: input.config.workspaceDir,
        cwd: input.config.workspaceDir,
        mcpServers: [],
        _meta: {
          "codebuddy.ai/continue": true,
          "53aihub.ai/sharedSessionId": input.config.sessionId
        }
      }
    }).then((result) => {
      activeAcpSessionId = readStringProperty(result, "sessionId") || input.config.sessionId;
    });
    sessionActive = true;
    lastError = undefined;
    lastSessionActivationAt = new Date().toISOString();
    await syncWorkBuddySessionIndex({
      workbuddyHome: input.config.workbuddyHome,
      sessionId: input.config.sessionId,
      cwd: input.config.workspaceDir,
      title: "53AIHub WorkBuddy",
      status: "completed"
    }).catch((error) => {
      input.logger?.warn?.(
        `[53aihub-workbuddy] failed to sync WorkBuddy session index: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    });
    input.logger?.info?.(
      `[53aihub-workbuddy] shared session activated: ${activeAcpSessionId ?? input.config.sessionId}`
    );
  }

  return {
    start,
    stop,
    restart,
    status
  };
}

export async function startWorkBuddySupervisorServer(input?: {
  config?: WorkBuddySupervisorConfig;
  logger?: WorkBuddySupervisorInput["logger"];
}) {
  const config = input?.config ?? loadWorkBuddySupervisorConfig();
  validateSupervisorConfig(config);
  const logger = input?.logger ?? stderrLogger;
  const supervisor = createWorkBuddySupervisor({ config, logger });
  const mcp = new Server(
    { name: "53aihub-workbuddy", version: "0.1.13" },
    {
      capabilities: { tools: {} },
      instructions: "Keeps the shared 53AIHub WorkBuddy channel worker running."
    }
  );

  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      {
        name: "status",
        description: "Inspect the 53AIHub WorkBuddy channel worker status.",
        inputSchema: { type: "object", properties: {} }
      },
      {
        name: "restart_worker",
        description: "Restart the 53AIHub WorkBuddy channel worker.",
        inputSchema: { type: "object", properties: {} }
      }
    ]
  }));

  mcp.setRequestHandler(CallToolRequestSchema, async (request) => {
    if (request.params.name === "restart_worker") {
      supervisor.restart();
    } else if (request.params.name !== "status") {
      throw new Error(`unknown tool: ${request.params.name}`);
    }
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(supervisor.status(), null, 2)
        }
      ]
    };
  });

  await supervisor.start();
  await mcp.connect(new StdioServerTransport());

  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    await supervisor.stop();
    await mcp.close();
  };
  const shutdownAndExit = () => {
    void shutdown().finally(() => process.exit(0));
  };
  process.once("SIGINT", () => {
    shutdownAndExit();
  });
  process.once("SIGTERM", () => {
    shutdownAndExit();
  });
  process.stdin.once("end", shutdownAndExit);
  process.stdin.once("close", shutdownAndExit);

  return {
    mcp,
    supervisor,
    stop: shutdown
  };
}

function validateSupervisorConfig(config: WorkBuddySupervisorConfig) {
  const missing = [
    ["HUB53AI_WS_URL", config.hubWsUrl],
    ["HUB53AI_BOT_ID", config.hubBotId],
    ["HUB53AI_SECRET", config.hubSecret],
    ["HUB53AI_CHANNEL_ENTRY_PATH", config.channelEntryPath]
  ].filter(([, value]) => !value);
  if (missing.length) {
    throw new Error(`missing WorkBuddy supervisor config: ${missing.map(([key]) => key).join(", ")}`);
  }
}

function readServePort(output: string): number | undefined {
  const match =
    output.match(/\bserve\s+(\d{2,5})\b/) ??
    output.match(/\bEndpoint\s+https?:\/\/(?:127\.0\.0\.1|localhost|\[[^\]]+\]|[^:\s/]+):(\d{2,5})\b/i);
  if (!match) {
    return undefined;
  }
  const port = Number(match[1]);
  return Number.isInteger(port) && port > 0 && port < 65536 ? port : undefined;
}

export function parseLsofListenPorts(output: string): number[] {
  const ports = new Set<number>();
  for (const line of output.split(/\r?\n/)) {
    if (!line.includes("(LISTEN)")) {
      continue;
    }
    const match = line.match(/:(\d{2,5})\s+\(LISTEN\)/);
    if (!match) {
      continue;
    }
    const port = Number(match[1]);
    if (Number.isInteger(port) && port > 0 && port < 65536) {
      ports.add(port);
    }
  }
  return [...ports];
}

function discoverListeningPortsForPid(pid: number): Promise<number[]> {
  return new Promise((resolve, reject) => {
    execFile("lsof", ["-nP", "-a", "-p", String(pid), "-iTCP", "-sTCP:LISTEN"], (error, stdout) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(parseLsofListenPorts(stdout));
    });
  });
}

async function acpJson(port: number, path: string, body: unknown): Promise<Record<string, unknown>> {
  const raw = await httpPost(port, path, body, {
    Accept: "application/json"
  });
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    throw new Error(`invalid ACP JSON response: ${raw.slice(0, 200)}`);
  }
}

async function acpSse(port: number, connectionId: string, body: unknown): Promise<Record<string, unknown>> {
  const raw = await httpPost(port, "/api/v1/acp", body, {
    Accept: "application/json, text/event-stream",
    "Content-Type": "application/json",
    "acp-connection-id": connectionId
  });
  const messages = parseSseMessages(raw);
  const response = messages.find((message) => message.id === (body as { id?: unknown }).id);
  if (!response) {
    throw new Error(`ACP response not found for request ${(body as { id?: unknown }).id ?? "unknown"}`);
  }
  if (response.error) {
    const error = response.error as Record<string, unknown>;
    throw new Error(typeof error.message === "string" ? error.message : JSON.stringify(error));
  }
  return toRecord(response.result);
}

function httpPost(
  port: number,
  path: string,
  body: unknown,
  headers: Record<string, string>
): Promise<string> {
  const payload = body === undefined ? "" : JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = request(
      {
        hostname: "127.0.0.1",
        port,
        path,
        method: "POST",
        headers: {
          "x-codebuddy-request": "1",
          ...(payload ? { "Content-Type": "application/json", "Content-Length": String(Buffer.byteLength(payload)) } : {}),
          ...headers
        },
        timeout: 15_000
      },
      (res) => {
        let raw = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          raw += chunk;
        });
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve(raw);
          } else {
            reject(new Error(`ACP HTTP ${res.statusCode}: ${raw.slice(0, 200)}`));
          }
        });
      }
    );
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy(new Error(`ACP request timed out: ${path}`));
    });
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

function parseSseMessages(raw: string): Array<Record<string, unknown>> {
  const messages: Array<Record<string, unknown>> = [];
  for (const line of raw.split(/\r?\n/)) {
    if (!line.startsWith("data:")) {
      continue;
    }
    const data = line.slice(5).trim();
    if (!data) {
      continue;
    }
    try {
      const parsed = JSON.parse(data);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        messages.push(parsed as Record<string, unknown>);
      }
    } catch {
      // Ignore non-JSON SSE frames such as keepalive markers.
    }
  }
  return messages;
}

function readStringProperty(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function resolveCodeBuddyCliPath(explicitPath: string): string {
  if (explicitPath) {
    return explicitPath;
  }
  if (existsSync(DEFAULT_WORKBUDDY_CLI_PATH)) {
    return DEFAULT_WORKBUDDY_CLI_PATH;
  }
  return "codebuddy";
}

function readEnv(env: NodeJS.ProcessEnv, ...keys: string[]): string {
  for (const key of keys) {
    const value = env[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function parseAccessPolicy(value: string): WorkBuddySupervisorConfig["hubAccessPolicy"] {
  return value === "allowlist" ? "allowlist" : "open";
}

function parseHistoryScope(value: string): WorkBuddySupervisorConfig["historyScope"] {
  return value === "channel" ? "channel" : "all";
}

function parseOptionalBoolean(value: string, fallback: boolean): boolean {
  if (!value) {
    return fallback;
  }
  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

const stderrLogger = {
  info(message: string) {
    process.stderr.write(`${message}\n`);
  },
  warn(message: string) {
    process.stderr.write(`${message}\n`);
  },
  error(message: string) {
    process.stderr.write(`${message}\n`);
  }
};

if (/^workbuddy-supervisor\.(?:cjs|js|ts)$/.test(basename(process.argv[1] ?? ""))) {
  startWorkBuddySupervisorServer().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[53aihub-workbuddy] fatal: ${message}\n`);
    process.exit(1);
  });
}
