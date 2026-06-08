import { EventEmitter } from "node:events";
import { createServer } from "node:http";

import { describe, expect, it } from "vitest";

import {
  buildWorkBuddyWorkerCommand,
  buildWorkBuddyWorkerMcpConfig,
  createWorkBuddySupervisor,
  parseLsofListenPorts,
  resolveWorkBuddyWorkerMcpConfigPath,
  type WorkBuddySupervisorConfig
} from "../src/workbuddy-supervisor";

describe("WorkBuddy supervisor", () => {
  it("builds a shared channel worker command with channels enabled", () => {
    const command = buildWorkBuddyWorkerCommand(buildConfig());

    expect(command.command).toBe("/Applications/WorkBuddy.app/Contents/Resources/app.asar.unpacked/cli/bin/codebuddy");
    expect(command.cwd).toBe("/tmp/workbuddy/channels/53aihub-workspace");
    expect(command.args).toEqual(
      expect.arrayContaining([
        "--serve",
        "--session-id",
        "53aihub-workbuddy-shared",
        "--channels",
        "server:53aihub-channel",
        "--dangerously-load-development-channels",
        "server:53aihub-channel",
        "--strict-mcp-config"
      ])
    );
    expect(command.args[command.args.indexOf("--mcp-config") + 1]).toBe(
      "/tmp/workbuddy/channels/53aihub-worker.mcp.json"
    );
    expect(command.args.join(" ")).not.toContain("ws://hub.example/ws");
    expect(command.args.join(" ")).not.toContain("hub-secret");
    expect(resolveWorkBuddyWorkerMcpConfigPath(buildConfig())).toBe("/tmp/workbuddy/channels/53aihub-worker.mcp.json");
  });

  it("passes the channel server through the worker MCP config", () => {
    expect(buildWorkBuddyWorkerMcpConfig(buildConfig())).toMatchObject({
      mcpServers: {
        "53aihub-channel": {
          command: "node",
          args: ["/tmp/plugin/dist/codebuddy-channel.cjs"],
          env: {
            HUB53AI_WS_URL: "ws://hub.example/ws",
            HUB53AI_BOT_ID: "hub-bot",
            HUB53AI_SECRET: "hub-secret",
            HUB53AI_WORKBUDDY_HISTORY_SCOPE: "all",
            HUB53AI_WORKBUDDY_SESSION_ID: "53aihub-workbuddy-shared"
          }
        }
      }
    });
  });

  it("starts only the CodeBuddy worker process", async () => {
    const spawned: Array<{ command: string; args: string[] }> = [];
    const fakeChild = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
      killed: false,
      pid: 12345,
      kill() {
        this.killed = true;
      }
    });
    const supervisor = createWorkBuddySupervisor({
      config: buildConfig(),
      spawnProcess: ((command: string, args: string[]) => {
        spawned.push({ command, args });
        return fakeChild;
      }) as never
    });

    await supervisor.start();

    expect(spawned).toHaveLength(1);
    expect(spawned[0].command).toContain("codebuddy");
    expect(spawned[0].args).toContain("--channels");
    expect(spawned[0].args).not.toContain("ws://hub.example/ws");

    await supervisor.stop();
  });

  it("activates the shared ACP session after the worker reports its serve port", async () => {
    const acp = await createFakeAcpServer();
    const requests: Array<{ path: string; body: any; headers: Record<string, string | string[] | undefined> }> = [];
    acp.onRequest((request) => requests.push(request));

    const fakeChild = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
      killed: false,
      pid: 12346,
      kill() {
        this.killed = true;
      }
    });
    const supervisor = createWorkBuddySupervisor({
      config: buildConfig(),
      spawnProcess: (() => fakeChild) as never
    });

    try {
      await supervisor.start();
      fakeChild.stdout.emit("data", `serve ${acp.port}\n`);

      await waitFor(() => {
        expect(supervisor.status()).toMatchObject({
          workerPort: acp.port,
          sessionActive: true,
          activeAcpSessionId: "created-session-1"
        });
      });
      expect(requests.map((request) => request.path)).toEqual([
        "/api/v1/acp/connect",
        "/api/v1/acp",
        "/api/v1/acp"
      ]);
      expect(requests[2].body).toMatchObject({
        method: "session/new",
        params: {
          workingDirectory: "/tmp/workbuddy/channels/53aihub-workspace",
          cwd: "/tmp/workbuddy/channels/53aihub-workspace",
          mcpServers: [],
          _meta: {
            "codebuddy.ai/continue": true,
            "53aihub.ai/sharedSessionId": "53aihub-workbuddy-shared"
          }
        }
      });
    } finally {
      await supervisor.stop();
      await acp.close();
    }
  });

  it("discovers the worker ACP port when stdout does not include a serve line", async () => {
    const acp = await createFakeAcpServer();
    const fakeChild = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
      killed: false,
      pid: 12347,
      kill() {
        this.killed = true;
      }
    });
    const supervisor = createWorkBuddySupervisor({
      config: buildConfig(),
      spawnProcess: (() => fakeChild) as never,
      discoverWorkerPorts: async () => [acp.port]
    });

    try {
      await supervisor.start();

      await waitFor(() => {
        expect(supervisor.status()).toMatchObject({
          workerPort: acp.port,
          sessionActive: true,
          activeAcpSessionId: "created-session-1"
        });
      });
    } finally {
      await supervisor.stop();
      await acp.close();
    }
  });

  it("parses lsof listen ports", () => {
    expect(
      parseLsofListenPorts([
        "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME",
        "node 123 user 15u IPv4 0x0 0t0 TCP 127.0.0.1:57209 (LISTEN)",
        "node 123 user 16u IPv6 0x0 0t0 TCP [::1]:57210 (LISTEN)"
      ].join("\n"))
    ).toEqual([57209, 57210]);
  });
});

function buildConfig(): WorkBuddySupervisorConfig {
  return {
    hubWsUrl: "ws://hub.example/ws",
    hubBotId: "hub-bot",
    hubSecret: "hub-secret",
    hubAccessPolicy: "open",
    hubAllowFrom: "",
    sendThinkingMessage: true,
    workbuddyHome: "/tmp/workbuddy",
    workspaceDir: "/tmp/workbuddy/channels/53aihub-workspace",
    sessionId: "53aihub-workbuddy-shared",
    historyScope: "all",
    codebuddyCliPath: "/Applications/WorkBuddy.app/Contents/Resources/app.asar.unpacked/cli/bin/codebuddy",
    channelEntryPath: "/tmp/plugin/dist/codebuddy-channel.cjs"
  };
}

async function createFakeAcpServer(): Promise<{
  port: number;
  onRequest(callback: (request: { path: string; body: any; headers: Record<string, string | string[] | undefined> }) => void): void;
  close(): Promise<void>;
}> {
  let handler: ((request: { path: string; body: any; headers: Record<string, string | string[] | undefined> }) => void) | undefined;
  const server = createServer((req, res) => {
    let raw = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      raw += chunk;
    });
    req.on("end", () => {
      const body = raw ? JSON.parse(raw) : undefined;
      handler?.({ path: req.url || "", body, headers: req.headers });
      if (req.url === "/api/v1/acp/connect") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ connectionId: "conn-1", sessionToken: "token-1" }));
        return;
      }
      res.writeHead(200, { "content-type": "text/event-stream" });
      const result = body?.method === "session/new" ? { sessionId: "created-session-1" } : { ok: true };
      res.end(`data: ${JSON.stringify({ jsonrpc: "2.0", id: body?.id, result })}\n\n`);
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("failed to bind fake ACP server");
  }
  return {
    port: address.port,
    onRequest(callback) {
      handler = callback;
    },
    close: () => new Promise((resolve) => server.close(() => resolve()))
  };
}

async function waitFor(assertion: () => void, timeoutMs = 1500): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw lastError instanceof Error ? lastError : new Error("condition not met");
}
