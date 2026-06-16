import { createServer } from "node:http";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket, { WebSocketServer } from "ws";

import { createCodexChannelBridge, type CodexChannelConfig } from "../src/codex-channel";
import type { CodexTurnRunner } from "../src/codex-app-server";
import { ensureCodexConversationWorkspace, readCodexWorkspaceMappings, updateCodexWorkspaceThread } from "../src/codex-workspace";
import { CODEX_SESSION_STATE_FILE } from "../src/codex-session-store";

const cleanupTasks: Array<() => Promise<void>> = [];

afterEach(async () => {
  await Promise.all(cleanupTasks.splice(0).map((cleanup) => cleanup()));
});

describe("Codex 53AIHub channel", () => {
  it("streams Codex reasoning, process output, answer deltas, and ledger terminal events", async () => {
    const server = await createFakeHubServer();
    cleanupTasks.push(server.close);
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-channel-"));
    cleanupTasks.push(async () => rm(tempRoot, { recursive: true, force: true }));
    const runner = createMockRunner();
    const bridge = createCodexChannelBridge({
      config: buildConfig(server.url, join(tempRoot, ".53ai", "codex-workspaces")),
      runner
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(buildChatFrame("req-1", "chat-alpha", "Alex Zhang", "hello codex"));

      await waitFor(() => {
        expect(frameByReq(server.frames, "req-1", "streaming")?.data?.choices?.[0]?.delta?.content).toBe("answer delta");
        expect(eventFrameByKind(server.frames, "assistant.thinking")).toMatchObject({
          data: {
            payload: {
              hostKind: "codex",
              runnerCommand: "codex-app-server"
            }
          }
        });
        expect(eventFrameByKind(server.frames, "process.step")).toMatchObject({
          data: {
            payload: {
              process_step: {
                step_code: "command_output",
                message: "command output"
              }
            }
          }
        });
        expect(eventFrameByKind(server.frames, "run.completed")).toMatchObject({
          status: "done",
          data: {
            payload: {
              openclaw_ledger: {
                event_type: "turn.completed",
                terminal_status: "completed"
              }
            }
          }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "chat-alpha" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-snapshot")).toMatchObject({
          status: "done",
          data: {
            session: {
              hostKind: "codex",
              runnerCommand: "codex-app-server",
              workspace: {
                workspaceName: expect.stringMatching(/^53aihub-Alex-Zhang-chat-alpha-/),
                codex_workspace_path: expect.stringContaining(".53ai/codex-workspaces/53aihub-Alex-Zhang-chat-alpha-")
              }
            },
            messages: [
              { role: "user", content: "hello codex" },
              { role: "assistant", content: "answer delta" }
            ],
            ledger_events: expect.arrayContaining([
              expect.objectContaining({ event_type: "turn.completed", terminal_status: "completed" })
            ])
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("hydrates workspace mappings on start so LaunchAgent restarts keep Codex sessions addressable", async () => {
    const server = await createFakeHubServer();
    cleanupTasks.push(server.close);
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-channel-"));
    cleanupTasks.push(async () => rm(tempRoot, { recursive: true, force: true }));
    const workspaceRoot = join(tempRoot, ".53ai", "codex-workspaces");
    await ensureCodexConversationWorkspace({
      conversationId: "agenthub_u1",
      userId: "agenthub_u1",
      userName: "test",
      workspaceRoot
    });
    await updateCodexWorkspaceThread(workspaceRoot, "agenthub_u1", "thread-restored");
    const bridge = createCodexChannelBridge({
      config: buildConfig(server.url, workspaceRoot),
      runner: createMockRunner()
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-current-restored",
          action: "sessions.current",
          status: "request",
          data: { chat_id: "agenthub_u1", user: "agenthub_u1" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-current-restored")).toMatchObject({
          status: "done",
          data: {
            id: "agenthub_u1",
            title: "53AI Hub-test",
            threadId: "thread-restored"
          }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-stale-restored",
          action: "sessions.messages",
          status: "request",
          data: {
            session_id: "agent:main:dashboard:legacy",
            conversation_id: "agent:main:dashboard:legacy",
            chat_id: "agenthub_u1",
            user: "agenthub_u1"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-stale-restored")).toMatchObject({
          status: "done",
          data: {
            messages: [],
            pagination: {
              total: 0,
              hasMore: false
            }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("persists Codex session messages and ledger events across channel restarts", async () => {
    const server = await createFakeHubServer();
    cleanupTasks.push(server.close);
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-channel-"));
    cleanupTasks.push(async () => rm(tempRoot, { recursive: true, force: true }));
    const workspaceRoot = join(tempRoot, ".53ai", "codex-workspaces");
    const bridge = createCodexChannelBridge({
      config: buildConfig(server.url, workspaceRoot),
      runner: createMockRunner()
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(buildChatFrame("req-persist", "chat-persist", "Alex Zhang", "remember this"));
      await waitFor(() => expect(frameByReq(server.frames, "req-persist", "done")).toBeTruthy());
    } finally {
      await bridge.stop();
    }

    const mapping = (await readCodexWorkspaceMappings(workspaceRoot)).conversations["chat-persist"];
    expect(mapping?.workspaceDir).toBeTruthy();
    const stateFile = JSON.parse(await readFile(join(mapping.workspaceDir, CODEX_SESSION_STATE_FILE), "utf8"));
    expect(stateFile.session.messages).toMatchObject([
      { role: "user", content: "remember this" },
      { role: "assistant", content: "answer delta" }
    ]);
    expect(stateFile.session.events.length).toBeGreaterThan(0);

    const restartServer = await createFakeHubServer();
    cleanupTasks.push(restartServer.close);
    const restartedBridge = createCodexChannelBridge({
      config: buildConfig(restartServer.url, workspaceRoot),
      runner: createMockRunner()
    });

    await restartedBridge.start();
    try {
      const connection = await restartServer.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-persist-messages",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "chat-persist", limit: 10 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-persist-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "chat-persist" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(restartServer.frames, "rpc-persist-messages")).toMatchObject({
          status: "done",
          data: {
            messages: [
              { role: "user", content: "remember this" },
              { role: "assistant", content: "answer delta" }
            ],
            pagination: {
              total: 2,
              hasMore: false
            }
          }
        });
        expect(frameByReq(restartServer.frames, "rpc-persist-snapshot")).toMatchObject({
          status: "done",
          data: {
            messages: [
              { role: "user", content: "remember this" },
              { role: "assistant", content: "answer delta" }
            ],
            ledger_events: expect.arrayContaining([
              expect.objectContaining({ event_type: "turn.completed", terminal_status: "completed" })
            ]),
            last_seq: expect.any(Number)
          }
        });
      });
    } finally {
      await restartedBridge.stop();
    }
  });

  it("emits Codex App Server event trace summaries when traceEvents is enabled", async () => {
    const server = await createFakeHubServer();
    cleanupTasks.push(server.close);
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-channel-"));
    cleanupTasks.push(async () => rm(tempRoot, { recursive: true, force: true }));
    const logs: string[] = [];
    const bridge = createCodexChannelBridge({
      config: {
        ...buildConfig(server.url, join(tempRoot, ".53ai", "codex-workspaces")),
        traceEvents: true
      },
      runner: createMockRunner(),
      logger: {
        info: (message) => logs.push(message),
        warn: (message) => logs.push(message),
        error: (message) => logs.push(message)
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(buildChatFrame("req-trace", "chat-trace", "Alex Zhang", "trace codex"));
      await waitFor(() => expect(frameByReq(server.frames, "req-trace", "done")).toBeTruthy());

      const traceLines = logs.filter((line) => line.startsWith("[53aihub-codex-trace]"));
      expect(traceLines.some((line) => line.includes("appserver.notification"))).toBe(true);
      expect(traceLines.some((line) => line.includes("item/reasoning/textDelta"))).toBe(true);
      expect(traceLines.some((line) => line.includes("appserver.map.process.delta"))).toBe(true);
      const summary = traceLines.find((line) => line.includes("appserver.turn_summary"));
      expect(summary).toContain('"reasoning_delta_count":1');
      expect(summary).toContain('"process_delta_count":1');
      expect(summary).toContain('"agent_message_delta_count":1');
      expect(summary).not.toContain("thinking delta");
      expect(summary).not.toContain("command output");
      expect(summary).not.toContain("answer delta");
    } finally {
      await bridge.stop();
    }
  });

  it("resolves stale OpenClaw RPC session ids through stable chat metadata", async () => {
    const server = await createFakeHubServer();
    cleanupTasks.push(server.close);
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-channel-"));
    cleanupTasks.push(async () => rm(tempRoot, { recursive: true, force: true }));
    const runner = createMockRunner();
    const bridge = createCodexChannelBridge({
      config: buildConfig(server.url, join(tempRoot, ".53ai", "codex-workspaces")),
      runner
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(buildChatFrame("req-legacy", "chat-alpha", "Alex Zhang", "hello from old url"));
      await waitFor(() => expect(frameByReq(server.frames, "req-legacy", "done")).toBeTruthy());

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-legacy-messages",
          action: "sessions.messages",
          status: "request",
          data: {
            session_id: "agent:main:dashboard:legacy",
            conversation_id: "agent:main:dashboard:legacy",
            chat_id: "chat-alpha",
            user: "chat-alpha",
            limit: 10
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-legacy-messages")).toMatchObject({
          status: "done",
          data: {
            messages: [
              { role: "user", content: "hello from old url" },
              { role: "assistant", content: "answer delta" }
            ],
            pagination: {
              total: 2,
              hasMore: false
            }
          }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-legacy-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: {
            session_id: "agent:main:dashboard:legacy",
            conversation_id: "agent:main:dashboard:legacy",
            user: "chat-alpha"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-legacy-snapshot")).toMatchObject({
          status: "done",
          data: {
            session: { id: "chat-alpha" },
            session_id: "chat-alpha",
            conversation_id: "chat-alpha"
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("allocates distinct hidden workspaces for different conversations from the same user", async () => {
    const server = await createFakeHubServer();
    cleanupTasks.push(server.close);
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-channel-"));
    cleanupTasks.push(async () => rm(tempRoot, { recursive: true, force: true }));
    const calls: string[] = [];
    const runner = createMockRunner((cwd) => calls.push(cwd));
    const bridge = createCodexChannelBridge({
      config: buildConfig(server.url, join(tempRoot, ".53ai", "codex-workspaces")),
      runner
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(buildChatFrame("req-a", "chat-a", "Alex Zhang", "first"));
      await waitFor(() => expect(frameByReq(server.frames, "req-a", "done")).toBeTruthy());
      connection.socket.send(buildChatFrame("req-b", "chat-b", "Alex Zhang", "second"));
      await waitFor(() => expect(frameByReq(server.frames, "req-b", "done")).toBeTruthy());

      expect(calls).toHaveLength(2);
      expect(calls[0]).toContain(".53ai/codex-workspaces/53aihub-Alex-Zhang-chat-a-");
      expect(calls[1]).toContain(".53ai/codex-workspaces/53aihub-Alex-Zhang-chat-b-");
      expect(calls[0]).not.toBe(calls[1]);
    } finally {
      await bridge.stop();
    }
  });

  it("passes stop control through to Codex turn interrupt", async () => {
    const server = await createFakeHubServer();
    cleanupTasks.push(server.close);
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-channel-"));
    cleanupTasks.push(async () => rm(tempRoot, { recursive: true, force: true }));
    let releaseTurn!: () => void;
    const interruptTurn = vi.fn(async () => {
      releaseTurn();
    });
    const runner: CodexTurnRunner = {
      async runTurn(input) {
        await input.onThreadStarted?.({ threadId: "thread-stop", cwd: input.cwd });
        await input.onTurnStarted?.({ threadId: "thread-stop", turnId: "turn-stop" });
        await new Promise<void>((resolve) => {
          releaseTurn = resolve;
        });
        await input.onEvent({
          method: "turn/completed",
          params: {
            threadId: "thread-stop",
            turn: { id: "turn-stop", status: "interrupted", items: [] }
          }
        });
        return { threadId: "thread-stop", turnId: "turn-stop", status: "interrupted", finalText: "" };
      },
      interruptTurn
    };
    const bridge = createCodexChannelBridge({
      config: buildConfig(server.url, join(tempRoot, ".53ai", "codex-workspaces")),
      runner
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(buildChatFrame("req-stop", "chat-stop", "Alex Zhang", "long task"));
      await waitFor(() => expect(eventFrameByKind(server.frames, "run.started")).toBeTruthy());
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-stop",
          action: "sessions.control",
          status: "request",
          data: { session_id: "chat-stop", action: "stop" }
        })
      );
      await waitFor(() => {
        expect(interruptTurn).toHaveBeenCalledWith("thread-stop", "turn-stop");
        expect(frameByReq(server.frames, "rpc-stop")).toMatchObject({
          status: "done",
          data: { ok: true, action: "stop", session_id: "chat-stop" }
        });
      });
    } finally {
      releaseTurn?.();
      await bridge.stop();
    }
  });
});

function createMockRunner(onCwd?: (cwd: string) => void): CodexTurnRunner {
  return {
    async runTurn(input) {
      onCwd?.(input.cwd);
      await input.onThreadStarted?.({ threadId: `thread-${input.conversationId}`, cwd: input.cwd });
      await input.onTurnStarted?.({ threadId: `thread-${input.conversationId}`, turnId: `turn-${input.conversationId}` });
      await input.onEvent({
        method: "item/reasoning/textDelta",
        params: { threadId: `thread-${input.conversationId}`, turnId: `turn-${input.conversationId}`, itemId: "reasoning-1", delta: "thinking delta" }
      });
      await input.onEvent({
        method: "item/commandExecution/outputDelta",
        params: { threadId: `thread-${input.conversationId}`, turnId: `turn-${input.conversationId}`, itemId: "command-1", delta: "command output" }
      });
      await input.onEvent({
        method: "item/agentMessage/delta",
        params: { threadId: `thread-${input.conversationId}`, turnId: `turn-${input.conversationId}`, itemId: "answer-1", delta: "answer delta" }
      });
      await input.onEvent({
        method: "turn/completed",
        params: {
          threadId: `thread-${input.conversationId}`,
          turn: {
            id: `turn-${input.conversationId}`,
            status: "completed",
            items: [{ type: "agentMessage", text: "answer delta" }]
          }
        }
      });
      return {
        threadId: `thread-${input.conversationId}`,
        turnId: `turn-${input.conversationId}`,
        status: "completed",
        finalText: "answer delta"
      };
    }
  };
}

function buildConfig(wsUrl: string, workspaceRoot: string): CodexChannelConfig {
  return {
    wsUrl,
    botId: "bot-123",
    secret: "sk-secret",
    accessPolicy: "open",
    allowFrom: [],
    sendThinkingMessage: false,
    reconnectBaseMs: 20,
    maxReconnectAttempts: 2,
    codexBinPath: "/usr/local/bin/codex",
    codexVersion: "codex-cli test",
    workspaceRoot
  };
}

function buildChatFrame(reqId: string, conversationId: string, userName: string, text: string): string {
  return JSON.stringify({
    req_id: reqId,
    action: "chat",
    data: {
      user: "user-a",
      conversation_id: conversationId,
      metadata: { userName },
      messages: [{ role: "user", content: text }]
    }
  });
}

async function createFakeHubServer(): Promise<{
  url: string;
  frames: Array<Record<string, any>>;
  connected: Promise<{ socket: WebSocket; headers: Record<string, string | undefined> }>;
  close: () => Promise<void>;
}> {
  const httpServer = createServer();
  const wsServer = new WebSocketServer({ server: httpServer });
  const frames: Array<Record<string, any>> = [];
  let resolveConnected!: (value: { socket: WebSocket; headers: Record<string, string | undefined> }) => void;
  const connected = new Promise<{ socket: WebSocket; headers: Record<string, string | undefined> }>((resolve) => {
    resolveConnected = resolve;
  });

  wsServer.on("connection", (socket, request) => {
    resolveConnected({
      socket,
      headers: request.headers as Record<string, string | undefined>
    });
    socket.on("message", (raw) => {
      const payload = safeParse(String(raw));
      if (payload?.action === "ping") {
        socket.send(JSON.stringify({ action: "pong", data: { ok: true } }));
      }
      if (payload?.req_id) {
        frames.push(payload);
      }
    });
  });

  await new Promise<void>((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(0, "127.0.0.1", () => {
      httpServer.off("error", reject);
      resolve();
    });
  });
  const address = httpServer.address();
  if (!address || typeof address === "string") {
    throw new Error("failed to bind fake hub server");
  }

  return {
    url: `ws://127.0.0.1:${address.port}`,
    frames,
    connected,
    close: async () => {
      for (const client of wsServer.clients) {
        client.close();
      }
      await new Promise<void>((resolve) => wsServer.close(() => resolve()));
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    }
  };
}

function frameByReq(
  frames: Array<Record<string, any>>,
  reqId: string,
  status?: string
): Record<string, any> | undefined {
  return frames.find((frame) => frame.req_id === reqId && (!status || frame.status === status));
}

function eventFrameByKind(frames: Array<Record<string, any>>, eventKind: string): Record<string, any> | undefined {
  return frames.find((frame) => frame.data?.event_kind === eventKind);
}

function safeParse(raw: string): Record<string, any> | null {
  try {
    return JSON.parse(raw) as Record<string, any>;
  } catch {
    return null;
  }
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
