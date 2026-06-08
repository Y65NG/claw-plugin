import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";
import WebSocket, { WebSocketServer } from "ws";

import {
  createCodeBuddyChannelBroker,
  createCodeBuddyChannelBridge,
  loadCodeBuddyChannelConfig,
  type CodeBuddyChannelNotification
} from "../src/codebuddy-channel";

const cleanupServers: Array<() => Promise<void>> = [];

afterEach(async () => {
  await Promise.all(cleanupServers.splice(0).map((cleanup) => cleanup()));
});

describe("CodeBuddy 53AIHub channel", () => {
  it("forwards Hub chat messages as channel notifications and replies through Hub chunks", async () => {
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const notifications: CodeBuddyChannelNotification[] = [];
    const bridge = createCodeBuddyChannelBridge({
      config: buildConfig(server.url),
      notifyChannel: async (notification) => {
        notifications.push(notification);
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-1",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            metadata: { userName: "Alex" },
            messages: [{ role: "user", content: "hello from hub" }]
          }
        })
      );

      await waitFor(() => {
        expect(notifications).toEqual([
          {
            content: [
              "hello from hub",
              "",
              "<reply_instruction>",
              "处理完这条 53AIHub 消息后，必须调用 53aihub-channel 的 reply 工具，把最终回复写入 text，并使用下面 meta 中的 chat_id 与 req_id。",
              "</reply_instruction>"
            ].join("\n"),
            meta: {
              source: "53aihub",
              sender: "Alex",
              chat_id: "chat-a",
              req_id: "req-1",
              msg_id: "req-1",
              user_id: "user-a",
              user_name: "Alex"
            }
          }
        ]);
      });
      await waitFor(() => {
        expect(frameByReq(server.frames, "req-1", "thinking")).toMatchObject({
          req_id: "req-1",
          action: "chat",
          status: "thinking"
        });
      });

      await bridge.reply({ chatId: "chat-a", text: "reply from codebuddy" });
      await waitFor(() => {
        expect(frameByReq(server.frames, "req-1", "done")).toMatchObject({
          req_id: "req-1",
          action: "chat",
          status: "done",
          data: {
            conversation_id: "chat-a",
            choices: [
              {
                delta: {
                  content: "reply from codebuddy"
                }
              }
            ]
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("rejects disallowed senders before notifying CodeBuddy", async () => {
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const notifications: CodeBuddyChannelNotification[] = [];
    const bridge = createCodeBuddyChannelBridge({
      config: {
        ...buildConfig(server.url),
        accessPolicy: "allowlist",
        allowFrom: ["trusted-user"]
      },
      notifyChannel: async (notification) => {
        notifications.push(notification);
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-denied",
          action: "chat",
          data: {
            user: "intruder",
            conversation_id: "chat-denied",
            messages: [{ role: "user", content: "should not enter channel" }]
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "req-denied", "error")).toMatchObject({
          status: "error",
          data: {
            error: {
              code: "ACCESS_DENIED"
            }
          }
        });
      });
      expect(notifications).toEqual([]);
    } finally {
      await bridge.stop();
    }
  });

  it("replies to the latest request for a chat when req_id is omitted", async () => {
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const notifications: CodeBuddyChannelNotification[] = [];
    const bridge = createCodeBuddyChannelBridge({
      config: { ...buildConfig(server.url), sendThinkingMessage: false },
      notifyChannel: async (notification) => {
        notifications.push(notification);
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      for (const reqId of ["req-old", "req-new"]) {
        connection.socket.send(
          JSON.stringify({
            req_id: reqId,
            action: "chat",
            data: {
              user: "user-a",
              conversation_id: "chat-a",
              messages: [{ role: "user", content: reqId }]
            }
          })
        );
      }

      await waitFor(() => {
        expect(notifications.map((item) => item.meta.req_id)).toEqual(["req-old", "req-new"]);
      });
      await bridge.reply({ chatId: "chat-a", text: "latest reply" });
      await waitFor(() => {
        expect(frameByReq(server.frames, "req-new", "done")).toMatchObject({
          status: "done",
          data: {
            choices: [
              {
                delta: {
                  content: "latest reply"
                }
              }
            ]
          }
        });
      });
      expect(frameByReq(server.frames, "req-old", "done")).toBeUndefined();
    } finally {
      await bridge.stop();
    }
  });

  it("responds to 53AIHub RPC requests used by the OpenClaw management UI", async () => {
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const notifications: CodeBuddyChannelNotification[] = [];
    const bridge = createCodeBuddyChannelBridge({
      config: { ...buildConfig(server.url), sendThinkingMessage: false },
      notifyChannel: async (notification) => {
        notifications.push(notification);
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-chat",
          action: "chat",
          data: {
            user: "agenthub_u1",
            conversation_id: "agenthub_u1",
            metadata: { userName: "User One" },
            messages: [{ role: "user", content: "first message" }]
          }
        })
      );

      await waitFor(() => {
        expect(notifications).toHaveLength(1);
      });
      await bridge.reply({ chatId: "agenthub_u1", text: "assistant answer" });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-status",
          action: "runtime.get",
          status: "request",
          data: { include: "status" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-list",
          action: "sessions.list",
          status: "request",
          data: { limit: 10, offset: 0 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-current",
          action: "sessions.current",
          status: "request",
          data: { chat_id: "agenthub_u1" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-messages",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "agenthub_u1", limit: 10, offset: 0 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-cron",
          action: "cron.tasks",
          status: "request",
          data: {}
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-status")).toMatchObject({
          action: "runtime.get",
          status: "done",
          data: {
            configured: true,
            connectionStatus: "connected",
            healthy: true,
            hostKind: "workbuddy"
          }
        });
        expect(frameByReq(server.frames, "rpc-list")).toMatchObject({
          action: "sessions.list",
          status: "done",
          data: {
            sessions: [{ id: "agenthub_u1", title: "53AI Hub-User One：first message" }],
            pagination: { limit: 10, offset: 0, total: 1, hasMore: false }
          }
        });
        expect(frameByReq(server.frames, "rpc-current")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: { id: "agenthub_u1" }
        });
        expect(frameByReq(server.frames, "rpc-messages")).toMatchObject({
          action: "sessions.messages",
          status: "done",
          data: {
            messages: [
              { role: "user", content: "first message" },
              { role: "assistant", content: "assistant answer" }
            ],
            pagination: { limit: 10, offset: 0, total: 2, hasMore: false }
          }
        });
        expect(frameByReq(server.frames, "rpc-cron")).toMatchObject({
          action: "cron.tasks",
          status: "done",
          data: {
            tasks: [],
            pagination: { limit: 100, offset: 0, total: 0, hasMore: false }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("loads required config from WorkBuddy/CodeBuddy environment aliases", () => {
    expect(
      loadCodeBuddyChannelConfig({
        CODEBUDDY_PLUGIN_OPTION_HUB53AI_WS_URL: "ws://127.0.0.1:1",
        CODEBUDDY_PLUGIN_OPTION_HUB53AI_BOT_ID: "bot-1",
        CODEBUDDY_PLUGIN_OPTION_HUB53AI_SECRET: "secret-1",
        CODEBUDDY_PLUGIN_OPTION_HUB53AI_ACCESS_POLICY: "allowlist",
        CODEBUDDY_PLUGIN_OPTION_HUB53AI_ALLOW_FROM: "user-a,user-b",
        CODEBUDDY_PLUGIN_OPTION_HUB53AI_SEND_THINKING_MESSAGE: "false"
      })
    ).toMatchObject({
      wsUrl: "ws://127.0.0.1:1",
      botId: "bot-1",
      secret: "secret-1",
      accessPolicy: "allowlist",
      allowFrom: ["user-a", "user-b"],
      sendThinkingMessage: false,
      workbuddyHistoryScope: "all",
      workbuddySessionId: "53aihub-workbuddy-shared"
    });
  });

  it("keeps only one local 53AIHub connection owner and forwards follower replies", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "53aihub-channel-"));
    cleanupServers.push(async () => {
      await rm(stateDir, { recursive: true, force: true });
    });
    const replies: Array<{ chatId: string; text: string; reqId?: string }> = [];
    const socketPath = join(stateDir, "broker.sock");
    const leader = await createCodeBuddyChannelBroker({
      socketPath,
      handlers: {
        reply: async (reply) => {
          replies.push(reply);
        },
        status: () => ({ connectionStatus: "connected" })
      }
    });
    cleanupServers.push(leader.close);
    const follower = await createCodeBuddyChannelBroker({
      socketPath,
      handlers: {
        reply: async () => {
          throw new Error("follower should not handle replies locally");
        },
        status: () => ({ connectionStatus: "follower" })
      }
    });

    expect(leader.role).toBe("leader");
    expect(follower.role).toBe("follower");

    await follower.requestReply({ chatId: "chat-a", text: "reply through leader", reqId: "req-a" });

    expect(replies).toEqual([{ chatId: "chat-a", text: "reply through leader", reqId: "req-a" }]);
    await expect(follower.requestStatus()).resolves.toMatchObject({ connectionStatus: "connected" });
  });

  it("exposes WorkBuddy local history through 53AIHub session RPCs", async () => {
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const bridge = createCodeBuddyChannelBridge({
      config: buildConfig(server.url),
      notifyChannel: async () => {},
      historyLoader: async () => ({
        sessions: [
          {
            id: "wb-session-1",
            title: "Existing WorkBuddy task",
            status: "completed" as const,
            hostKind: "workbuddy" as const,
            runnerCommand: "workbuddy" as const,
            createdAt: "2026-06-01T00:00:00.000Z",
            updatedAt: "2026-06-01T00:01:00.000Z",
            lastEventSeq: 2
          }
        ],
        messagesBySessionId: new Map([
          [
            "wb-session-1",
            [
              {
                id: "m1",
                sessionId: "wb-session-1",
                role: "user",
                content: "historical question",
                createdAt: "2026-06-01T00:00:00.000Z"
              },
              {
                id: "m2",
                sessionId: "wb-session-1",
                role: "assistant",
                content: "historical answer",
                createdAt: "2026-06-01T00:01:00.000Z"
              }
            ]
          ]
        ])
      })
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-history-list",
          action: "sessions.list",
          status: "request",
          data: { limit: 10, offset: 0 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-history-messages",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "wb-session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-history-list")).toMatchObject({
          status: "done",
          data: {
            sessions: [{ id: "wb-session-1", title: "Existing WorkBuddy task", runnerCommand: "workbuddy" }]
          }
        });
        expect(frameByReq(server.frames, "rpc-history-messages")).toMatchObject({
          status: "done",
          data: {
            messages: [
              { role: "user", content: "historical question" },
              { role: "assistant", content: "historical answer" }
            ]
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });
});

function buildConfig(wsUrl: string) {
  return {
    wsUrl,
    botId: "bot-123",
    secret: "sk-secret",
    accessPolicy: "open" as const,
    allowFrom: [],
    sendThinkingMessage: true,
    reconnectBaseMs: 20,
    maxReconnectAttempts: 2,
    workbuddyHome: "/tmp/workbuddy",
    workbuddyHistoryScope: "all" as const,
    workbuddySessionId: "53aihub-workbuddy-shared"
  };
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

function safeParse(raw: string): Record<string, unknown> | null {
  try {
    return JSON.parse(raw) as Record<string, unknown>;
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
