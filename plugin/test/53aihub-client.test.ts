import { createServer } from "node:http";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";
import WebSocket, { WebSocketServer } from "ws";

import {
  createHub53AIBridge,
  parseIncomingMessage,
  sliceLatestWindowPage,
  type Hub53AIOutgoingFrame
} from "../src/53aihub-client";
import type { GatewayEvent, GatewaySession } from "../src/gateway-client";
import type { SessionMessage, SessionStatus } from "../src/models";

const cleanupPaths: string[] = [];
const cleanupServers: Array<() => Promise<void>> = [];

afterEach(async () => {
  await Promise.all(cleanupServers.splice(0).map((cleanup) => cleanup()));
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("53AIHub client", () => {
  it("slices older pages from the fetched latest message window", () => {
    const messages = ["m3", "m4", "m5", "m6"];

    expect(sliceLatestWindowPage(messages, 2, 0)).toEqual(["m5", "m6"]);
    expect(sliceLatestWindowPage(messages, 2, 2)).toEqual(["m3", "m4"]);
  });

  it("parses OpenAI-compatible chat messages and direct message payloads", () => {
    const chat = parseIncomingMessage(
      JSON.stringify({
        req_id: "req-1",
        action: "chat",
        data: {
          user: "user-a",
          conversation_id: "chat-a",
          messages: [
            { role: "system", content: "ignore" },
            {
              role: "user",
              content: [
                { type: "text", text: "hello" },
                { type: "image_url", image_url: { url: "https://example.com/image.png" } }
              ]
            }
          ]
        }
      })
    );

    expect(chat).toMatchObject({
      reqId: "req-1",
      msgId: "req-1",
      chatId: "chat-a",
      userId: "user-a",
      text: "hello",
      imageUrls: ["https://example.com/image.png"]
    });

    const direct = parseIncomingMessage(
      JSON.stringify({
        action: "message",
        data: {
          msgId: "msg-1",
          chatId: "chat-b",
          userId: "user-b",
          content: "direct hello",
          files: [{ url: "https://example.com/file.pdf" }]
        }
      })
    );

    expect(direct).toMatchObject({
      reqId: "msg-1",
      msgId: "msg-1",
      chatId: "chat-b",
      userId: "user-b",
      text: "direct hello",
      fileUrls: ["https://example.com/file.pdf"]
    });
    expect(parseIncomingMessage(JSON.stringify({ action: "ping" }))).toBeNull();
    expect(parseIncomingMessage("not-json")).toBeNull();
  });

  it("extracts 53AIHub user names and creates readable local session titles", async () => {
    const chat = parseIncomingMessage(
      JSON.stringify({
        req_id: "req-title",
        action: "chat",
        data: {
          user: "user-123",
          conversation_id: "chat-title",
          metadata: {
            userName: "杨芳贤"
          },
          messages: [
            {
              role: "user",
              content: "每日CRM与企微资讯简报查看53ai.com官网的更新"
            }
          ]
        }
      })
    );

    expect(chat).toMatchObject({
      userName: "杨芳贤",
      userId: "user-123",
      chatId: "chat-title",
      text: "每日CRM与企微资讯简报查看53ai.com官网的更新"
    });

    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-title",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-title",
            metadata: {
              userName: "杨芳贤"
            },
            messages: [
              {
                role: "user",
                content: "每日CRM与企微资讯简报查看53ai.com官网的更新"
              }
            ]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.createdTitles).toEqual([
          "53AI Hub-杨芳贤：每日CRM与企微资讯简报查看53ai.com官网的更新"
        ]);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("preserves OpenClaw stream metadata when sending Hub chat chunks", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-delta",
        sessionId: "session-1",
        seq: 1,
        kind: "assistant.delta",
        payload: {
          content: "临时回复",
          state: "delta",
          mode: "replace",
          replace: true
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-thinking",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.thinking",
        payload: {
          content: "正在思考",
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-tool-call",
        sessionId: "session-1",
        seq: 3,
        kind: "tool.call",
        payload: {
          data: {
            name: "web_search",
            args: {
              query: "OpenClaw 53AI"
            }
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-done",
        sessionId: "session-1",
        seq: 4,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: true,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-meta",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-meta",
            messages: [{ role: "user", content: "测试" }]
          }
        })
      );

      await waitFor(() => {
        const streaming = server.frames.find(
          (frame) =>
            frame.action === "chat" &&
            frame.status === "streaming" &&
            frame.data.choices[0]?.delta.content === "临时回复"
        );
        const thinking = server.frames.find(
          (frame) =>
            frame.action === "chat" &&
            frame.status === "thinking" &&
            frame.data.choices[0]?.delta.content === "正在思考"
        );
        const toolCall = server.frames.find(
          (frame) =>
            frame.action === "chat" &&
            frame.status === "thinking" &&
            frame.data.event_kind === "tool.call"
        );

        expect(streaming).toMatchObject({
          data: {
            status: "streaming",
            mode: "replace",
            replace: true,
            event_kind: "assistant.delta"
          }
        });
        expect(thinking).toMatchObject({
          data: {
            status: "thinking",
            mode: "replace",
            replace: true,
            event_kind: "assistant.thinking",
            payload: {
              content: "正在思考",
              state: "final",
              mode: "replace",
              replace: true
            }
          }
        });
        expect(toolCall).toMatchObject({
          data: {
            status: "thinking",
            mode: "append",
            replace: false,
            event_kind: "tool.call",
            payload: {
              data: {
                name: "web_search",
                args: {
                  query: "OpenClaw 53AI"
                }
              }
            }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("renames only old 53AIHub placeholder session titles for existing mappings", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify(
        {
          mappings: {
            "chat-title": "session-existing"
          },
          outbox: []
        },
        null,
        2
      )
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "session-existing",
      title: "53AIHub chat-title",
      status: "idle",
      hostKind: "qclaw",
      runnerCommand: "gateway",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      lastEventSeq: 0
    });

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-rename",
          action: "message",
          data: {
            msgId: "msg-title",
            chatId: "chat-title",
            userId: "user-123",
            userName: "杨芳贤",
            content: "每日CRM与企微资讯简报查看53ai.com官网的更新"
          }
        })
      );

      await waitFor(() => {
        expect(gateway.renames).toEqual([
          {
            sessionId: "session-existing",
            title: "53AI Hub-杨芳贤：每日CRM与企微资讯简报查看53ai.com官网的更新"
          }
        ]);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("creates a uniquely titled session when the first 53AIHub title is already used", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.failCreateTitleOnce("53AI Hub-杨芳贤：重复问题", "label already in use");
    gateway.eventsToEmit = [
      {
        id: "evt-1",
        sessionId: "session-1",
        seq: 1,
        kind: "assistant.delta",
        payload: { content: "created" },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-2",
        sessionId: "session-1",
        seq: 2,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-duplicate-title",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "new-chat-a",
            metadata: { userName: "杨芳贤" },
            messages: [{ role: "user", content: "重复问题" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.createdTitles).toEqual([
          "53AI Hub-杨芳贤：重复问题",
          "53AI Hub-杨芳贤：重复问题 (2)"
        ]);
        const streaming = server.frames.find((frame) => frame.action === "chat" && frame.status === "streaming");
        expect(streaming?.data.session_id).toBe("session-1");
        expect(streaming?.data.choices[0]?.delta.content).toBe("created");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("authenticates to 53AIHub and bridges remote chat frames to the local gateway", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-status-running",
        sessionId: "session-1",
        seq: 1,
        kind: "status.update",
        payload: { status: "running" },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-thinking",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.thinking",
        payload: { content: "Thinking through the request" },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-1",
        sessionId: "session-1",
        seq: 3,
        kind: "assistant.delta",
        payload: { content: "Hello from local Claw" },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-status-done",
        sessionId: "session-1",
        seq: 4,
        kind: "status.update",
        payload: { status: "done" },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-2",
        sessionId: "session-1",
        seq: 5,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];
    const userMessages: SessionMessage[] = [];
    const statuses: Array<{ sessionId: string; status: SessionStatus }> = [];

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: true,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async (message) => {
          userMessages.push(message);
        },
        onSessionStatus: async (sessionId, status) => {
          statuses.push({ sessionId, status });
        },
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      expect(connection.headers.authorization).toBe("Bearer sk-secret");
      expect(connection.headers["x-bot-id"]).toBe("bot-123");
      expect(connection.headers["x-api-key"]).toBe("sk-secret");
      expect(connection.headers["proxy-authorization"]).toBe(
        `Basic ${Buffer.from("bot-123:sk-secret").toString("base64")}`
      );

      connection.socket.send(
        JSON.stringify({
          req_id: "req-bridge",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            messages: [{ role: "user", content: "Say hello" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([{ sessionId: "session-1", content: "Say hello" }]);
        expect(userMessages.at(-1)?.content).toBe("Say hello");
        expect(statuses.at(-1)).toEqual({ sessionId: "session-1", status: "running" });
      });

      await waitFor(() => {
        const chatFrames = server.frames.filter((frame) => frame.action === "chat");
        expect(chatFrames.some((frame) => frame.status === "thinking")).toBe(true);
        expect(
          chatFrames.some(
            (frame) =>
              frame.status === "thinking" &&
              frame.data.choices[0]?.delta.content === "Thinking through the request"
          )
        ).toBe(true);
        const thinkingText = chatFrames
          .filter((frame) => frame.status === "thinking")
          .map((frame) => frame.data.choices[0]?.delta.content)
          .join("\n");
        expect(thinkingText).not.toContain("running");
        expect(thinkingText).not.toContain("done");
        expect(chatFrames.some((frame) => frame.status === "streaming")).toBe(true);
        const streaming = chatFrames.find((frame) => frame.status === "streaming");
        expect(streaming?.data.session_id).toBe("session-1");
        expect(streaming?.data.conversation_id).toBe("session-1");
        expect(streaming?.data.choices[0]?.delta.content).toBe("Hello from local Claw");
        const done = chatFrames.find((frame) => frame.status === "done");
        expect(done?.data.session_id).toBe("session-1");
        expect(done?.data.conversation_id).toBe("session-1");
        expect(done?.data.choices[0]?.delta.content).toBe("");
      });

      expect(bridge.getStatus()).toMatchObject({
        enabled: true,
        configured: true,
        connectionStatus: "connected",
        botId: "bo***23",
        receivedMessageCount: 1
      });
    } finally {
      await bridge.stop();
    }
  });

  it("ignores stale terminal events replayed before the current OpenClaw send", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-stale-interrupted",
        sessionId: "session-1",
        seq: 8,
        kind: "run.interrupted",
        payload: { reason: "previous stop" },
        createdAt: "2026-05-27T09:55:28.000Z"
      },
      {
        id: "evt-current-delta",
        sessionId: "session-1",
        seq: 9,
        kind: "assistant.delta",
        payload: { content: "fresh reply" },
        createdAt: new Date(Date.now() + 20).toISOString()
      },
      {
        id: "evt-current-complete",
        sessionId: "session-1",
        seq: 10,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date(Date.now() + 30).toISOString()
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-after-stop",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            messages: [{ role: "user", content: "message after stop" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([{ sessionId: "session-1", content: "message after stop" }]);
        const streamingFrame = server.frames.find(
          (frame) => frame.req_id === "req-after-stop" && frame.status === "streaming"
        );
        expect(streamingFrame?.data.choices[0]?.delta.content).toBe("fresh reply");
        expect(server.frames.find((frame) => frame.req_id === "req-after-stop" && frame.status === "done")).toMatchObject({
          status: "done"
        });
      });

      const errorFrame = server.frames.find((frame) => frame.req_id === "req-after-stop" && frame.status === "error");
      expect(errorFrame).toBeUndefined();
    } finally {
      await bridge.stop();
    }
  });

  it("sends chat frames directly to existing OpenClaw session ids", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const existingSessionId = "agent:main:dashboard:0ea52c76-4a2d-410f-985a-ed1771a67e28";
    gateway.upsertSession({
      id: existingSessionId,
      title: "Existing OpenClaw session",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      lastEventSeq: 0
    });

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-existing-session",
          action: "chat",
          data: {
            user: "user-1",
            conversation_id: existingSessionId,
            messages: [{ role: "user", content: "test" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([{ sessionId: existingSessionId, content: "test" }]);
      });
      expect(gateway.createdTitles).toEqual([]);
    } finally {
      await bridge.stop();
    }
  });

  it("continues queued chat frames after a gateway stream disconnect", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const existingSessionId = "agent:main:dashboard:disconnect-test";
    gateway.disconnectOnNextSend = true;
    gateway.disconnectCompletionDelayMs = 600;
    gateway.upsertSession({
      id: existingSessionId,
      title: "Disconnect test",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      lastEventSeq: 0
    });

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-disconnect-1",
          action: "chat",
          data: {
            user: "user-1",
            conversation_id: existingSessionId,
            messages: [{ role: "user", content: "first" }]
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "req-disconnect-1")).toMatchObject({
          action: "chat",
          status: "error",
          data: {
            error: {
              code: "WEBSOCKET_ERROR"
            }
          }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "req-disconnect-2",
          action: "chat",
          data: {
            user: "user-1",
            conversation_id: existingSessionId,
            messages: [{ role: "user", content: "second" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([
          { sessionId: existingSessionId, content: "first" },
          { sessionId: existingSessionId, content: "second" }
        ]);
      }, 250);
    } finally {
      await bridge.stop();
    }
  });

  it("deduplicates cumulative assistant text before streaming it back to 53AIHub", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const firstChunk = "我查到 Literary Hub / Book Marks 的榜单。先给你 5 本：\n\n1. A";
    const finalText = `${firstChunk}\n\n2. B\n\n3. C`;
    gateway.eventsToEmit = [
      {
        id: "evt-1",
        sessionId: "session-1",
        seq: 1,
        kind: "assistant.delta",
        payload: { content: firstChunk },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-2",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.delta",
        payload: { content: finalText },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-3",
        sessionId: "session-1",
        seq: 3,
        kind: "assistant.message",
        payload: { content: finalText },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-4",
        sessionId: "session-1",
        seq: 4,
        kind: "run.completed",
        payload: { content: finalText },
        createdAt: new Date().toISOString()
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-cumulative",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            messages: [{ role: "user", content: "summarize books" }]
          }
        })
      );

      await waitFor(() => {
        const chatFrames = server.frames.filter((frame) => frame.action === "chat");
        const streamingText = chatFrames
          .filter((frame) => frame.status === "streaming")
          .map((frame) => frame.data.choices[0]?.delta.content ?? "");
        expect(streamingText.join("")).toBe(finalText);
        expect(streamingText).toEqual([firstChunk, "\n\n2. B\n\n3. C"]);
        const done = chatFrames.find((frame) => frame.status === "done");
        expect(done?.data.choices[0]?.delta.content).toBe("");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("responds to request-response RPC actions without starting a chat run", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.sessionPage = {
      sessions: [
        {
          id: "session-1",
          title: "53AI Hub-openclaw-local@example.com：旧会话",
          status: "completed",
          hostKind: "qclaw",
          runnerCommand: "gateway",
          createdAt: "2026-05-20T10:00:00.000Z",
          updatedAt: "2026-05-20T10:04:00.000Z",
          lastEventSeq: 2
        }
      ],
      pagination: {
        limit: 1,
        offset: 0,
        total: 1,
        hasMore: false
      }
    };
    gateway.upsertSession(gateway.sessionPage.sessions[0]);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      `${JSON.stringify({ mappings: { agenthub_u2001: "session-1" }, outbox: [] }, null, 2)}\n`
    );
    gateway.messagesBySession.set("session-1", [
      {
        id: "message-1",
        sessionId: "session-1",
        role: "user",
        content: "hello",
        createdAt: "2026-05-20T10:00:00.000Z"
      },
      {
        id: "message-2",
        sessionId: "session-1",
        role: "assistant",
        content: "hi",
        createdAt: "2026-05-20T10:00:01.000Z"
      }
    ]);
    gateway.eventsBySession.set("session-1", [
      {
        id: "event-1",
        sessionId: "session-1",
        seq: 1,
        kind: "assistant.thinking",
        payload: { content: "checking gateway" },
        createdAt: "2026-05-20T10:00:00.500Z"
      },
      {
        id: "event-2",
        sessionId: "session-1",
        seq: 2,
        kind: "run.interrupted",
        payload: { reason: "user stop" },
        createdAt: "2026-05-20T10:00:02.000Z"
      }
    ]);
    gateway.runtimeInfo = {
      modelPrimary: "openai/gpt-5.5",
      enabledSkills: ["browser", "weather"],
      cronScheduler: {
        enabled: true,
        jobCount: 1,
        nextWakeAt: "2026-05-21T08:00:00.000Z"
      },
      cronTasks: [
        {
          id: "daily-brief",
          name: "Daily brief",
          enabled: true,
          schedule: "cron 0 8 * * *"
        }
      ]
    };

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      rpcContext: {
        getStatusSnapshot: () => ({
          healthy: true,
          enabledSkills: ["browser", "weather"]
        }),
        getConfigSnapshot: () => ({
          hub53ai: {
            enabled: true,
            secret: "[redacted]"
          }
        })
      },
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-sessions",
          action: "sessions.list",
          status: "request",
          data: { limit: 1, offset: 0 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-current",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agenthub_u2001",
            userName: "openclaw-local@example.com"
          }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-current-missing",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agenthub_u404",
            userName: "missing@example.com"
          }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-messages",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "session-1", limit: 1, offset: 1 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-events",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 1, offset: 1 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-control",
          action: "sessions.control",
          status: "request",
          data: { session_id: "session-1", action: "stop" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-control-invalid",
          action: "sessions.control",
          status: "request",
          data: { session_id: "session-1", action: "pause" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-skills",
          action: "runtime.get",
          status: "request",
          data: { include: "skills" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-config",
          action: "runtime.get",
          status: "request",
          data: { include: "config" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-cron",
          action: "cron.tasks",
          status: "request",
          data: { limit: 10, offset: 0 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-unsupported",
          action: "future.unsupported",
          status: "request",
          data: {}
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-server-response",
          action: "sessions.list",
          status: "done",
          data: { sessions: [] }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-sessions")).toMatchObject({
          action: "sessions.list",
          status: "done",
          data: {
            sessions: [{ id: "session-1" }],
            pagination: { limit: 1, offset: 0, hasMore: false }
          }
        });
        expect(frameByReq(server.frames, "rpc-current")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: {
            id: "session-1",
            title: "53AI Hub-openclaw-local@example.com：旧会话"
          }
        });
        expect(frameByReq(server.frames, "rpc-current-missing")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: null
        });
        expect(frameByReq(server.frames, "rpc-messages")).toMatchObject({
          action: "sessions.messages",
          status: "done",
          data: {
            messages: [{ id: "message-1", content: "hello" }],
            pagination: { limit: 1, offset: 1, total: 3, hasMore: true, nextOffset: 2 }
          }
        });
        expect(frameByReq(server.frames, "rpc-events")).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [{ id: "event-2", kind: "run.interrupted" }],
            pagination: { limit: 1, offset: 1, total: 2, hasMore: false }
          }
        });
        expect(frameByReq(server.frames, "rpc-control")).toMatchObject({
          action: "sessions.control",
          status: "done",
          data: {
            ok: true,
            action: "stop",
            session_id: "session-1"
          }
        });
        expect(frameByReq(server.frames, "rpc-control-invalid")).toMatchObject({
          action: "sessions.control",
          status: "error",
          data: {
            code: "PARAM_ERROR"
          }
        });
        expect(frameByReq(server.frames, "rpc-skills")).toMatchObject({
          action: "runtime.get",
          status: "done",
          data: {
            skills: ["browser", "weather"],
            enabledSkills: ["browser", "weather"]
          }
        });
        expect(frameByReq(server.frames, "rpc-config")).toMatchObject({
          action: "runtime.get",
          status: "done",
          data: {
            hub53ai: {
              secret: "[redacted]"
            }
          }
        });
        expect(frameByReq(server.frames, "rpc-cron")).toMatchObject({
          action: "cron.tasks",
          status: "done",
          data: {
            tasks: [{ id: "daily-brief", name: "Daily brief" }],
            pagination: { limit: 10, offset: 0, total: 1, hasMore: false }
          }
        });
        expect(frameByReq(server.frames, "rpc-unsupported")).toMatchObject({
          action: "future.unsupported",
          status: "error",
          data: {
            code: "FEATURE_NOT_AVAILABLE"
          }
        });
      });
      expect(frameByReq(server.frames, "rpc-server-response")).toBeUndefined();
      expect(gateway.sentMessages).toEqual([]);
      expect(gateway.controls).toEqual([{ sessionId: "session-1", action: "stop" }]);
    } finally {
      await bridge.stop();
    }
  });

  it("does not restore sessions.current by display name when the stable mapping is missing or stale", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-current-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      `${JSON.stringify({ mappings: { agenthub_u2001: "deleted-session" }, outbox: [] }, null, 2)}\n`
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.sessionPage = {
      sessions: [
        {
          id: "session-a",
          title: "53AI Hub-Alex：旧会话",
          status: "completed",
          hostKind: "qclaw",
          runnerCommand: "gateway",
          createdAt: "2026-05-20T10:00:00.000Z",
          updatedAt: "2026-05-20T10:04:00.000Z",
          lastEventSeq: 1
        },
        {
          id: "session-b",
          title: "53AI Hub-Alex：另一个用户的会话",
          status: "completed",
          hostKind: "qclaw",
          runnerCommand: "gateway",
          createdAt: "2026-05-20T11:00:00.000Z",
          updatedAt: "2026-05-20T11:04:00.000Z",
          lastEventSeq: 2
        }
      ],
      pagination: {
        limit: 100,
        offset: 0,
        total: 2,
        hasMore: false
      }
    };

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-current-stale",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agenthub_u2001",
            userName: "Alex"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-current-stale")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: null
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("deduplicates tool timeline events by tool call id while preserving history order", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "history-tool-result",
        sessionId: "session-1",
        seq: 30,
        kind: "tool.result",
        payload: {
          data: {
            phase: "result",
            name: "web_fetch",
            toolCallId: "call-1"
          }
        },
        createdAt: "2026-05-27T10:23:31.000Z"
      }
    ]);
    const storedEvents: GatewayEvent[] = [
      {
        id: "stored-tool-result",
        sessionId: "session-1",
        seq: 9,
        kind: "tool.result",
        payload: {
          data: {
            phase: "result",
            name: "web_fetch",
            toolCallId: "call-1",
            meta: "from https://example.com (max 8000 chars)",
            result: {
              details: {
                status: "error",
                error: "timeout"
              }
            }
          }
        },
        createdAt: "2026-05-27T10:23:32.000Z"
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2
      },
      gateway,
      callbacks: {
        onSessionUpsert: async (session) => {
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        listSessionEvents: () => storedEvents,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-events-dedupe",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-events-dedupe")).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [
              {
                id: "history-tool-result",
                seq: 30,
                kind: "tool.result",
                payload: {
                  data: {
                    name: "web_fetch",
                    toolCallId: "call-1",
                    meta: "from https://example.com (max 8000 chars)",
                    result: {
                      details: {
                        status: "error",
                        error: "timeout"
                      }
                    }
                  }
                }
              }
            ],
            pagination: { limit: 10, offset: 0, total: 1, hasMore: false }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });
});

class FakeGateway {
  private sessions = new Map<string, GatewaySession>();
  private listeners = new Map<string, Set<(event: GatewayEvent) => void>>();
  private disconnectHandlers = new Map<string, Set<(error?: Error) => void>>();
  sentMessages: Array<{ sessionId: string; content: string }> = [];
  eventsToEmit?: GatewayEvent[];
  disconnectOnNextSend = false;
  disconnectCompletionDelayMs = 0;
  createdTitles: string[] = [];
  renames: Array<{ sessionId: string; title: string }> = [];
  controls: Array<{ sessionId: string; action: string }> = [];
  private createTitleFailures = new Map<string, string>();
  sessionPage?: { sessions: GatewaySession[]; pagination: { limit: number; offset: number; total?: number; hasMore: boolean } };
  messagesBySession = new Map<string, SessionMessage[]>();
  eventsBySession = new Map<string, GatewayEvent[]>();
  runtimeInfo?: {
    modelPrimary?: string;
    enabledSkills: string[];
    cronScheduler?: { enabled?: boolean; jobCount?: number; nextWakeAt?: string };
    cronTasks?: Array<{ id: string; name: string; enabled: boolean; schedule?: string }>;
  };

  async listSessions(): Promise<GatewaySession[]> {
    return [...this.sessions.values()];
  }

  async listSessionPage(): Promise<{ sessions: GatewaySession[]; pagination: { limit: number; offset: number; total?: number; hasMore: boolean } }> {
    return this.sessionPage ?? {
      sessions: [...this.sessions.values()],
      pagination: {
        limit: this.sessions.size,
        offset: 0,
        total: this.sessions.size,
        hasMore: false
      }
    };
  }

  async getRuntimeInfo(): Promise<{ enabledSkills: string[] }> {
    return this.runtimeInfo ?? { enabledSkills: [] };
  }

  async getHealth(): Promise<{ ok: boolean; status: "ok" }> {
    return { ok: true, status: "ok" };
  }

  async createSession(title: string): Promise<GatewaySession> {
    this.createdTitles.push(title);
    const failure = this.createTitleFailures.get(title);
    if (failure) {
      this.createTitleFailures.delete(title);
      throw new Error(failure);
    }
    const now = new Date().toISOString();
    const session: GatewaySession = {
      id: `session-${this.sessions.size + 1}`,
      title,
      status: "idle",
      hostKind: "qclaw",
      runnerCommand: "gateway",
      createdAt: now,
      updatedAt: now,
      lastEventSeq: 0
    };
    this.sessions.set(session.id, session);
    return session;
  }

  failCreateTitleOnce(title: string, message: string) {
    this.createTitleFailures.set(title, message);
  }

  upsertSession(session: GatewaySession) {
    this.sessions.set(session.id, session);
  }

  async getSession(sessionId: string): Promise<GatewaySession> {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error("unknown session");
    }
    return session;
  }

  async getSessionMessages(sessionId: string): Promise<SessionMessage[]> {
    return this.messagesBySession.get(sessionId) ?? [];
  }

  async sendMessage(sessionId: string, content: string): Promise<void> {
    this.sentMessages.push({ sessionId, content });
    setTimeout(() => {
      if (this.disconnectOnNextSend) {
        this.disconnectOnNextSend = false;
        this.emitDisconnect(sessionId, new Error("gateway stream disconnected"));
        if (this.disconnectCompletionDelayMs > 0) {
          setTimeout(() => {
            this.emit(sessionId, {
              id: "evt-disconnect-complete",
              sessionId,
              seq: 99,
              kind: "run.completed",
              payload: { ok: true },
              createdAt: new Date().toISOString()
            });
          }, this.disconnectCompletionDelayMs);
        }
        return;
      }
      const events =
        this.eventsToEmit ?? [
          {
            id: "evt-1",
            sessionId,
            seq: 1,
            kind: "assistant.delta",
            payload: { content: "Hello from local Claw" },
            createdAt: new Date().toISOString()
          },
          {
            id: "evt-2",
            sessionId,
            seq: 2,
            kind: "run.completed",
            payload: { ok: true },
            createdAt: new Date().toISOString()
          }
        ];
      for (const event of events) {
        this.emit(sessionId, { ...event, sessionId });
      }
    }, 10);
  }

  async controlSession(sessionId: string, action?: string, title?: string): Promise<void> {
    if (action) {
      this.controls.push({ sessionId, action });
    }
    if (action === "rename" && title) {
      this.renames.push({ sessionId, title });
      const session = this.sessions.get(sessionId);
      if (session) {
        this.sessions.set(sessionId, {
          ...session,
          title,
          updatedAt: new Date().toISOString()
        });
      }
    }
    return;
  }

  async listEvents(sessionId: string, afterSeq = 0): Promise<GatewayEvent[]> {
    return (this.eventsBySession.get(sessionId) ?? []).filter((event) => event.seq > afterSeq);
  }

  subscribe(
    sessionId: string,
    _afterSeq: number,
    handlers: {
      onEvent: (event: GatewayEvent) => void;
      onDisconnect: (error?: Error) => void;
    }
  ): () => void {
    const listeners = this.listeners.get(sessionId) ?? new Set<(event: GatewayEvent) => void>();
    listeners.add(handlers.onEvent);
    this.listeners.set(sessionId, listeners);
    const disconnectHandlers = this.disconnectHandlers.get(sessionId) ?? new Set<(error?: Error) => void>();
    disconnectHandlers.add(handlers.onDisconnect);
    this.disconnectHandlers.set(sessionId, disconnectHandlers);
    return () => {
      listeners.delete(handlers.onEvent);
      disconnectHandlers.delete(handlers.onDisconnect);
    };
  }

  async stop(): Promise<void> {
    return;
  }

  private emit(sessionId: string, event: GatewayEvent) {
    for (const listener of this.listeners.get(sessionId) ?? []) {
      listener(event);
    }
  }

  private emitDisconnect(sessionId: string, error?: Error) {
    for (const handler of this.disconnectHandlers.get(sessionId) ?? []) {
      handler(error);
    }
  }
}

async function createFakeHubServer(): Promise<{
  url: string;
  frames: Hub53AIOutgoingFrame[];
  connected: Promise<{ socket: WebSocket; headers: Record<string, string | undefined> }>;
  close: () => Promise<void>;
}> {
  const httpServer = createServer();
  const wsServer = new WebSocketServer({ server: httpServer });
  const frames: Hub53AIOutgoingFrame[] = [];
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
      if (payload?.action === "chat") {
        frames.push(payload as Hub53AIOutgoingFrame);
      }
      if (payload?.req_id && payload?.action !== "ping" && payload?.action !== "chat") {
        frames.push(payload as Hub53AIOutgoingFrame);
      }
    });
  });

  await new Promise<void>((resolve) => {
    httpServer.listen(0, "127.0.0.1", resolve);
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

function frameByReq(frames: Hub53AIOutgoingFrame[], reqId: string): Hub53AIOutgoingFrame | undefined {
  return frames.find((frame) => frame.req_id === reqId);
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
