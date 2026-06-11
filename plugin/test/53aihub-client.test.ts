import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
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
            frame.data.choices[0]?.delta.reasoning_content === "正在思考"
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
            event_kind: "assistant.delta",
            payload: {
              openclaw_timeline: {
                protocol_version: "openclaw.timeline.v2",
                segment_type: "answer",
                operation: "replace",
                visibility: "stream",
                final: false
              }
            }
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
              replace: true,
              openclaw_timeline: {
                protocol_version: "openclaw.timeline.v2",
                segment_type: "thinking",
                operation: "replace",
                visibility: "final",
                final: true
              }
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
              },
              openclaw_timeline: {
                protocol_version: "openclaw.timeline.v2",
                segment_type: "tool_call",
                operation: "replace",
                visibility: "final",
                final: true
              }
            }
          }
        });
        expect(streaming?.data.payload.openclaw_timeline.turn_id).toBe(
          thinking?.data.payload.openclaw_timeline.turn_id
        );
        expect(thinking?.data.payload.openclaw_timeline.turn_id).toBe(
          toolCall?.data.payload.openclaw_timeline.turn_id
        );
        const done = server.frames.find(
          (frame) =>
            frame.action === "chat" &&
            frame.status === "done" &&
            frame.data.event_kind === "run.completed"
        );
        expect(done).toMatchObject({
          data: {
            event_kind: "run.completed",
            payload: {
              event_id: "evt-done",
              event_kind: "run.completed",
              openclaw_timeline: {
                protocol_version: "openclaw.timeline.v2",
                segment_type: "run",
                operation: "close",
                visibility: "final",
                final: true
              }
            }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("returns the same OpenClaw timeline contract from realtime stream and sessions.events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-contract-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const createdAt = "2026-06-09T13:24:16.000Z";
    const events: GatewayEvent[] = [
      {
        id: "evt-run-start",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-doc-15", phase: "start" },
        createdAt
      },
      {
        id: "evt-thinking",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.thinking",
          payload: {
            content: "Need create a short file and report the path.",
            rawSeq: 21,
            state: "final",
            mode: "replace",
            replace: true
        },
        createdAt
      },
      {
        id: "evt-tool-call",
        sessionId: "session-1",
        seq: 3,
        kind: "tool.call",
        payload: {
          runId: "run-doc-15",
          data: {
            name: "exec",
            toolCallId: "tool-write-doc",
            args: { command: "printf 这是十五字文档 > /tmp/十五字文档.txt" }
          }
        },
        createdAt
      },
      {
        id: "evt-answer",
        sessionId: "session-1",
        seq: 4,
        kind: "assistant.message",
          payload: {
            content: "已创建一个15字的文档：/tmp/十五字文档.txt",
            rawSeq: 26,
            state: "final",
            mode: "replace",
            replace: true
        },
        createdAt
      },
      {
        id: "evt-files",
        sessionId: "session-1",
        seq: 5,
        kind: "process.step",
        payload: {
          runId: "run-doc-15",
          object: "process.step",
          process_step: {
            step_code: "output_files",
            name: "生成文件",
            status: "completed",
            message: "生成了 1 个文件",
            data: {
              files: [
                {
                  id: "file-doc-15",
                  file_name: "十五字文档.txt",
                  url: "file:///tmp/十五字文档.txt",
                  mime_type: "text/plain"
                }
              ],
              contract_version: "v1"
            },
            timestamp: 1_780_000_000
          }
        },
        createdAt
      },
      {
        id: "evt-done",
        sessionId: "session-1",
        seq: 6,
        kind: "run.completed",
        payload: { runId: "run-doc-15", phase: "end", status: "completed" },
        createdAt
      }
    ];
    gateway.eventsToEmit = events;
    gateway.eventsBySession.set("session-1", events);

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
        listSessionEvents: () => [],
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    const readTimeline = (frameOrEvent: any) =>
      frameOrEvent?.data?.payload?.openclaw_timeline ??
      frameOrEvent?.data?.process_step?.data?.openclaw_timeline ??
      frameOrEvent?.payload?.openclaw_timeline ??
      frameOrEvent?.payload?.process_step?.data?.openclaw_timeline;
    const contractOf = (value: any) => {
      const timeline = readTimeline(value);
      return timeline
        ? {
            turn_id: timeline.turn_id,
            segment_id: timeline.segment_id,
            segment_type: timeline.segment_type,
            operation: timeline.operation,
            visibility: timeline.visibility,
            final: timeline.final
          }
        : null;
    };

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-contract",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-contract",
            messages: [{ role: "user", content: "给我一个15字的文档" }]
          }
        })
      );

      await waitFor(() => {
        expect(server.frames.some((frame: any) => frame.action === "chat" && frame.status === "done")).toBe(true);
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-contract-events",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 20, offset: 0 }
        })
      );

      await waitFor(() => {
        const rpcFrame = frameByReq(server.frames, "rpc-contract-events");
        expect(rpcFrame).toMatchObject({ action: "sessions.events", status: "done" });

        const streamByKind = new Map<string, any>();
        for (const frame of server.frames as any[]) {
          if (frame.action !== "chat") continue;
          const kind = frame.data?.event_kind || (frame.data?.object === "process.step" ? "process.step" : "");
          if (!kind || streamByKind.has(kind)) continue;
          streamByKind.set(kind, frame);
        }
        const eventsByKind = new Map<string, any>(rpcFrame.data.events.map((event: any) => [event.kind, event]));

        for (const kind of ["assistant.thinking", "tool.call", "assistant.message", "process.step", "run.completed"]) {
          expect(contractOf(streamByKind.get(kind))).toEqual(contractOf(eventsByKind.get(kind)));
        }
        expect(contractOf(streamByKind.get("assistant.message"))?.segment_type).toBe("answer");
        expect(contractOf(streamByKind.get("assistant.thinking"))?.segment_type).toBe("thinking");
        expect(contractOf(streamByKind.get("process.step"))?.segment_type).toBe("output_files");
        expect(streamByKind.get("assistant.thinking")?.data?.payload?.runId).toBe("run-doc-15");
        expect(streamByKind.get("assistant.message")?.data?.payload?.runId).toBe("run-doc-15");
        expect(eventsByKind.get("assistant.thinking")?.payload?.runId).toBe("run-doc-15");
        expect(eventsByKind.get("assistant.message")?.payload?.runId).toBe("run-doc-15");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("uses tool call ids for OpenClaw tool timeline segments", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-tool-newyork",
        sessionId: "session-1",
        seq: 1,
        kind: "tool.call",
        payload: {
          data: {
            name: "exec",
            toolCallId: "chatcmpl-tool-newyork",
            args: { command: 'curl -s "wttr.in/NewYork?1"' }
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-tool-new-york",
        sessionId: "session-1",
        seq: 2,
        kind: "tool.call",
        payload: {
          data: {
            name: "exec",
            toolCallId: "chatcmpl-tool-new-york",
            args: { command: 'curl -s "wttr.in/New_York?1"' }
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-tool-nyc",
        sessionId: "session-1",
        seq: 3,
        kind: "tool.call",
        payload: {
          data: {
            name: "exec",
            tool_call_id: "call-nyc",
            args: { command: 'curl -s "wttr.in/NYC?1"' }
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
          req_id: "req-multi-tool-segments",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-multi-tool-segments",
            messages: [{ role: "user", content: "今天纽约天气怎么样？" }]
          }
        })
      );

      await waitFor(() => {
        const toolFrames = server.frames.filter(
          (frame) =>
            frame.req_id === "req-multi-tool-segments" &&
            frame.action === "chat" &&
            frame.status === "thinking" &&
            frame.data?.event_kind === "tool.call"
        );
        expect(toolFrames).toHaveLength(3);
        const segmentIds = toolFrames.map((frame) => frame.data.payload.openclaw_timeline.segment_id);
        expect(segmentIds).toEqual([
          expect.stringContaining("chatcmpl-tool-newyork"),
          expect.stringContaining("chatcmpl-tool-new-york"),
          expect.stringContaining("call-nyc")
        ]);
        expect(new Set(segmentIds).size).toBe(3);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("keeps repeated same-name OpenClaw tool calls distinct when call ids are missing", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-tool-newyork-call",
        sessionId: "session-1",
        seq: 21,
        kind: "tool.call",
        payload: {
          rawSeq: 21,
          data: {
            name: "exec",
            args: { command: 'curl -s "wttr.in/NewYork?1"' }
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-tool-newyork-result",
        sessionId: "session-1",
        seq: 25,
        kind: "tool.result",
        payload: {
          rawSeq: 25,
          data: {
            name: "exec",
            result: "New York UK weather"
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-tool-new-york-call",
        sessionId: "session-1",
        seq: 83,
        kind: "tool.call",
        payload: {
          rawSeq: 83,
          data: {
            name: "exec",
            args: { command: 'curl -s "wttr.in/New_York?1"' }
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-tool-new-york-result",
        sessionId: "session-1",
        seq: 87,
        kind: "tool.result",
        payload: {
          rawSeq: 87,
          data: {
            name: "exec",
            result: "New York City weather"
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-done",
        sessionId: "session-1",
        seq: 88,
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
          req_id: "req-repeated-exec-tools",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-repeated-exec-tools",
            messages: [{ role: "user", content: "今天纽约天气怎么样？" }]
          }
        })
      );

      await waitFor(() => {
        const toolFrames = server.frames.filter(
          (frame) =>
            frame.req_id === "req-repeated-exec-tools" &&
            frame.action === "chat" &&
            frame.status === "thinking" &&
            (frame.data?.event_kind === "tool.call" || frame.data?.event_kind === "tool.result")
        );
        expect(toolFrames).toHaveLength(4);
        const segmentIds = toolFrames.map((frame) => frame.data.payload.openclaw_timeline.segment_id);
        expect(segmentIds).toEqual([
          expect.stringContaining("tool_call:exec:21"),
          expect.stringContaining("tool_result:exec:25"),
          expect.stringContaining("tool_call:exec:83"),
          expect.stringContaining("tool_result:exec:87")
        ]);
        expect(new Set(segmentIds).size).toBe(4);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("emits standard output_files process steps for gateway file events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-tool-result",
        sessionId: "session-1",
        seq: 1,
        kind: "tool.result",
        payload: {
          data: {
            name: "write_file",
            content: "普通工具文本不应被误判为文件",
            output_files: [
              {
                id: "file-report",
                file_name: "output/report.md",
                url: "https://example.com/report.md",
                mime_type: "text/markdown",
                size: 128
              }
            ]
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 2,
        kind: "run.completed",
        payload: {
          output_files: [
            {
              id: "file-report",
              file_name: "output/report.md",
              url: "https://example.com/report.md",
              mime_type: "text/markdown",
              size: 128
            }
          ]
        },
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
          req_id: "req-output-files",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-files",
            messages: [{ role: "user", content: "生成报告" }]
          }
        })
      );

      await waitFor(() => {
        const outputSteps = server.frames.filter(
          (frame) =>
            frame.req_id === "req-output-files" &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        );
        expect(outputSteps).toHaveLength(1);
        expect(outputSteps[0]).toMatchObject({
          status: "streaming",
          data: {
            object: "process.step",
            process_step: {
              step_code: "output_files",
              status: "completed",
              data: {
                contract_version: "v1",
                files: [
                  {
                    id: "file-report",
                    file_name: "output/report.md",
                    url: "https://example.com/report.md",
                    mime_type: "text/markdown",
                    size: 128
                  }
                ],
                media_attachments: [
                  {
                    id: "file-report",
                    file_name: "output/report.md",
                    url: "https://example.com/report.md",
                    mime_type: "text/markdown",
                    size: 128,
                    kind: "text"
                  }
                ],
                media_contract_version: "v1"
              }
            }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("emits same-name output_file revisions when the file snapshot changes", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-draft-file",
        sessionId: "session-1",
        seq: 1,
        kind: "tool.result",
        payload: {
          data: {
            name: "write_file",
            output_files: [
              {
                id: "local-draft",
                file_name: "report.txt",
                mime_type: "text/plain",
                size: 5,
                base64: Buffer.from("wrong").toString("base64")
              }
            ]
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-final-file",
        sessionId: "session-1",
        seq: 2,
        kind: "tool.result",
        payload: {
          data: {
            name: "write_file",
            output_files: [
              {
                id: "local-final",
                file_name: "report.txt",
                mime_type: "text/plain",
                size: 7,
                base64: Buffer.from("correct").toString("base64")
              }
            ]
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 3,
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
          req_id: "req-output-file-revisions",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-file-revisions",
            messages: [{ role: "user", content: "生成并修正同名文件" }]
          }
        })
      );

      await waitFor(() => {
        const outputSteps = server.frames.filter(
          (frame) =>
            frame.req_id === "req-output-file-revisions" &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        );
        expect(outputSteps).toHaveLength(2);
        expect(outputSteps.map((frame) => frame.data.process_step.data.files[0].id)).toEqual([
          "local-draft",
          "local-final"
        ]);
        expect(outputSteps.map((frame) => frame.data.process_step.data.files[0].base64)).toEqual([
          Buffer.from("wrong").toString("base64"),
          Buffer.from("correct").toString("base64")
        ]);
        const segmentIds = outputSteps.map(
          (frame) => frame.data.process_step.data.openclaw_timeline?.segment_id
        );
        expect(new Set(segmentIds).size).toBe(2);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("emits output_files for files created in the local workspace during a Hub run", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    await mkdir(workspaceDir, { recursive: true });
    await writeFile(join(workspaceDir, "existing.txt"), "already here");

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 2,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];
    gateway.beforeEmit = async () => {
      await writeFile(join(workspaceDir, "created.md"), "# created\n");
      await writeFile(join(workspaceDir, ".hidden.md"), "# hidden\n");
    };

    const bridge = createHub53AIBridge({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2,
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspaceDir],
        createdFilesMaxFileBytes: 1024,
        createdFilesMaxCount: 5
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
          req_id: "req-local-files",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-local-files",
            messages: [{ role: "user", content: "创建本地文件" }]
          }
        })
      );

      await waitFor(() => {
        const outputStep = server.frames.find(
          (frame) =>
            frame.req_id === "req-local-files" &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        );
        expect(outputStep).toBeTruthy();
        const files = outputStep!.data.process_step.data.files;
        expect(files).toHaveLength(1);
        expect(files[0]).toMatchObject({
          file_name: "created.md",
          mime_type: "text/markdown",
          size: 10,
          base64: Buffer.from("# created\n").toString("base64")
        });
        expect(files[0].url).toBeUndefined();
        expect(files[0].id).toMatch(/^local-/);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("emits local output_files when a final assistant message mentions the created file", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    const reportPath = join(workspaceDir, "classics_2026.txt");
    await mkdir(workspaceDir, { recursive: true });

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.beforeEmit = async () => {
      await writeFile(reportPath, "created from final assistant message\n");
    };
    gateway.eventsToEmit = [
      {
        id: "evt-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-message",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.message",
        payload: {
          content: `已为您保存为 txt 文件：${reportPath}`,
          state: "final",
          mode: "replace"
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 3,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2,
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspaceDir],
        createdFilesMaxFileBytes: 1024,
        createdFilesMaxCount: 5
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
          req_id: "req-final-message-local-files",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-final-message-local-files",
            messages: [{ role: "user", content: "创建并返回本地文件路径" }]
          }
        })
      );

      await waitFor(() => {
        const outputStep = server.frames.find(
          (frame) =>
            frame.req_id === "req-final-message-local-files" &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        );
        expect(outputStep).toBeTruthy();
        const files = outputStep!.data.process_step.data.files;
        expect(files).toHaveLength(1);
        expect(files[0]).toMatchObject({
          file_name: "classics_2026.txt",
          mime_type: "text/plain",
          size: 37,
          base64: Buffer.from("created from final assistant message\n").toString("base64")
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("emits output_files for referenced local workspace files that were modified during the Hub run", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    const reportPath = join(workspaceDir, "novels_2026.txt");
    await mkdir(workspaceDir, { recursive: true });
    await writeFile(reportPath, "already generated");

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.beforeEmit = async () => {
      await writeFile(reportPath, "generated now");
    };
    gateway.eventsToEmit = [
      {
        id: "evt-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-message",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.message",
        payload: { content: `已保存为 ${reportPath}` },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 3,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2,
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspaceDir],
        createdFilesMaxFileBytes: 1024,
        createdFilesMaxCount: 5
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
          req_id: "req-referenced-local-files",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-referenced-local-files",
            messages: [{ role: "user", content: "返回已有本地文件" }]
          }
        })
      );

      await waitFor(() => {
        const outputStep = server.frames.find(
          (frame) =>
            frame.req_id === "req-referenced-local-files" &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        );
        expect(outputStep).toBeTruthy();
        const files = outputStep!.data.process_step.data.files;
        expect(files).toHaveLength(1);
        expect(files[0]).toMatchObject({
          file_name: "novels_2026.txt",
          mime_type: "text/plain",
          size: 13,
          base64: Buffer.from("generated now").toString("base64")
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not emit output_files for referenced local workspace files that were unchanged during the Hub run", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    const reportPath = join(workspaceDir, "old-report.txt");
    await mkdir(workspaceDir, { recursive: true });
    await writeFile(reportPath, "old file");

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-message",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.message",
        payload: { content: `参考文件 ${reportPath}` },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 3,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];

    const bridge = createHub53AIBridge({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: server.url,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2,
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspaceDir],
        createdFilesMaxFileBytes: 1024,
        createdFilesMaxCount: 5
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
          req_id: "req-unchanged-referenced-local-files",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-unchanged-referenced-local-files",
            messages: [{ role: "user", content: "提到了旧文件" }]
          }
        })
      );

      await waitFor(() => {
        expect(
          server.frames.some(
            (frame) =>
              frame.req_id === "req-unchanged-referenced-local-files" &&
              frame.action === "chat" &&
              frame.data?.object === "chat.completion.chunk" &&
              frame.status === "done"
          )
        ).toBe(true);
      });
      expect(
        server.frames.some(
          (frame) =>
            frame.req_id === "req-unchanged-referenced-local-files" &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        )
      ).toBe(false);
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
        const readThinkingDelta = (frame: any) =>
          frame.data.choices[0]?.delta.reasoning_content ?? frame.data.choices[0]?.delta.content;
        expect(chatFrames.some((frame) => frame.status === "thinking")).toBe(true);
        expect(
          chatFrames.some(
            (frame) =>
              frame.status === "thinking" &&
              readThinkingDelta(frame) === "Thinking through the request"
          )
        ).toBe(true);
        const thinkingText = chatFrames
          .filter((frame) => frame.status === "thinking")
          .map(readThinkingDelta)
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

  it("keeps reconnecting after max reconnect attempts until 53AIHub is available again", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-reconnect-"));
    cleanupPaths.push(stateDir);
    const port = await getFreePort();
    const gateway = new FakeGateway();
    const statusErrors: string[] = [];

    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: `ws://127.0.0.1:${port}`,
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
        onStatusChange: () => {
          const status = bridge.getStatus();
          if (status.lastError) {
            statusErrors.push(status.lastError);
          }
        }
      }
    });

    await bridge.start();
    try {
      await waitFor(() => {
        expect(bridge.getStatus()).toMatchObject({
          connectionStatus: "error",
          lastError: "Max reconnect attempts (2) reached; continuing background reconnects"
        });
      });

      const recoveredServer = await createFakeHubServer(port);
      cleanupServers.push(recoveredServer.close);
      await recoveredServer.connected;

      await waitFor(() => {
        expect(bridge.getStatus()).toMatchObject({
          connectionStatus: "connected",
          lastError: undefined
        });
      });
      expect(statusErrors.some((lastError) => lastError.includes("continuing background reconnects"))).toBe(true);
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

  it("releases the 53AIHub chat queue after stop control even when the stopped run has no terminal event", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-stop-queue-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsToEmit = [];

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
          req_id: "req-stopped-run",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: "chat-stop-queue",
            messages: [{ role: "user", content: "first message" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([{ sessionId: "session-1", content: "first message" }]);
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-stop-stuck-run",
          action: "sessions.control",
          status: "request",
          data: { session_id: "session-1", action: "stop" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-stop-stuck-run")).toMatchObject({
          action: "sessions.control",
          status: "done"
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "req-after-stopped-run",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: "chat-stop-queue",
            messages: [{ role: "user", content: "second message" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([
          { sessionId: "session-1", content: "first message" },
          { sessionId: "session-1", content: "second message" }
        ]);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("sends chat frames directly to existing 53AIHub OpenClaw session ids", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const existingSessionId = "agent:main:dashboard:0ea52c76-4a2d-410f-985a-ed1771a67e28";
    gateway.upsertSession({
      id: existingSessionId,
      title: "53AI Hub-user-1：Existing OpenClaw session",
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
      title: "53AI Hub-user-1：Disconnect test",
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

  it("streams full replacement snapshots without appending stale draft text", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const draft = "旧草稿：天气未知";
    const finalText = "上海今日天气总体良好，无雨，傍晚转晴。";
    gateway.eventsToEmit = [
      {
        id: "evt-draft",
        sessionId: "session-1",
        seq: 1,
        kind: "assistant.delta",
        payload: { content: draft, mode: "append", replace: false },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-replace",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.delta",
        payload: { content: finalText, mode: "replace", replace: true },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-final",
        sessionId: "session-1",
        seq: 3,
        kind: "assistant.message",
        payload: { content: finalText, mode: "replace", replace: true, state: "final" },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 4,
        kind: "run.completed",
        payload: { content: finalText, mode: "replace", replace: true },
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
          req_id: "req-replace",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            messages: [{ role: "user", content: "weather" }]
          }
        })
      );

      await waitFor(() => {
        const streamingFrames = server.frames.filter((frame) => frame.action === "chat" && frame.status === "streaming");
        expect(streamingFrames.map((frame) => frame.data.choices[0]?.delta.content ?? "")).toEqual([draft, finalText]);
        expect(streamingFrames.map((frame) => frame.data.replace)).toEqual([false, true]);
        expect(streamingFrames.map((frame) => frame.data.payload?.mode)).toEqual(["append", "replace"]);
        expect(streamingFrames.map((frame) => frame.data.payload?.replace)).toEqual([false, true]);
        expect(streamingFrames.map((frame) => frame.data.choices[0]?.delta.content ?? "")).not.toContain(
          `${draft}${finalText}`
        );
        const done = server.frames.find((frame) => frame.action === "chat" && frame.status === "done");
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

  it("restores sessions.current from the latest matching 53AIHub session without using stable mappings", async () => {
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
          title: "53AI Hub-Alex：最新会话",
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
          data: {
            id: "session-b",
            title: "53AI Hub-Alex：最新会话"
          }
        });
      });

      const persisted = JSON.parse(await readFile(join(stateDir, "claw-control-center-53aihub.json"), "utf8"));
      expect(persisted.mappings.agenthub_u2001).toBe("deleted-session");
    } finally {
      await bridge.stop();
    }
  });

  it("does not send messages to a stale control center mapping and restores the latest 53AIHub session", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-send-control-"));
    cleanupPaths.push(stateDir);
    const statePath = join(stateDir, "claw-control-center-53aihub.json");
    await writeFile(
      statePath,
      `${JSON.stringify({ mappings: { agenthub_u2001: "control-session" }, outbox: [] }, null, 2)}\n`
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "control-session",
      title: "Claw Control Center",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T09:00:00.000Z",
      updatedAt: "2026-05-20T09:04:00.000Z",
      lastEventSeq: 1
    });
    gateway.sessionPage = {
      sessions: [
        {
          id: "hub-old",
          title: "53AI Hub-Alex：旧会话",
          status: "completed",
          hostKind: "qclaw",
          runnerCommand: "gateway",
          createdAt: "2026-05-20T10:00:00.000Z",
          updatedAt: "2026-05-20T10:04:00.000Z",
          lastEventSeq: 1
        },
        {
          id: "hub-latest",
          title: "53AI Hub-Alex：最近会话",
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
    gateway.upsertSession(gateway.sessionPage.sessions[0]);
    gateway.upsertSession(gateway.sessionPage.sessions[1]);

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
          req_id: "req-stale-control-send",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: "agenthub_u2001",
            metadata: { userName: "Alex" },
            messages: [{ role: "user", content: "今天天气怎么样" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([
          {
            sessionId: "hub-latest",
            content: "今天天气怎么样"
          }
        ]);
      });
      expect(gateway.createdTitles).toEqual([]);

      const persisted = JSON.parse(await readFile(statePath, "utf8"));
      expect(persisted.mappings.agenthub_u2001).toBe("control-session");
    } finally {
      await bridge.stop();
    }
  });

  it("does not persist synthetic bridge tool placeholder thinking events for the resolved 53AIHub session", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-tool-control-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      `${JSON.stringify({ mappings: { agenthub_u2001: "control-session" }, outbox: [] }, null, 2)}\n`
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "control-session",
      title: "Claw Control Center",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T09:00:00.000Z",
      updatedAt: "2026-05-20T09:04:00.000Z",
      lastEventSeq: 1
    });
    gateway.sessionPage = {
      sessions: [
        {
          id: "hub-latest",
          title: "53AI Hub-Alex：最近会话",
          status: "completed",
          hostKind: "openclaw",
          runnerCommand: "gateway",
          createdAt: "2026-05-20T11:00:00.000Z",
          updatedAt: "2026-05-20T11:04:00.000Z",
          lastEventSeq: 2
        }
      ],
      pagination: {
        limit: 100,
        offset: 0,
        total: 1,
        hasMore: false
      }
    };
    gateway.upsertSession(gateway.sessionPage.sessions[0]);
    gateway.eventsToEmit = [
      {
        id: "tool-call",
        sessionId: "hub-latest",
        seq: 1,
        kind: "tool.call",
        payload: {
          data: {
            name: "web_search",
            toolCallId: "call-1",
            args: { query: "weather" }
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "done",
        sessionId: "hub-latest",
        seq: 2,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];
    const bridgeThinkingEvents: GatewayEvent[] = [];

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
        onBridgeThinkingEvent: async (event) => {
          bridgeThinkingEvents.push(event);
        },
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-tool-stale-control",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: "agenthub_u2001",
            metadata: { userName: "Alex" },
            messages: [{ role: "user", content: "查一下天气" }]
          }
        })
      );

      const toolEvents = bridgeThinkingEvents.filter((event) =>
        String(event.payload?.content ?? "").includes("web_search")
      );
      expect(toolEvents).toHaveLength(0);
    } finally {
      await bridge.stop();
    }
  });

  it("ignores sessions.current mappings that point at the control center session", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-current-control-"));
    cleanupPaths.push(stateDir);
    const statePath = join(stateDir, "claw-control-center-53aihub.json");
    await writeFile(
      statePath,
      `${JSON.stringify({ mappings: { agenthub_u2001: "control-session" }, outbox: [] }, null, 2)}\n`
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "control-session",
      title: "Claw Control Center",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T10:00:00.000Z",
      updatedAt: "2026-05-20T10:04:00.000Z",
      lastEventSeq: 1
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
          req_id: "rpc-current-control",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agenthub_u2001",
            userName: "Alex"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-current-control")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: null
        });
      });

      const persisted = JSON.parse(await readFile(statePath, "utf8"));
      expect(persisted.mappings.agenthub_u2001).toBe("control-session");
    } finally {
      await bridge.stop();
    }
  });

  it("does not treat an explicit OpenClaw session id as the default current session", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-current-agent-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "agent:main:dashboard:hub",
      title: "53AI Hub-Alex：测试",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T10:00:00.000Z",
      updatedAt: "2026-05-20T10:04:00.000Z",
      lastEventSeq: 1
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
          req_id: "rpc-current-agent",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agent:main:dashboard:hub"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-current-agent")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: null
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not allow an explicit control center session id to become the current 53AIHub session", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-current-explicit-control-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "agent:main:dashboard:control",
      title: "Claw Control Center",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T10:00:00.000Z",
      updatedAt: "2026-05-20T10:04:00.000Z",
      lastEventSeq: 1
    });
    gateway.sessionPage = {
      sessions: [
        {
          id: "agent:main:dashboard:hub-latest",
          title: "53AI Hub-Alex：最近会话",
          status: "completed",
          hostKind: "openclaw",
          runnerCommand: "gateway",
          createdAt: "2026-05-20T11:00:00.000Z",
          updatedAt: "2026-05-20T11:04:00.000Z",
          lastEventSeq: 2
        }
      ],
      pagination: {
        limit: 100,
        offset: 0,
        total: 1,
        hasMore: false
      }
    };
    gateway.upsertSession(gateway.sessionPage.sessions[0]);

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
          req_id: "rpc-current-explicit-control",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agent:main:dashboard:control",
            userName: "Alex"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-current-explicit-control")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: {
            id: "agent:main:dashboard:hub-latest",
            title: "53AI Hub-Alex：最近会话"
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("sends a message to an explicitly selected control center session id", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-send-explicit-control-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "agent:main:dashboard:control",
      title: "Claw Control Center",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T10:00:00.000Z",
      updatedAt: "2026-05-20T10:04:00.000Z",
      lastEventSeq: 1
    });
    gateway.sessionPage = {
      sessions: [
        {
          id: "agent:main:dashboard:hub-latest",
          title: "53AI Hub-Alex：最近会话",
          status: "completed",
          hostKind: "openclaw",
          runnerCommand: "gateway",
          createdAt: "2026-05-20T11:00:00.000Z",
          updatedAt: "2026-05-20T11:04:00.000Z",
          lastEventSeq: 2
        }
      ],
      pagination: {
        limit: 100,
        offset: 0,
        total: 1,
        hasMore: false
      }
    };
    gateway.upsertSession(gateway.sessionPage.sessions[0]);

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
          req_id: "req-explicit-control-send",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: "agent:main:dashboard:control",
            metadata: { userName: "Alex" },
            messages: [{ role: "user", content: "不要发到控制中心" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([
          {
            sessionId: "agent:main:dashboard:control",
            content: "不要发到控制中心"
          }
        ]);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("uses a known 53AIHub title when gateway reports the same session as the control center", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-known-title-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const hubSession = {
      id: "agent:main:dashboard:hub-session",
      title: "53AI Hub-Alex：最近会话",
      status: "completed" as const,
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T11:00:00.000Z",
      updatedAt: "2026-05-20T11:04:00.000Z",
      lastEventSeq: 2
    };
    gateway.sessionPage = {
      sessions: [
        {
          ...hubSession,
          title: "Claw Control Center"
        }
      ],
      pagination: {
        limit: 100,
        offset: 0,
        total: 1,
        hasMore: false
      }
    };
    gateway.upsertSession(gateway.sessionPage.sessions[0]);

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
        onStatusChange: () => undefined,
        listKnownSessions: () => [hubSession]
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-known-current",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agenthub_u2001",
            userName: "Alex"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-known-current")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: {
            id: "agent:main:dashboard:hub-session",
            title: "53AI Hub-Alex：最近会话"
          }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "req-known-send",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            metadata: { userName: "Alex" },
            messages: [{ role: "user", content: "继续这个会话" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([
          {
            sessionId: "agent:main:dashboard:hub-session",
            content: "继续这个会话"
          }
        ]);
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-known-list",
          action: "sessions.list",
          status: "request",
          data: {
            limit: 10,
            offset: 0
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-known-list")).toMatchObject({
          action: "sessions.list",
          status: "done",
          data: {
            sessions: [
              {
                id: "agent:main:dashboard:hub-session",
                title: "53AI Hub-Alex：最近会话"
              }
            ]
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not restore or list known 53AIHub sessions that are absent from the gateway list", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-stale-known-session-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const staleHubSession = {
      id: "agent:main:dashboard:deleted-hub-session",
      title: "53AI Hub-Alex：已删除会话",
      status: "completed" as const,
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T11:00:00.000Z",
      updatedAt: "2026-05-20T11:04:00.000Z",
      lastEventSeq: 2
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
      gateway: new FakeGateway(),
      callbacks: {
        onSessionUpsert: async () => undefined,
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined,
        listKnownSessions: () => [staleHubSession]
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-stale-current",
          action: "sessions.current",
          status: "request",
          data: {
            chat_id: "agenthub_u2001",
            userName: "Alex"
          }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-stale-list",
          action: "sessions.list",
          status: "request",
          data: {
            limit: 10,
            offset: 0
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-stale-current")).toMatchObject({
          action: "sessions.current",
          status: "done",
          data: null
        });
        expect(frameByReq(server.frames, "rpc-stale-list")).toMatchObject({
          action: "sessions.list",
          status: "done",
          data: {
            sessions: []
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("preserves a known 53AIHub title when sending to an explicit Hub session id", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-explicit-known-title-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const hubSession = {
      id: "agent:main:dashboard:hub-session",
      title: "53AI Hub-Alex：最近会话",
      status: "completed" as const,
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T11:00:00.000Z",
      updatedAt: "2026-05-20T11:04:00.000Z",
      lastEventSeq: 2
    };
    gateway.upsertSession({
      ...hubSession,
      title: "Claw Control Center"
    });

    const upsertedSessions: GatewaySession[] = [];
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
          upsertedSessions.push(session);
          gateway.upsertSession(session);
        },
        onUserMessage: async () => undefined,
        onSessionStatus: async () => undefined,
        onEnsureSessionStream: async () => undefined,
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined,
        listKnownSessions: () => [hubSession]
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-explicit-known-send",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: "agent:main:dashboard:hub-session",
            metadata: { userName: "Alex" },
            messages: [{ role: "user", content: "继续显式会话" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([
          {
            sessionId: "agent:main:dashboard:hub-session",
            content: "继续显式会话"
          }
        ]);
      });

      expect(upsertedSessions[0]).toMatchObject({
        id: "agent:main:dashboard:hub-session",
        title: "53AI Hub-Alex：最近会话"
      });
    } finally {
      await bridge.stop();
    }
  });

  it("uses a 53AIHub conversation title hint when sending to an explicit polluted session id", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-explicit-title-hint-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "agent:main:dashboard:polluted-hub",
      title: "Claw Control Center",
      status: "completed",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-05-20T11:00:00.000Z",
      updatedAt: "2026-05-20T11:04:00.000Z",
      lastEventSeq: 2
    });

    const upsertedSessions: GatewaySession[] = [];
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
          upsertedSessions.push(session);
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
          req_id: "req-explicit-title-hint-send",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: "agent:main:dashboard:polluted-hub",
            metadata: {
              userName: "Alex",
              openclaw_conversation_title: "53AI Hub-Alex：最近会话"
            },
            messages: [{ role: "user", content: "继续显式污染会话" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toEqual([
          {
            sessionId: "agent:main:dashboard:polluted-hub",
            content: "继续显式污染会话"
          }
        ]);
      });

      expect(upsertedSessions[0]).toMatchObject({
        id: "agent:main:dashboard:polluted-hub",
        title: "53AI Hub-Alex：最近会话"
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

  it("filters synthetic bridge tool placeholder thinking events from sessions.events history", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-weather" },
        createdAt: "2026-06-09T07:15:36.000Z"
      },
      {
        id: "tool-call-1",
        sessionId: "session-1",
        seq: 2,
        kind: "tool.call",
        payload: {
          data: {
            name: "exec",
            args: { command: "curl -s wttr.in/Chongqing?format=j1" }
          }
        },
        createdAt: "2026-06-09T07:15:40.000Z"
      },
      {
        id: "tool-result-1",
        sessionId: "session-1",
        seq: 3,
        kind: "tool.result",
        payload: {
          data: {
            name: "exec",
            result: { output: "{\"current_condition\":[]}" }
          }
        },
        createdAt: "2026-06-09T07:15:42.000Z"
      },
      {
        id: "assistant-final",
        sessionId: "session-1",
        seq: 4,
        kind: "assistant.message",
        payload: { content: "重庆明天天气多云。" },
        createdAt: "2026-06-09T07:15:45.000Z"
      }
    ]);

    const storedEvents: GatewayEvent[] = [
      {
        id: "session-1:hub-thinking:1",
        sessionId: "session-1",
        seq: -8000000000000001,
        kind: "assistant.thinking",
        payload: { content: "Used tool exec", source: "hub53ai" },
        createdAt: "2026-06-09T07:15:41.000Z"
      },
      {
        id: "session-1:hub-thinking:2",
        sessionId: "session-1",
        seq: -8000000000000000,
        kind: "assistant.thinking",
        payload: { content: "Tool exec returned a result", source: "hub53ai" },
        createdAt: "2026-06-09T07:15:43.000Z"
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
          req_id: "rpc-events-filter-placeholders",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-events-filter-placeholders")).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [
              { id: "run-started", kind: "run.started" },
              { id: "tool-call-1", kind: "tool.call" },
              { id: "tool-result-1", kind: "tool.result" },
              { id: "assistant-final", kind: "assistant.message" }
            ],
            pagination: { limit: 10, offset: 0, total: 4, hasMore: false }
          }
        });
        const frame = frameByReq(server.frames, "rpc-events-filter-placeholders");
        expect(frame.data.events.map((event: any) => event.payload?.content)).not.toContain("Used tool exec");
        expect(frame.data.events.map((event: any) => event.payload?.content)).not.toContain("Tool exec returned a result");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("normalizes mismatched protocol segment types in sessions.events history", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "tool-result-1",
        sessionId: "session-1",
        seq: 3,
        kind: "tool.result",
        payload: {
          data: {
            name: "exec",
            result: { output: "Shenyang: sunny 25C" }
          },
          segment_type: "answer",
          openclaw_timeline: {
            protocol_version: "openclaw.timeline.v2",
            turn_id: "session-1:turn:weather",
            segment_id: "session-1:turn:weather:answer:0",
            segment_type: "answer",
            segment_index: 2,
            delta_index: 0,
            operation: "replace",
            visibility: "final",
            final: true
          }
        },
        createdAt: "2026-06-09T07:15:42.000Z"
      }
    ]);

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
        listSessionEvents: () => [],
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-events-normalize-segment-type",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-events-normalize-segment-type")).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [
              {
                id: "tool-result-1",
                kind: "tool.result",
                payload: {
                  segment_type: "tool_result",
                  openclaw_timeline: {
                    segment_type: "tool_result"
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

  it("adds OpenClaw history message seq metadata without replacing stream rawSeq", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "session-1:history:163:tool-call:chatcmpl-tool-new-york",
        sessionId: "session-1",
        seq: 1631,
        kind: "tool.call",
        payload: {
          rawSeq: 83,
          data: {
            phase: "update",
            name: "exec",
            toolCallId: "chatcmpl-tool-new-york",
            args: { command: "curl -s \"wttr.in/New_York?1\"" }
          }
        },
        createdAt: "2026-06-09T13:24:16.000Z"
      }
    ]);

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
        listSessionEvents: () => [],
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-events-history-message-seq",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-events-history-message-seq")).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [
              {
                id: "session-1:history:163:tool-call:chatcmpl-tool-new-york",
                kind: "tool.call",
                payload: {
                  rawSeq: 83,
                  messageSeq: 163,
                  message_seq: 163,
                  data: {
                    toolCallId: "chatcmpl-tool-new-york"
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

  it("drops live thinking snapshots superseded by OpenClaw history thinking events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "session-1:history:163:thinking",
        sessionId: "session-1",
        seq: 1630,
        kind: "assistant.thinking",
        payload: {
          rawSeq: 163,
          content: "NewYork is ambiguous, try New_York next."
        },
        createdAt: "2026-06-09T13:24:15.000Z"
      },
      {
        id: "session-1:thinking:1675",
        sessionId: "session-1",
        seq: 1675,
        kind: "assistant.thinking",
        payload: {
          rawSeq: 163,
          content: "NewYork is ambiguous, try New_York next."
        },
        createdAt: "2026-06-09T13:25:12.000Z"
      },
      {
        id: "session-1:history:163:tool-call:chatcmpl-tool-new-york",
        sessionId: "session-1",
        seq: 1631,
        kind: "tool.call",
        payload: {
          rawSeq: 83,
          data: {
            phase: "update",
            name: "exec",
            toolCallId: "chatcmpl-tool-new-york",
            args: { command: "curl -s \"wttr.in/New_York?1\"" }
          }
        },
        createdAt: "2026-06-09T13:24:16.000Z"
      }
    ]);

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
        listSessionEvents: () => [],
        getLastEventSeq: () => 0,
        onStatusChange: () => undefined
      }
    });

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-events-history-thinking-dedupe",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-events-history-thinking-dedupe");
        expect(frame).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [
              { id: "session-1:history:163:thinking", kind: "assistant.thinking" },
              { id: "session-1:history:163:tool-call:chatcmpl-tool-new-york", kind: "tool.call" }
            ],
            pagination: { limit: 10, offset: 0, total: 2, hasMore: false }
          }
        });
        expect(frame.data.events.map((event: any) => event.id)).not.toContain("session-1:thinking:1675");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("deduplicates assistant message echoes while preserving the richer chat final event", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const finalText = "上海今日天气总体良好，无雨。";
    gateway.eventsBySession.set("session-1", [
      {
        id: "run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-weather" },
        createdAt: "2026-06-09T07:15:36.000Z"
      },
      {
        id: "session-message-final",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.message",
        payload: { content: finalText },
        createdAt: "2026-06-09T07:15:52.000Z"
      },
      {
        id: "chat-final",
        sessionId: "session-1",
        seq: 3,
        kind: "assistant.message",
        payload: {
          content: finalText,
          runId: "run-weather",
          rawSeq: 46,
          state: "final",
          mode: "replace"
        },
        createdAt: "2026-06-09T07:16:02.000Z"
      }
    ]);

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
          req_id: "rpc-assistant-echo-dedupe",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-assistant-echo-dedupe")).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [
              { id: "run-started", kind: "run.started" },
              {
                id: "chat-final",
                seq: 3,
                kind: "assistant.message",
                payload: {
                  content: finalText,
                  runId: "run-weather",
                  rawSeq: 46,
                  state: "final",
                  mode: "replace"
                }
              }
            ],
            pagination: { limit: 10, offset: 0, total: 2, hasMore: false }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("collapses a provisional session.message answer into the later chat replace final snapshot", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const provisionalText = "搜索结果主要是网络小说和新书，让我重新搜索更全面的经典小说推荐。";
    const finalText = "十部经典小说推荐如下：一、红楼梦。二、百年孤独。三、堂吉诃德。四、战争与和平。";
    gateway.eventsBySession.set("session-1", [
      {
        id: "run-started",
        sessionId: "session-1",
        seq: 10,
        kind: "run.started",
        payload: { runId: "run-books" },
        createdAt: "2026-06-09T07:15:36.000Z"
      },
      {
        id: "session-message-final",
        sessionId: "session-1",
        seq: 11,
        kind: "assistant.message",
        payload: { content: provisionalText },
        createdAt: "2026-06-09T07:15:52.000Z"
      },
      {
        id: "chat-replace-delta",
        sessionId: "session-1",
        seq: 12,
        kind: "assistant.delta",
        payload: {
          content: finalText,
          runId: "run-books",
          rawSeq: 926,
          state: "delta",
          mode: "replace",
          replace: true
        },
        createdAt: "2026-06-09T07:16:00.000Z"
      },
      {
        id: "chat-final",
        sessionId: "session-1",
        seq: 13,
        kind: "assistant.message",
        payload: {
          content: finalText,
          runId: "run-books",
          rawSeq: 926,
          state: "final",
          mode: "replace"
        },
        createdAt: "2026-06-09T07:16:02.000Z"
      },
      {
        id: "run-completed",
        sessionId: "session-1",
        seq: 14,
        kind: "run.completed",
        payload: { runId: "run-books" },
        createdAt: "2026-06-09T07:16:03.000Z"
      }
    ]);

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
          req_id: "rpc-assistant-replace-dedupe",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-assistant-replace-dedupe")).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [
              { id: "run-started", kind: "run.started" },
              {
                id: "chat-final",
                seq: 13,
                kind: "assistant.message",
                payload: {
                  content: finalText,
                  runId: "run-books",
                  rawSeq: 926,
                  state: "final",
                  mode: "replace"
                }
              },
              { id: "run-completed", kind: "run.completed" }
            ],
            pagination: { limit: 10, offset: 0, total: 3, hasMore: false }
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
  beforeEmit?: (sessionId: string) => void | Promise<void>;
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
    setTimeout(async () => {
      await this.beforeEmit?.(sessionId);
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

async function createFakeHubServer(port?: number): Promise<{
  url: string;
  port: number;
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

  await new Promise<void>((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(port ?? 0, "127.0.0.1", () => {
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
    port: address.port,
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

async function getFreePort(): Promise<number> {
  const httpServer = createServer();
  await new Promise<void>((resolve) => {
    httpServer.listen(0, "127.0.0.1", resolve);
  });
  const address = httpServer.address();
  if (!address || typeof address === "string") {
    throw new Error("failed to reserve test port");
  }
  const port = address.port;
  await new Promise<void>((resolve) => httpServer.close(() => resolve()));
  return port;
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
