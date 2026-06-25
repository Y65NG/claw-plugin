import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket, { WebSocketServer } from "ws";

const skillInstallerMock = vi.hoisted(() => ({
  ensureHubSkillInstalled: vi.fn()
}));

vi.mock("../src/skill-installer", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/skill-installer")>();
  return {
    ...actual,
    ensureHubSkillInstalled: skillInstallerMock.ensureHubSkillInstalled
  };
});

import {
  createHub53AIBridge,
  parseIncomingMessage,
  sliceLatestWindowPage,
  type Hub53AIOutgoingFrame
} from "../src/53aihub-client";
import type { GatewayEvent, GatewaySession } from "../src/gateway-client";
import { resolveLocalOutputManifestPath } from "../src/local-output-files";
import type { SessionMessage, SessionStatus } from "../src/models";

const cleanupPaths: string[] = [];
const cleanupServers: Array<() => Promise<void>> = [];

afterEach(async () => {
  skillInstallerMock.ensureHubSkillInstalled.mockReset();
  await Promise.all(cleanupServers.splice(0).map((cleanup) => cleanup()));
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

function ledgerTimelineEvent(
  sessionId: string,
  seq: number,
  turnId: string,
  eventType: "turn.started" | "part.replace" | "turn.completed",
  text: string
): GatewayEvent {
  return {
    id: `event-${seq}`,
    sessionId,
    seq,
    kind: eventType === "part.replace" ? "assistant.message" : eventType === "turn.started" ? "run.started" : "run.completed",
    payload: {
      content: text,
      openclaw_ledger: {
        protocol_version: "openclaw.ledger.v1",
        seq,
        session_id: sessionId,
        conversation_id: sessionId,
        turn_id: `${sessionId}:${turnId}`,
        active_request_id: turnId,
        part_id: `${sessionId}:${turnId}:${eventType}`,
        part_type: eventType === "part.replace" ? "answer" : "status",
        event_type: eventType,
        operation: eventType === "part.replace" ? "replace" : "noop",
        visibility: "final",
        text,
        created_at: "2026-06-18T08:00:00.000Z",
        raw_event_ref: `${sessionId}:${seq}:event-${seq}`
      }
    },
    createdAt: "2026-06-18T08:00:00.000Z"
  };
}

function fakeGatewaySession(id: string): GatewaySession {
  return {
    id,
    title: id,
    status: "idle",
    hostKind: "qclaw",
    runnerCommand: "gateway",
    createdAt: "2026-05-20T10:00:00.000Z",
    updatedAt: "2026-05-20T10:00:00.000Z",
    lastEventSeq: 0
  };
}

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
          metadata: {
            openclaw_client_message_id: "client-message-a"
          },
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
      clientMessageId: "client-message-a",
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

  it("ignores business heartbeat messages before they reach the gateway", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-heartbeat-message-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const onUserMessage = vi.fn();
    const onSessionStatus = vi.fn();

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
        onUserMessage,
        onSessionStatus,
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
          req_id: "req-heartbeat",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "agent:main:session-123:heartbeat",
            messages: [
              {
                role: "user",
                content: "HEARTBEAT_OK"
              }
            ]
          }
        })
      );

      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(gateway.sentMessages).toEqual([]);
      expect(gateway.createdTitles).toEqual([]);
      expect(onUserMessage).not.toHaveBeenCalled();
      expect(onSessionStatus).not.toHaveBeenCalled();
    } finally {
      await bridge.stop();
    }
  });

  it("sends selected skills and uploaded files together to the gateway", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const skillRoot = join(stateDir, "skills");
    await mkdir(join(skillRoot, "openclaw_pdf_probe"), { recursive: true });
    await writeFile(
      join(stateDir, "openclaw.json"),
      JSON.stringify({ skills: { load: { extraDirs: [skillRoot] }, entries: { openclaw_pdf_probe: { enabled: true } } } }),
      "utf8"
    );
    await writeFile(
      join(skillRoot, "openclaw_pdf_probe", "SKILL.md"),
      "When this skill is selected, inspect the attached document before answering.",
      "utf8"
    );
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const fileServer = createServer((req, res) => {
      if (req.url === "/probe.txt") {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("probe file content");
        return;
      }
      res.writeHead(404);
      res.end("not found");
    });
    await new Promise<void>((resolve) => fileServer.listen(0, "127.0.0.1", resolve));
    cleanupServers.push(
      () =>
        new Promise<void>((resolve) => {
          fileServer.close(() => resolve());
        })
    );
    const filePort = (fileServer.address() as any).port;
    const gateway = new FakeGateway();
    const userMessages: SessionMessage[] = [];
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
        onUserMessage: async (message) => {
          userMessages.push(message);
        },
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
          req_id: "req-skill-file",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-skill-file",
            metadata: {
              openclaw_client_message_id: "client-skill-file",
              openclaw_skill: {
                skill_id: "skill-1",
                skill_name: "openclaw_pdf_probe",
                display_name: "PDF Probe"
              },
              openclaw_input_files: [
                {
                  id: "file-1",
                  file_name: "probe.txt",
                  mime_type: "text/plain",
                  signed_download_url: `http://127.0.0.1:${filePort}/probe.txt`
                }
              ]
            },
            messages: [{ role: "user", content: "测试技能效果" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toHaveLength(1);
      });

      const sentMessage = gateway.sentMessages[0];
      const expectedLocalPath = join(stateDir, "input-files", "req-skill-file", "probe.txt");
      expect(sentMessage.content).toContain("测试技能效果");
      expect(sentMessage.content).not.toContain("Files:\n");
      expect(sentMessage.content).not.toContain("Attached files:\n");
      expect(sentMessage.content).not.toContain(`http://127.0.0.1:${filePort}/probe.txt`);
      expect(sentMessage.content).toContain("<53aihub-openclaw-runtime-context>");
      expect(sentMessage.content).toContain("Local input files:");
      expect(sentMessage.content).toContain(`@${expectedLocalPath}`);
      expect(sentMessage.content).toContain("Selected skill: /openclaw_pdf_probe");
      expect(sentMessage.content).not.toContain("53AIHub selected skill instructions for /openclaw_pdf_probe");
      expect(sentMessage.content).not.toContain("inspect the attached document");
      expect(sentMessage.attachments).toEqual([
        expect.objectContaining({
          type: "file",
          fileName: "probe.txt",
          mimeType: "text/plain",
          content: Buffer.from("probe file content").toString("base64")
        })
      ]);
      expect(userMessages).toHaveLength(1);
      expect(userMessages[0]).toMatchObject({
        content: "测试技能效果",
        metadata: {
          openclaw_client_message_id: "client-skill-file",
          openclaw_skill: {
            skill_name: "openclaw_pdf_probe"
          },
          openclaw_input_files: [
            expect.objectContaining({
              file_name: "probe.txt",
              local_path: expectedLocalPath
            })
          ]
        }
      });
      expect(userMessages[0]?.content).not.toContain("<53aihub-openclaw-runtime-context>");
      expect(userMessages[0]?.content).not.toContain("Selected skill:");
      expect(userMessages[0]?.content).not.toContain("Attached files:");
    } finally {
      await bridge.stop();
    }
  });

  it("falls back to runtime prompt context when native attachment send is rejected", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const fileServer = createServer((req, res) => {
      if (req.url === "/probe.txt") {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("probe file content");
        return;
      }
      res.writeHead(404);
      res.end("not found");
    });
    await new Promise<void>((resolve) => fileServer.listen(0, "127.0.0.1", resolve));
    cleanupServers.push(
      () =>
        new Promise<void>((resolve) => {
          fileServer.close(() => resolve());
        })
    );
    const filePort = (fileServer.address() as any).port;
    const gateway = new FakeGateway();
    gateway.failNextAttachmentSendMessage = "attachments unsupported by current gateway";
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
          req_id: "req-attachment-fallback",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-attachment-fallback",
            metadata: {
              openclaw_client_message_id: "client-attachment-fallback",
              openclaw_input_files: [
                {
                  id: "file-1",
                  file_name: "probe.txt",
                  mime_type: "text/plain",
                  signed_download_url: `http://127.0.0.1:${filePort}/probe.txt`
                }
              ]
            },
            messages: [{ role: "user", content: "读取附件" }]
          }
        })
      );

      await waitFor(() => {
        expect(gateway.sentMessages).toHaveLength(1);
      });

      const sentMessage = gateway.sentMessages[0];
      const expectedLocalPath = join(stateDir, "input-files", "req-attachment-fallback", "probe.txt");
      expect(sentMessage.attachments).toBeUndefined();
      expect(sentMessage.content).toContain("读取附件");
      expect(sentMessage.content).toContain("<53aihub-openclaw-runtime-context>");
      expect(sentMessage.content).toContain("Local input files:");
      expect(sentMessage.content).toContain(`@${expectedLocalPath}`);
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
        payload: { ok: true, run_id: "gateway-run-1" },
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
            metadata: {
              openclaw_client_message_id: "client-meta"
            },
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
              },
              openclaw_ledger: {
                protocol_version: "openclaw.ledger.v1",
                active_request_id: "client-meta",
                part_type: "answer",
                event_type: "part.replace",
                operation: "replace",
                visibility: "stream"
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
              },
              openclaw_ledger: {
                protocol_version: "openclaw.ledger.v1",
                active_request_id: "client-meta",
                part_type: "thinking",
                event_type: "part.replace",
                operation: "replace",
                visibility: "final"
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
              },
              openclaw_ledger: {
                protocol_version: "openclaw.ledger.v1",
                active_request_id: "client-meta",
                part_type: "tool",
                event_type: "part.replace",
                operation: "replace",
                visibility: "final"
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
        expect(streaming?.data.payload.openclaw_timeline.turn_id).toContain("client-meta");
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
                final: true,
                turn_id: streaming?.data.payload.openclaw_timeline.turn_id
              },
              openclaw_ledger: {
                protocol_version: "openclaw.ledger.v1",
                active_request_id: "client-meta",
                turn_id: streaming?.data.payload.openclaw_timeline.turn_id,
                run_id: "gateway-run-1",
                part_type: "status",
                event_type: "turn.completed",
                operation: "close",
                terminal_status: "completed"
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
            replace: true,
            eventType: "response.output_text.done"
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

  it("keeps resumed assistant text on the same canonical answer part after tool activity", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-answer-segments-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const createdAt = "2026-06-12T01:29:24.000Z";
    const events: GatewayEvent[] = [
      {
        id: "evt-run-start",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-doc-10", phase: "start" },
        createdAt
      },
      {
        id: "evt-answer-intro",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.message",
        payload: {
          content: "好的！我来为您生成一个包含10个字的纯文本测试文档。",
          runId: "run-doc-10",
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
          runId: "run-doc-10",
          data: {
            name: "write",
            toolCallId: "tool-write-doc",
            args: { path: "test_document.txt" }
          }
        },
        createdAt
      },
      {
        id: "evt-tool-result",
        sessionId: "session-1",
        seq: 4,
        kind: "tool.result",
        payload: {
          runId: "run-doc-10",
          data: {
            name: "write",
            toolCallId: "tool-write-doc",
            result: { output: "ok" }
          }
        },
        createdAt
      },
      {
        id: "evt-answer-final",
        sessionId: "session-1",
        seq: 5,
        kind: "assistant.message",
        payload: {
          content: "完成！我已经为您生成了一个纯文本测试文档。\n\n**文件信息：**",
          runId: "run-doc-10",
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt
      },
      {
        id: "evt-done",
        sessionId: "session-1",
        seq: 6,
        kind: "run.completed",
        payload: { runId: "run-doc-10", phase: "end", status: "completed" },
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

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-answer-segments",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-answer-segments",
            messages: [{ role: "user", content: "生成一个10字测试文档" }]
          }
        })
      );

      await waitFor(() => {
        const answerFrames = server.frames.filter(
          (frame) =>
            frame.req_id === "req-answer-segments" &&
            frame.action === "chat" &&
            frame.data?.event_kind === "assistant.message"
        );
        expect(answerFrames).toHaveLength(2);
        expect(answerFrames.map((frame) => frame.data.payload.openclaw_timeline.segment_id)).toEqual([
          "session-1:turn:req-answer-segments:answer:0",
          "session-1:turn:req-answer-segments:answer:0"
        ]);
        expect(answerFrames.map((frame) => frame.data.payload.openclaw_ledger.part_id)).toEqual([
          "session-1:turn:req-answer-segments:answer:0",
          "session-1:turn:req-answer-segments:answer:0"
        ]);
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-answer-segments-events",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 20, offset: 0 }
        })
      );

      await waitFor(() => {
        const rpcFrame = frameByReq(server.frames, "rpc-answer-segments-events");
        expect(rpcFrame).toMatchObject({ action: "sessions.events", status: "done" });
        const ledgerAnswers = rpcFrame.data.ledger_events.filter((event: any) => event.part_type === "answer");
        expect(ledgerAnswers).toHaveLength(1);
        expect(ledgerAnswers[0]).toMatchObject({
          part_id: "session-1:turn:req-answer-segments:answer:0",
          text: "完成！我已经为您生成了一个纯文本测试文档。\n\n**文件信息：**"
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("replaces polluted completed answers with ordered typed transcript text", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-typed-final-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const polluted = [
      "Let me try again with a shorter timeout.",
      "The command is still running. Let me try a different approach.",
      "让我搜索一下以下是最终结果。"
    ].join("");
    const typedIntro = "让我搜索一下";
    const typedFinal = "以下是最终结果。";
    const expected = `${typedIntro}\n\n${typedFinal}`;
    gateway.eventsToEmit = [
      {
        id: "run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-typed-final" },
        createdAt: "2026-06-12T07:25:00.000Z"
      },
      {
        id: "thinking-leak",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.thinking",
        payload: {
          content: "Let me try again with a shorter timeout.",
          runId: "run-typed-final",
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt: "2026-06-12T07:25:01.000Z"
      },
      {
        id: "session-1:chat:3",
        sessionId: "session-1",
        seq: 3,
        kind: "assistant.delta",
        payload: {
          content: polluted,
          runId: "run-typed-final",
          rawSeq: 227,
          state: "delta",
          mode: "replace",
          replace: true
        },
        createdAt: "2026-06-12T07:25:02.000Z"
      },
      {
        id: "session-1:chat:4",
        sessionId: "session-1",
        seq: 4,
        kind: "assistant.message",
        payload: {
          content: polluted,
          runId: "run-typed-final",
          rawSeq: 228,
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt: "2026-06-12T07:25:03.000Z"
      },
      {
        id: "run-completed",
        sessionId: "session-1",
        seq: 5,
        kind: "run.completed",
        payload: { runId: "run-typed-final" },
        createdAt: "2026-06-12T07:25:04.000Z"
      }
    ];
    gateway.messagesBySession.set("session-1", [
      {
        id: "user-typed-final",
        sessionId: "session-1",
        role: "user",
        content: "搜索并总结",
        createdAt: "2026-06-12T07:24:59.000Z",
        seq: 10
      },
      {
        id: "assistant-typed-final",
        sessionId: "session-1",
        role: "assistant",
        content: expected,
        createdAt: "2026-06-12T07:25:04.000Z",
        seq: 11,
        payload: {
          runId: "run-typed-final",
          openclaw_typed_text_segments: [typedIntro, typedFinal],
          openclaw_typed_text_segment_count: 2
        }
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

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-typed-final",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-typed-final",
            metadata: {
              openclaw_client_message_id: "client-typed-final"
            },
            messages: [{ role: "user", content: "搜索并总结" }]
          }
        })
      );

      await waitFor(() => {
        expect(server.frames.some((frame) => frame.req_id === "req-typed-final" && frame.status === "done")).toBe(true);
      });

      const answerFrames = server.frames.filter(
        (frame) =>
          frame.req_id === "req-typed-final" &&
          frame.action === "chat" &&
          frame.status === "streaming"
      );
      expect(answerFrames.map((frame) => frame.data.choices[0]?.delta.content ?? "")).not.toContain(polluted);
      const typedFrame = answerFrames.find((frame) => frame.data?.payload?.source_kind === "typed_transcript.live_replace");
      expect(typedFrame?.data.choices[0]?.delta.content).toBe(expected);
      expect(typedFrame?.data.mode).toBe("replace");
      expect(typedFrame?.data.replace).toBe(true);
      expect(typedFrame?.data.payload).toMatchObject({
        typed_live: true,
        source_kind: "typed_transcript.live_replace",
        typed_live_match_strategy: "run_id",
        typed_live_text_segment_count: 2,
        openclaw_ledger: {
          part_id: "session-1:turn:client-typed-final:answer:0",
          part_type: "answer",
          event_type: "part.replace",
          operation: "replace",
          text: expected,
          payload: {
            source_kind: "typed_transcript.live_replace",
            typed_live: true
          }
        }
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-typed-final-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-typed-final-snapshot");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const answerEvents = snapshot?.data?.ledger_events.filter((event: any) => event.part_type === "answer");
        expect(answerEvents).toHaveLength(1);
        expect(answerEvents[0]).toMatchObject({
          text: expected,
          payload: {
            source_kind: "typed_transcript.final_replace",
            typed_final_text_segment_count: 2
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("promotes typed final when matching raw answer text was only hidden", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-typed-final-hidden-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const baseMs = Date.now() + 60_000;
    const typedText = "重庆今天天气：\n\n☁️ **多云**，**21°C**。";
    gateway.eventsToEmit = [
      {
        id: "run-started-hidden-same-text",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-hidden-same-text" },
        createdAt: new Date(baseMs).toISOString()
      },
      {
        id: "session-1:message:hidden-same-text",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.message",
        payload: {
          content: typedText,
          runId: "run-hidden-same-text",
          rawSeq: 228,
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt: new Date(baseMs).toISOString()
      },
      {
        id: "run-completed-hidden-same-text",
        sessionId: "session-1",
        seq: 3,
        kind: "run.completed",
        payload: { runId: "run-hidden-same-text" },
        createdAt: new Date(baseMs + 2_000).toISOString()
      }
    ];
    gateway.messagesBySession.set("session-1", [
      {
        id: "assistant-hidden-same-text",
        sessionId: "session-1",
        role: "assistant",
        content: typedText,
        createdAt: new Date(baseMs + 2_000).toISOString(),
        seq: 11,
        payload: {
          openclaw_typed_text_segments: [typedText],
          openclaw_typed_text_segment_count: 1
        }
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

    await bridge.start();
    try {
      const connection = await server.connected;
      connection.socket.send(
        JSON.stringify({
          req_id: "req-typed-final-hidden",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-typed-final-hidden",
            metadata: {
              openclaw_client_message_id: "client-typed-final-hidden"
            },
            messages: [{ role: "user", content: "今天重庆天气如何" }]
          }
        })
      );

      await waitFor(() => {
        expect(server.frames.some((frame) => frame.req_id === "req-typed-final-hidden" && frame.status === "done")).toBe(true);
      });

      const answerFrames = server.frames.filter(
        (frame) =>
          frame.req_id === "req-typed-final-hidden" &&
          frame.action === "chat" &&
          frame.status === "streaming"
      );
      expect(answerFrames.some((frame) => frame.data?.payload?.source_kind === "typed_transcript.live_replace")).toBe(false);
      const typedFinalFrame = answerFrames.find((frame) => frame.data?.payload?.source_kind === "typed_transcript.final_replace");
      expect(typedFinalFrame?.data.choices[0]?.delta.content).toBe(typedText);
      expect(typedFinalFrame?.data.replace).toBe(true);
      expect(typedFinalFrame?.data.payload.openclaw_ledger).toMatchObject({
        part_id: "session-1:turn:client-typed-final-hidden:answer:0",
        part_type: "answer",
        event_type: "part.replace",
        operation: "replace",
        visibility: "final",
        text: typedText,
        payload: {
          source_kind: "typed_transcript.final_replace",
          typed_final: true
        }
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-typed-final-hidden-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-typed-final-hidden-snapshot");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const answerEvents = snapshot?.data?.ledger_events.filter((event: any) => event.part_type === "answer");
        expect(answerEvents).toHaveLength(1);
        expect(answerEvents[0]).toMatchObject({
          visibility: "final",
          text: typedText,
          payload: {
            source_kind: "typed_transcript.final_replace",
            typed_final: true
          }
        });
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

  it("enriches live exec tool calls with command arguments from typed history events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-live-exec-call",
        sessionId: "session-1",
        seq: 10,
        kind: "tool.call",
        payload: {
          data: {
            phase: "call",
            name: "Exec",
            toolCallId: "call-exec-1"
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-live-exec-result",
        sessionId: "session-1",
        seq: 11,
        kind: "tool.result",
        payload: {
          data: {
            phase: "result",
            name: "Exec",
            toolCallId: "call-exec-1",
            result: {
              status: "completed",
              exitCode: 0,
              aggregated: "New York: cloudy"
            }
          }
        },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-done",
        sessionId: "session-1",
        seq: 12,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      }
    ];
    gateway.eventsBySession.set("session-1", [
      {
        id: "history-exec-call",
        sessionId: "session-1",
        seq: 101,
        kind: "tool.call",
        payload: {
          data: {
            phase: "call",
            name: "exec",
            toolCallId: "call-exec-1",
            args: {
              command: 'curl -s --max-time 10 "wttr.in/NewYork?1"',
              timeout: 15
            }
          }
        },
        createdAt: new Date().toISOString()
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
          req_id: "req-exec-enrich",
          action: "chat",
          data: {
            user: "user-123",
            conversation_id: "chat-exec-enrich",
            messages: [{ role: "user", content: "今天纽约天气怎么样？" }]
          }
        })
      );

      await waitFor(() => {
        const toolCall = server.frames.find(
          (frame) =>
            frame.req_id === "req-exec-enrich" &&
            frame.action === "chat" &&
            frame.status === "thinking" &&
            frame.data?.event_kind === "tool.call"
        );
        expect(toolCall?.data.payload.data).toMatchObject({
          name: "exec",
          command: 'curl -s --max-time 10 "wttr.in/NewYork?1"',
          args: {
            command: 'curl -s --max-time 10 "wttr.in/NewYork?1"',
            timeout: 15
          }
        });
        expect(toolCall?.data.payload.openclaw_ledger.payload.data.args.command).toBe(
          'curl -s --max-time 10 "wttr.in/NewYork?1"'
        );
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
                artifact_id: "artifact-report",
                upload_file_id: "upload-report",
                file_name: "output/report.md",
                url: "/api/preview/report-preview.md",
                preview_key: "report-preview.md",
                preview_url: "/api/preview/report-preview.md",
                download_url: "/api/openclaw/agents/bot-123/artifacts/artifact-report/download",
                signed_download_url: "https://files.example.com/report.md?sig=1",
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
                artifact_id: "artifact-report",
                upload_file_id: "upload-report",
                file_name: "output/report.md",
                url: "/api/preview/report-preview.md",
                preview_key: "report-preview.md",
                preview_url: "/api/preview/report-preview.md",
                download_url: "/api/openclaw/agents/bot-123/artifacts/artifact-report/download",
                signed_download_url: "https://files.example.com/report.md?sig=1",
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
                    artifact_id: "artifact-report",
                    upload_file_id: "upload-report",
                    file_name: "output/report.md",
                    url: "/api/preview/report-preview.md",
                    preview_key: "report-preview.md",
                    preview_url: "/api/preview/report-preview.md",
                    download_url: "/api/openclaw/agents/bot-123/artifacts/artifact-report/download",
                    signed_download_url: "https://files.example.com/report.md?sig=1",
                    mime_type: "text/markdown",
                    size: 128
                  }
                ],
                media_attachments: [
                  {
                    id: "file-report",
                    artifact_id: "artifact-report",
                    upload_file_id: "upload-report",
                    file_name: "output/report.md",
                    url: "/api/preview/report-preview.md",
                    preview_key: "report-preview.md",
                    preview_url: "/api/preview/report-preview.md",
                    download_url: "/api/openclaw/agents/bot-123/artifacts/artifact-report/download",
                    signed_download_url: "https://files.example.com/report.md?sig=1",
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

  it("keeps uploaded artifact preview and download fields separate in output_files and canonical events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-uploaded-artifact-"));
    cleanupPaths.push(stateDir);
    const artifactResponse = {
      artifact_id: "artifact-uploaded",
      upload_file_id: "upload-uploaded",
      file_name: "uploaded.txt",
      mime_type: "text/plain",
      size: 4,
      sha256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
      preview_key: "uploaded-preview.txt",
      preview_url: "/api/preview/uploaded-preview.txt",
      url: "/api/preview/uploaded-preview.txt",
      download_url: "/api/openclaw/agents/bot-123/artifacts/artifact-uploaded/download",
      signed_download_url: "https://files.example.com/uploaded.txt?sig=1",
      source_kind: "openclaw_artifact"
    };
    const server = await createFakeHubServer(undefined, { artifactUploadResponse: artifactResponse });
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-file",
        sessionId: "session-1",
        seq: 1,
        kind: "tool.result",
        payload: {
          data: {
            name: "write_file",
            output_files: [
              {
                id: "local-uploaded",
                file_name: "uploaded.txt",
                mime_type: "text/plain",
                size: 4,
                base64: Buffer.from("abcd").toString("base64")
              }
            ]
          }
        },
        createdAt: "2026-06-23T08:00:00.000Z"
      },
      {
        id: "evt-completed",
        sessionId: "session-1",
        seq: 2,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: "2026-06-23T08:00:01.000Z"
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
          req_id: "req-uploaded-artifact",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-uploaded-artifact",
            messages: [{ role: "user", content: "生成 4 字文件" }]
          }
        })
      );

      await waitFor(() => {
        const outputStep = server.frames.find(
          (frame) =>
            frame.req_id === "req-uploaded-artifact" &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        );
        expect(outputStep).toBeTruthy();
        const file = outputStep?.data?.process_step?.data?.files?.[0];
        expect(file).toMatchObject({
          id: "artifact-uploaded",
          artifact_id: "artifact-uploaded",
          upload_file_id: "upload-uploaded",
          preview_key: "uploaded-preview.txt",
          url: "/api/preview/uploaded-preview.txt",
          preview_url: "/api/preview/uploaded-preview.txt",
          download_url: "/api/openclaw/agents/bot-123/artifacts/artifact-uploaded/download",
          signed_download_url: "https://files.example.com/uploaded.txt?sig=1"
        });
        expect(JSON.stringify(file)).not.toContain("localhost");
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-uploaded-artifact-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-uploaded-artifact-snapshot");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const outputEvent = snapshot?.data?.ledger_events?.find((event: any) => event.part_type === "output_file");
        expect(JSON.stringify(outputEvent)).toContain("/api/preview/uploaded-preview.txt");
        expect(JSON.stringify(outputEvent)).toContain("/api/openclaw/agents/bot-123/artifacts/artifact-uploaded/download");
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
        expect(new Set(segmentIds).size).toBe(1);
        expect(segmentIds[0]).toContain("output_files:name:report.txt");
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

  it("emits manifest output_files and skips legacy workspace scan when the manifest matches", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    await mkdir(workspaceDir, { recursive: true });

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const reqId = "req-manifest-local-files";
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
    gateway.beforeEmit = async (sessionId) => {
      const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId: sessionId })!;
      await mkdir(dirname(manifestPath), { recursive: true });
      const manifestFilePath = join(workspaceDir, "manifest-only.md");
      const strayFilePath = join(workspaceDir, "stray.md");
      const manifestContent = "# manifest\n";
      await writeFile(manifestFilePath, manifestContent);
      await writeFile(strayFilePath, "# stray\n");
      await writeFile(
        manifestPath,
        JSON.stringify(
          buildOutputManifestRecord({
            conversationId: sessionId,
            turnId: `${sessionId}:turn:${reqId}`,
            activeRequestId: reqId,
            path: manifestFilePath,
            logicalPath: "manifest-only.md",
            mimeType: "text/markdown",
            content: manifestContent
          })
        )
      );
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
          req_id: reqId,
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-manifest-local-files",
            messages: [{ role: "user", content: "创建 manifest 文件" }]
          }
        })
      );

      await waitFor(() => {
        const outputStep = server.frames.find(
          (frame) =>
            frame.req_id === reqId &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        );
        expect(outputStep).toBeTruthy();
        const files = outputStep!.data.process_step.data.files;
        expect(files).toHaveLength(1);
        expect(files[0]).toMatchObject({
          file_name: "manifest-only.md",
          mime_type: "text/markdown",
          size: 11,
          base64: Buffer.from("# manifest\n").toString("base64")
        });
      });
      expect(gateway.sentMessages[0]?.content).toContain("Output artifact manifest:");
      expect(gateway.sentMessages[0]?.content).toContain(`active_request_id: "${reqId}"`);
      expect(gateway.sentMessages[0]?.content).toContain("Allowed output workspace roots:");

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-manifest-output-seq",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-manifest-output-seq");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const outputLedger = (snapshot?.data?.ledger_events || []).find(
          (event: any) => event.part_type === "output_file"
        );
        expect(outputLedger).toBeTruthy();
        expect(outputLedger.payload?.event_seq).toBeGreaterThan(2);
        expect(outputLedger.raw_event_ref).toContain(`session-1:${outputLedger.payload.event_seq}:`);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("backfills manifest output_files into canonical snapshot when live emission missed them", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    await mkdir(workspaceDir, { recursive: true });

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    const reqId = "req-manifest-backfill";
    gateway.eventsToEmit = [
      {
        id: "evt-started-backfill",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      },
      {
        id: "evt-completed-backfill",
        sessionId: "session-1",
        seq: 2,
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
        detectCreatedFiles: false,
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
          req_id: reqId,
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-manifest-backfill",
            messages: [{ role: "user", content: "创建 manifest backfill 文件" }]
          }
        })
      );

      await waitFor(() => {
        const done = server.frames.find(
          (frame) => frame.req_id === reqId && frame.action === "chat" && frame.status === "done"
        );
        expect(done).toBeTruthy();
      });

      expect(
        server.frames.some(
          (frame) =>
            frame.req_id === reqId &&
            frame.action === "chat" &&
            frame.data?.object === "process.step" &&
            frame.data.process_step?.step_code === "output_files"
        )
      ).toBe(false);

      const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId: "session-1" })!;
      await mkdir(dirname(manifestPath), { recursive: true });
      const manifestFilePath = join(workspaceDir, "backfilled.md");
      const manifestContent = "# backfilled\n";
      await writeFile(manifestFilePath, manifestContent);
      await writeFile(
        manifestPath,
        JSON.stringify(
          buildOutputManifestRecord({
            conversationId: "session-1",
            turnId: `session-1:turn:${reqId}`,
            activeRequestId: reqId,
            path: manifestFilePath,
            logicalPath: "backfilled.md",
            mimeType: "text/markdown",
            content: manifestContent
          })
        )
      );

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-manifest-output-backfill",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-manifest-output-backfill");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const outputLedger = (snapshot?.data?.ledger_events || []).find(
          (event: any) => event.part_type === "output_file"
        );
        expect(outputLedger).toBeTruthy();
        expect(outputLedger.turn_id).toBe(`session-1:turn:${reqId}`);
        expect(outputLedger.active_request_id).toBe(reqId);
        expect(outputLedger.payload?.process_step?.data?.files?.[0]).toMatchObject({
          file_name: "backfilled.md",
          sha256: createHash("sha256").update(manifestContent).digest("hex")
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("uses manifest active_request_id to canonicalize matching history answer and output files together", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-history-manifest-scope-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    await mkdir(workspaceDir, { recursive: true });

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const sessionId = "session-1";
    const reqId = "1782203678209";
    const turnId = `${sessionId}:turn:${reqId}`;
    const startedAtMs = Number(reqId) + 900;
    const endedAtMs = startedAtMs + 3_000;
    const typedText = "我已经成功创建了一个正好4个字符长度的文件。";
    const rawPollutedText = `临时过程。${typedText}`;
    gateway.eventsBySession.set(sessionId, [
      {
        id: "history-run-started",
        sessionId,
        seq: 10,
        kind: "run.started",
        payload: {
          runId: "run-history-file",
          startedAt: startedAtMs
        },
        createdAt: new Date(startedAtMs).toISOString()
      },
      {
        id: "history-answer-polluted",
        sessionId,
        seq: 11,
        kind: "assistant.message",
        payload: {
          content: rawPollutedText,
          runId: "run-history-file",
          rawSeq: 20,
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt: new Date(startedAtMs + 1_000).toISOString()
      },
      {
        id: "history-run-completed",
        sessionId,
        seq: 12,
        kind: "run.completed",
        payload: {
          runId: "run-history-file",
          startedAt: startedAtMs,
          endedAt: endedAtMs
        },
        createdAt: new Date(endedAtMs).toISOString()
      }
    ]);
    gateway.messagesBySession.set(sessionId, [
      {
        id: "assistant-history-file-final",
        sessionId,
        role: "assistant",
        content: typedText,
        createdAt: new Date(endedAtMs).toISOString(),
        seq: 30,
        payload: {
          runId: "run-history-file",
          openclaw_typed_text_segments: [typedText],
          openclaw_typed_text_segment_count: 1
        }
      }
    ]);

    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId: sessionId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    const manifestFilePath = join(workspaceDir, "4chars.txt");
    const manifestContent = "abcd";
    await writeFile(manifestFilePath, manifestContent);
    await writeFile(
      manifestPath,
      JSON.stringify(
        buildOutputManifestRecord({
          conversationId: sessionId,
          turnId,
          activeRequestId: reqId,
          path: manifestFilePath,
          logicalPath: "4chars.txt",
          mimeType: "text/plain",
          content: manifestContent
        })
      )
    );

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
        detectCreatedFiles: false,
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
          req_id: "rpc-history-manifest-scope",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: sessionId }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-history-manifest-scope");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const events = snapshot?.data?.ledger_events || [];
        const answerEvents = events.filter((event: any) => event.part_type === "answer");
        const outputEvents = events.filter((event: any) => event.part_type === "output_file");
        expect(answerEvents).toHaveLength(1);
        expect(outputEvents).toHaveLength(1);
        expect(answerEvents[0]).toMatchObject({
          turn_id: turnId,
          active_request_id: reqId,
          run_id: "run-history-file",
          text: typedText,
          payload: expect.objectContaining({
            source_kind: "typed_transcript.final_replace",
            typed_final: true
          })
        });
        expect(outputEvents[0]).toMatchObject({
          turn_id: turnId,
          active_request_id: reqId,
          payload: expect.objectContaining({
            process_step: expect.objectContaining({
              data: expect.objectContaining({
                files: expect.arrayContaining([
                  expect.objectContaining({
                    file_name: "4chars.txt",
                    sha256: createHash("sha256").update(manifestContent).digest("hex")
                  })
                ])
              })
            })
          })
        });
        expect(JSON.stringify(events)).not.toContain("active_request_id\":\"history:run-history-file");
      });

      gateway.messagesBySession.set(sessionId, [
        {
          id: "user-history-file",
          sessionId,
          role: "user",
          content: "创建一个长度为4个字符的文件",
          createdAt: new Date(startedAtMs - 1_000).toISOString(),
          seq: 29
        },
        {
          id: "assistant-history-file-final",
          sessionId,
          role: "assistant",
          content: typedText,
          createdAt: new Date(endedAtMs).toISOString(),
          seq: 30,
          payload: {
            runId: "run-history-file",
            openclaw_typed_text_segments: [typedText],
            openclaw_typed_text_segment_count: 1
          }
        }
      ]);
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-history-manifest-messages",
          action: "sessions.messages",
          status: "request",
          data: { session_id: sessionId, limit: 20, offset: 0 }
        })
      );

      await waitFor(() => {
        const messagesFrame = frameByReq(server.frames, "rpc-history-manifest-messages");
        expect(messagesFrame).toMatchObject({ action: "sessions.messages", status: "done" });
        const events = messagesFrame?.data?.ledger_events || messagesFrame?.data?.ledgerEvents || [];
        const outputEvents = events.filter((event: any) => event.part_type === "output_file");
        expect(outputEvents).toHaveLength(1);
        expect(outputEvents[0]).toMatchObject({
          turn_id: turnId,
          active_request_id: reqId,
          payload: expect.objectContaining({
            process_step: expect.objectContaining({
              data: expect.objectContaining({
                files: expect.arrayContaining([expect.objectContaining({ file_name: "4chars.txt" })])
              })
            })
          })
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("keeps manifest output files in message pages without using weak typed final history matches", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-history-manifest-page-"));
    cleanupPaths.push(stateDir);
    const workspaceDir = join(stateDir, "workspace");
    await mkdir(workspaceDir, { recursive: true });

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const sessionId = "session-1";
    const reqId = "1782207557133";
    const turnId = `${sessionId}:turn:${reqId}`;
    const startedAtMs = Number(reqId) + 700;
    const endedAtMs = startedAtMs + 2_500;
    const rawText = "我已经成功创建了另一个正好4个字符长度的文件。文件名：file4.txt，内容：wxyz。";
    const unrelatedTypedText = "我已经成功创建了另一个正好4个字符长度的文件。文件名：test4.txt，内容：qrst。";
    gateway.eventsBySession.set(sessionId, [
      {
        id: "history-page-run-started",
        sessionId,
        seq: 100,
        kind: "run.started",
        payload: {
          runId: "run-history-page-file",
          startedAt: startedAtMs
        },
        createdAt: new Date(startedAtMs).toISOString()
      },
      {
        id: "history-page-answer",
        sessionId,
        seq: 101,
        kind: "assistant.message",
        payload: {
          content: rawText,
          runId: "run-history-page-file",
          rawSeq: 20,
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt: new Date(startedAtMs + 1_000).toISOString()
      },
      {
        id: "history-page-run-completed",
        sessionId,
        seq: 102,
        kind: "run.completed",
        payload: {
          runId: "run-history-page-file",
          startedAt: startedAtMs,
          endedAt: endedAtMs
        },
        createdAt: new Date(endedAtMs).toISOString()
      }
    ]);
    gateway.messagesBySession.set(sessionId, [
      {
        id: "user-history-page-file",
        sessionId,
        role: "user",
        content: "创建一个长度为4个字符的文件",
        createdAt: new Date(startedAtMs - 500).toISOString(),
        seq: 19
      },
      {
        id: "assistant-history-page-file",
        sessionId,
        role: "assistant",
        content: rawText,
        createdAt: new Date(startedAtMs + 1_000).toISOString(),
        seq: 20,
        payload: {
          runId: "run-history-page-file"
        }
      },
      {
        id: "assistant-unrelated-latest-typed-final",
        sessionId,
        role: "assistant",
        content: unrelatedTypedText,
        createdAt: new Date(endedAtMs + 60_000).toISOString(),
        seq: 30,
        payload: {
          openclaw_typed_text_segments: [unrelatedTypedText],
          openclaw_typed_text_segment_count: 1
        }
      }
    ]);

    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId: sessionId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    const manifestFilePath = join(workspaceDir, "file4.txt");
    const manifestContent = "wxyz";
    await writeFile(manifestFilePath, manifestContent);
    await writeFile(
      manifestPath,
      JSON.stringify(
        buildOutputManifestRecord({
          conversationId: sessionId,
          turnId,
          activeRequestId: reqId,
          path: manifestFilePath,
          logicalPath: "file4.txt",
          mimeType: "text/plain",
          content: manifestContent
        })
      )
    );

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
        detectCreatedFiles: false,
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
          req_id: "rpc-history-page-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: sessionId }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-history-page-snapshot");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const events = snapshot?.data?.ledger_events || [];
        const answerEvents = events.filter((event: any) => event.part_type === "answer");
        const outputEvents = events.filter((event: any) => event.part_type === "output_file");
        expect(answerEvents).toHaveLength(1);
        expect(outputEvents).toHaveLength(1);
        expect(answerEvents[0]).toMatchObject({
          turn_id: turnId,
          active_request_id: reqId,
          text: rawText
        });
        expect(JSON.stringify(answerEvents)).not.toContain(unrelatedTypedText);
        expect(JSON.stringify(answerEvents)).not.toContain("typed_transcript.final_replace");
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-history-page-messages",
          action: "sessions.messages",
          status: "request",
          data: { session_id: sessionId, limit: 20, offset: 0 }
        })
      );

      await waitFor(() => {
        const messagesFrame = frameByReq(server.frames, "rpc-history-page-messages");
        expect(messagesFrame).toMatchObject({ action: "sessions.messages", status: "done" });
        const events = messagesFrame?.data?.ledger_events || messagesFrame?.data?.ledgerEvents || [];
        const answerEvents = events.filter((event: any) => event.part_type === "answer");
        const outputEvents = events.filter((event: any) => event.part_type === "output_file");
        expect(answerEvents).toHaveLength(1);
        expect(outputEvents).toHaveLength(1);
        expect(answerEvents[0]).toMatchObject({
          turn_id: turnId,
          active_request_id: reqId,
          text: rawText
        });
        expect(outputEvents[0]).toMatchObject({
          turn_id: turnId,
          active_request_id: reqId,
          payload: expect.objectContaining({
            process_step: expect.objectContaining({
              data: expect.objectContaining({
                files: expect.arrayContaining([
                  expect.objectContaining({
                    file_name: "file4.txt",
                    sha256: createHash("sha256").update(manifestContent).digest("hex")
                  })
                ])
              })
            })
          })
        });
        expect(JSON.stringify(events)).not.toContain(unrelatedTypedText);
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
        expect(visibleSentMessages(gateway)).toEqual([{ sessionId: "session-1", content: "Say hello" }]);
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

  it("classifies QClaw login failures from failed status updates into ledger terminal events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-qclaw-login-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-run-started-qclaw-login",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-qclaw-login" },
        createdAt: "2026-06-11T12:00:00.000Z"
      },
      {
        id: "evt-status-qclaw-login-failed",
        sessionId: "session-1",
        seq: 2,
        kind: "status.update",
        payload: {
          runId: "run-qclaw-login",
          phase: "error",
          status: "failed",
          session: {
            status: "failed",
            modelProvider: "qclaw",
            model: "pool-hy3-preview",
            runtimeMs: 393
          }
        },
        createdAt: "2026-06-11T12:00:00.393Z"
      }
    ];
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
      connection.socket.send(
        JSON.stringify({
          req_id: "req-qclaw-login",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            messages: [{ role: "user", content: "Trigger QClaw login failure" }]
          }
        })
      );

      await waitFor(() => {
        const errorFrame = server.frames.find((frame) => frame.req_id === "req-qclaw-login" && frame.status === "error");
        expect(errorFrame).toMatchObject({
          action: "chat",
          status: "error",
          data: {
            event_kind: "run.failed",
            error: {
              code: "QCLAW_LOGIN_REQUIRED"
            },
            payload: {
              failure_code: "QCLAW_LOGIN_REQUIRED",
              failure_reason: "qclaw_login_or_provider_auth_failed",
              user_message: expect.stringContaining("QClaw/OpenClaw 智能体登录失败"),
              terminal_status: "failed",
              derived_terminal: true,
              derived_from_kind: "status.update",
              openclaw_ledger: {
                protocol_version: "openclaw.ledger.v1",
                seq: 2,
                run_id: "run-qclaw-login",
                event_type: "turn.failed",
                terminal_status: "failed",
                payload: {
                  failure_code: "QCLAW_LOGIN_REQUIRED"
                }
              }
            }
          }
        });
        expect(errorFrame?.data.choices[0]?.delta.content).toContain("QClaw/OpenClaw 智能体登录失败");
      });
      expect(statuses.at(-1)).toEqual({ sessionId: "session-1", status: "failed" });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-qclaw-login-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1", after_seq: 1 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-qclaw-login-snapshot")).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            session_id: "session-1",
            conversation_id: "session-1",
            last_seq: 2,
            active_turns: [],
            ledger_events: [
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 2,
                run_id: "run-qclaw-login",
                part_type: "status",
                event_type: "turn.failed",
                operation: "close",
                terminal_status: "failed",
                payload: {
                  failure_code: "QCLAW_LOGIN_REQUIRED",
                  derived_from_kind: "status.update"
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

  it("classifies raw QClaw login text even when failed status updates have no run identity", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-qclaw-login-raw-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-status-qclaw-login-raw-failed",
        sessionId: "session-1",
        seq: 1,
        kind: "status.update",
        payload: {
          phase: "error",
          status: "failed",
          error: "QClaw login required: please sign in to continue",
          session: {
            status: "failed",
            runtimeMs: 318
          }
        },
        createdAt: new Date(Date.now() + 10_000).toISOString()
      }
    ];
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
      connection.socket.send(
        JSON.stringify({
          req_id: "req-qclaw-login-raw",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            messages: [{ role: "user", content: "Trigger raw QClaw login failure" }]
          }
        })
      );

      await waitFor(() => {
        const errorFrame = server.frames.find((frame) => frame.req_id === "req-qclaw-login-raw" && frame.status === "error");
        expect(errorFrame).toMatchObject({
          action: "chat",
          status: "error",
          data: {
            event_kind: "run.failed",
            error: {
              code: "QCLAW_LOGIN_REQUIRED"
            },
            payload: {
              failure_code: "QCLAW_LOGIN_REQUIRED",
              failure_reason: "qclaw_auth_failed",
              user_message: expect.stringContaining("QClaw/OpenClaw 智能体登录失败"),
              raw_error_message: "QClaw login required: please sign in to continue",
              terminal_status: "failed",
              derived_terminal: true,
              derived_from_kind: "status.update",
              openclaw_ledger: {
                protocol_version: "openclaw.ledger.v1",
                seq: 1,
                active_request_id: "req-qclaw-login-raw",
                event_type: "turn.failed",
                terminal_status: "failed",
                payload: {
                  failure_code: "QCLAW_LOGIN_REQUIRED",
                  raw_error_message: "QClaw login required: please sign in to continue"
                }
              }
            }
          }
        });
        expect(errorFrame?.data.choices[0]?.delta.content).toContain("QClaw/OpenClaw 智能体登录失败");
      });
      expect(statuses.at(-1)).toEqual({ sessionId: "session-1", status: "failed" });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-qclaw-login-raw-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1", after_seq: 0 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-qclaw-login-raw-snapshot")).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            session_id: "session-1",
            last_seq: 1,
            active_turns: [],
            ledger_events: [
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 1,
                part_type: "status",
                event_type: "turn.failed",
                operation: "close",
                terminal_status: "failed",
                payload: {
                  failure_code: "QCLAW_LOGIN_REQUIRED",
                  derived_from_kind: "status.update"
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

  it("terminates a stale Hub websocket when heartbeats are not acknowledged and reconnects", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-heartbeat-"));
    cleanupPaths.push(stateDir);
    const httpServer = createServer();
    const wsServer = new WebSocketServer({ server: httpServer });
    let connectionCount = 0;
    wsServer.on("connection", (socket) => {
      connectionCount += 1;
      socket.on("message", () => {
        // Intentionally do not answer app-level ping frames; the bridge should
        // notice the unacknowledged heartbeat and create a fresh connection.
      });
    });
    await new Promise<void>((resolve, reject) => {
      httpServer.once("error", reject);
      httpServer.listen(0, "127.0.0.1", () => {
        httpServer.off("error", reject);
        resolve();
      });
    });
    cleanupServers.push(async () => {
      for (const client of wsServer.clients) {
        client.close();
      }
      await new Promise<void>((resolve) => wsServer.close(() => resolve()));
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    });
    const address = httpServer.address();
    if (!address || typeof address === "string") {
      throw new Error("failed to bind heartbeat test hub server");
    }

    const gateway = new FakeGateway();
    const statusErrors: string[] = [];
    const bridge = createHub53AIBridge({
      stateDir,
      config: {
        enabled: true,
        botId: "bot-123",
        secret: "sk-secret",
        wsUrl: `ws://127.0.0.1:${address.port}`,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: false,
        reconnectBaseMs: 20,
        maxReconnectAttempts: 2,
        heartbeatIntervalMs: 1_000,
        heartbeatTimeoutMs: 2_000,
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
        },
      },
    });

    await bridge.start();
    try {
      await waitFor(() => {
        expect(connectionCount).toBeGreaterThanOrEqual(2);
      }, 5_000);
      expect(statusErrors.some((lastError) => lastError.includes("heartbeat timed out"))).toBe(true);
    } finally {
      await bridge.stop();
    }
  }, 8_000);

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
        expect(visibleSentMessages(gateway)).toEqual([{ sessionId: "session-1", content: "message after stop" }]);
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
        expect(visibleSentMessages(gateway)).toEqual([{ sessionId: "session-1", content: "first message" }]);
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
        expect(frameByReq(server.frames, "req-stopped-run")).toMatchObject({
          action: "chat",
          status: "done",
          data: {
            event_kind: "run.interrupted",
            payload: {
              synthetic_terminal: true,
              openclaw_ledger: {
                protocol_version: "openclaw.ledger.v1",
                event_type: "turn.interrupted",
                terminal_status: "interrupted",
                active_request_id: "req-stopped-run"
              }
            }
          }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-stop-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-stop-snapshot")).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            session_id: "session-1",
            active_turns: [],
            recent_events: [
              {
                protocol_version: "openclaw.ledger.v1",
                event_type: "turn.interrupted",
                terminal_status: "interrupted",
                active_request_id: "req-stopped-run",
                payload: {
                  synthetic_terminal: true,
                  synthetic_reason: "control.stop"
                }
              }
            ]
          }
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
        expect(visibleSentMessages(gateway)).toEqual([
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
        expect(visibleSentMessages(gateway)).toEqual([{ sessionId: existingSessionId, content: "test" }]);
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
        expect(visibleSentMessages(gateway)).toEqual([
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
            messages: [
              { id: "message-1", content: "hello" },
              { id: "message-2", content: "hi" }
            ],
            pagination: { limit: 1, offset: 1, total: 2, hasMore: false }
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

  it("adds successfully ensured 53AI skills to runtime skills responses", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-runtime-skill-overlay-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.runtimeInfo = { enabledSkills: ["browser"] };
    skillInstallerMock.ensureHubSkillInstalled.mockResolvedValueOnce({
      ok: true,
      status: "installed",
      skill_id: "skill-1",
      skill_name: "openclaw_pdf_probe",
      display_name: "PDF Probe",
      install_path: "/Users/y65ng/.qclaw/skills/openclaw_pdf_probe"
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
          req_id: "rpc-ensure-skill",
          action: "runtime.skills.ensure",
          status: "request",
          data: {
            skill_id: "skill-1",
            skill_name: "openclaw_pdf_probe",
            display_name: "PDF Probe"
          }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-ensure-skill")).toMatchObject({
          action: "runtime.skills.ensure",
          status: "done",
          data: { ok: true, status: "installed" }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-runtime-skills-after-ensure",
          action: "runtime.get",
          status: "request",
          data: { include: "skills" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-runtime-skills-after-ensure")).toMatchObject({
          action: "runtime.get",
          status: "done",
          data: {
            enabledSkills: [
              "browser",
              expect.objectContaining({
                skill_id: "skill-1",
                skill_name: "openclaw_pdf_probe",
                display_name: "PDF Probe",
                status: "enabled",
                source: "53aihub"
              })
            ]
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not add failed or duplicate ensured 53AI skills to runtime skills responses", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-runtime-skill-overlay-dedupe-"));
    cleanupPaths.push(stateDir);
    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.runtimeInfo = { enabledSkills: ["browser", "openclaw_pdf_probe"] };
    skillInstallerMock.ensureHubSkillInstalled
      .mockResolvedValueOnce({
        ok: false,
        status: "failed",
        skill_name: "failed_skill",
        error: "missing package"
      })
      .mockResolvedValueOnce({
        ok: true,
        status: "up_to_date",
        skill_name: "openclaw_pdf_probe",
        display_name: "PDF Probe"
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
          req_id: "rpc-ensure-failed-skill",
          action: "runtime.skills.ensure",
          status: "request",
          data: { skill_name: "failed_skill" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-ensure-duplicate-skill",
          action: "runtime.skills.ensure",
          status: "request",
          data: { skill_name: "openclaw_pdf_probe", display_name: "PDF Probe" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-ensure-failed-skill")).toMatchObject({
          status: "done",
          data: { ok: false, status: "failed" }
        });
        expect(frameByReq(server.frames, "rpc-ensure-duplicate-skill")).toMatchObject({
          status: "done",
          data: { ok: true, status: "up_to_date" }
        });
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-runtime-skills-after-failed-and-duplicate",
          action: "runtime.get",
          status: "request",
          data: { include: "skills" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-runtime-skills-after-failed-and-duplicate");
        expect(frame).toMatchObject({
          action: "runtime.get",
          status: "done",
          data: { enabledSkills: ["browser", "openclaw_pdf_probe"] }
        });
        expect(JSON.stringify(frame?.data)).not.toContain("failed_skill");
      });
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
        expect(visibleSentMessages(gateway)).toEqual([
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
        expect(visibleSentMessages(gateway)).toEqual([
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
        expect(visibleSentMessages(gateway)).toEqual([
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
        expect(visibleSentMessages(gateway)).toEqual([
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
        expect(visibleSentMessages(gateway)).toEqual([
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

  it("does not expose raw-only stored tool timeline events through sessions.events", async () => {
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
            events: [],
            ledger_events: [],
            pagination: { limit: 10, offset: 0, total: 0, hasMore: false }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("returns canonical OpenClaw ledger snapshot for resumable session recovery", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-snapshot-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsToEmit = [
      {
        id: "evt-run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-ledger" },
        createdAt: "2026-06-11T09:00:00.000Z"
      },
      {
        id: "evt-answer-delta",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.delta",
        payload: { content: "Hel", runId: "run-ledger" },
        createdAt: "2026-06-11T09:00:01.000Z"
      },
      {
        id: "evt-run-completed",
        sessionId: "session-1",
        seq: 3,
        kind: "run.completed",
        payload: { runId: "run-ledger" },
        createdAt: "2026-06-11T09:00:02.000Z"
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
          req_id: "req-ledger-snapshot",
          action: "chat",
          data: {
            user: "user-a",
            conversation_id: "chat-a",
            metadata: {
              openclaw_client_message_id: "client-ledger-snapshot"
            },
            messages: [{ role: "user", content: "Start a running ledger turn" }]
          }
        })
      );

      await waitFor(() => {
        expect(server.frames.some((frame) => frame.req_id === "req-ledger-snapshot" && frame.status === "done")).toBe(true);
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1", after_seq: 1 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-snapshot")).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            session_id: "session-1",
            conversation_id: "session-1",
            last_seq: 3,
            active_turns: [],
            recent_events: [
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 2,
                run_id: "run-ledger",
                active_request_id: "client-ledger-snapshot",
                part_type: "answer",
                event_type: "part.delta",
                operation: "append",
                text: "Hel"
              },
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 3,
                run_id: "run-ledger",
                active_request_id: "client-ledger-snapshot",
                part_type: "status",
                event_type: "turn.completed",
                operation: "close",
                terminal_status: "completed"
              }
            ],
            ledger_events: [
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 2,
                run_id: "run-ledger",
                active_request_id: "client-ledger-snapshot",
                part_type: "answer",
                event_type: "part.delta",
                operation: "append",
                text: "Hel"
              },
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 3,
                run_id: "run-ledger",
                active_request_id: "client-ledger-snapshot",
                part_type: "status",
                event_type: "turn.completed",
                operation: "close",
                terminal_status: "completed"
              }
            ]
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not expose stale historical running turns after later terminal events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-stale-active-turn-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "evt-stale-run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-stale" },
        createdAt: "2026-06-11T09:00:00.000Z"
      },
      {
        id: "evt-stale-answer-delta",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.delta",
        payload: { content: "still old", runId: "run-stale" },
        createdAt: "2026-06-11T09:00:01.000Z"
      },
      {
        id: "evt-current-run-started",
        sessionId: "session-1",
        seq: 3,
        kind: "run.started",
        payload: { runId: "run-current" },
        createdAt: "2026-06-11T09:00:02.000Z"
      },
      {
        id: "evt-current-run-completed",
        sessionId: "session-1",
        seq: 4,
        kind: "run.completed",
        payload: { runId: "run-current" },
        createdAt: "2026-06-11T09:00:03.000Z"
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
          req_id: "rpc-stale-active-turn-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-stale-active-turn-snapshot")).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            session_id: "session-1",
            last_seq: 2,
            active_turns: [],
            recent_events: [
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 1,
                run_id: "run-current",
                part_type: "status",
                event_type: "turn.started",
                terminal_status: "running"
              },
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 2,
                run_id: "run-current",
                part_type: "status",
                event_type: "turn.completed",
                terminal_status: "completed"
              }
            ],
            ledger_events: [
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 1,
                run_id: "run-current",
                part_type: "status",
                event_type: "turn.started",
                terminal_status: "running"
              },
              {
                protocol_version: "openclaw.ledger.v1",
                seq: 2,
                run_id: "run-current",
                part_type: "status",
                event_type: "turn.completed",
                terminal_status: "completed"
              }
            ]
          }
        });
        const recentEvents = frameByReq(server.frames, "rpc-stale-active-turn-snapshot")?.data?.recent_events || [];
        expect(recentEvents.some((event: any) => event.run_id === "run-stale")).toBe(false);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("backfills historical raw events into separate canonical ledger turns", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-history-backfill-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "first-run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-one" },
        createdAt: "2026-06-11T09:00:00.000Z"
      },
      {
        id: "first-thinking",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.thinking",
        payload: { content: "Thinking for one." },
        createdAt: "2026-06-11T09:00:01.000Z"
      },
      {
        id: "first-answer",
        sessionId: "session-1",
        seq: 3,
        kind: "assistant.message",
        payload: { content: "Answer one." },
        createdAt: "2026-06-11T09:00:02.000Z"
      },
      {
        id: "first-run-completed",
        sessionId: "session-1",
        seq: 4,
        kind: "run.completed",
        payload: { runId: "run-one" },
        createdAt: "2026-06-11T09:00:03.000Z"
      },
      {
        id: "second-run-started",
        sessionId: "session-1",
        seq: 5,
        kind: "run.started",
        payload: { runId: "run-two" },
        createdAt: "2026-06-11T09:01:00.000Z"
      },
      {
        id: "second-thinking",
        sessionId: "session-1",
        seq: 6,
        kind: "assistant.thinking",
        payload: { content: "Thinking for two." },
        createdAt: "2026-06-11T09:01:01.000Z"
      },
      {
        id: "second-answer",
        sessionId: "session-1",
        seq: 7,
        kind: "assistant.message",
        payload: { content: "Answer two." },
        createdAt: "2026-06-11T09:01:02.000Z"
      },
      {
        id: "second-run-completed",
        sessionId: "session-1",
        seq: 8,
        kind: "run.completed",
        payload: { runId: "run-two" },
        createdAt: "2026-06-11T09:01:03.000Z"
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
          req_id: "rpc-history-backfill-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-history-backfill-snapshot");
        expect(frame).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            session_id: "session-1",
            last_seq: 8,
            active_turns: []
          }
        });
        const events = frame?.data?.recent_events || [];
        expect(events).toHaveLength(8);

        const firstThinking = events.find((event: any) => event.seq === 2);
        const secondThinking = events.find((event: any) => event.seq === 6);
        expect(firstThinking).toMatchObject({
          protocol_version: "openclaw.ledger.v1",
          run_id: "run-one",
          part_type: "thinking",
          event_type: "part.replace",
          text: "Thinking for one."
        });
        expect(secondThinking).toMatchObject({
          protocol_version: "openclaw.ledger.v1",
          run_id: "run-two",
          part_type: "thinking",
          event_type: "part.replace",
          text: "Thinking for two."
        });
        expect(firstThinking.turn_id).not.toBe(secondThinking.turn_id);
        expect(firstThinking.active_request_id).not.toBe(secondThinking.active_request_id);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("exposes only canonical ledger events from sessions.events after raw history backfill", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-events-canonical-only-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "raw-run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-canonical-only" },
        createdAt: "2026-06-12T00:00:00.000Z"
      },
      {
        id: "raw-thinking",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.thinking",
        payload: { content: "Raw thinking should be exposed only through ledger payload." },
        createdAt: "2026-06-12T00:00:01.000Z"
      },
      {
        id: "raw-answer",
        sessionId: "session-1",
        seq: 3,
        kind: "assistant.message",
        payload: { content: "Raw answer should be exposed only through ledger payload." },
        createdAt: "2026-06-12T00:00:02.000Z"
      },
      {
        id: "raw-run-completed",
        sessionId: "session-1",
        seq: 4,
        kind: "run.completed",
        payload: { runId: "run-canonical-only" },
        createdAt: "2026-06-12T00:00:03.000Z"
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
          req_id: "rpc-events-canonical-only",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 10, offset: 0 }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-events-canonical-only");
        expect(frame).toMatchObject({ action: "sessions.events", status: "done" });
        const events = frame?.data?.events || [];
        expect(events).toHaveLength(4);
        expect(
          events.every((event: any) => event.payload?.openclaw_ledger?.protocol_version === "openclaw.ledger.v1")
        ).toBe(true);
        expect(frame?.data?.ledger_events).toHaveLength(4);
        expect(frame?.data?.ledger_events.map((event: any) => event.event_type)).toEqual([
          "turn.started",
          "part.replace",
          "part.replace",
          "turn.completed"
        ]);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("limits snapshot ledger payloads for large cached histories", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-snapshot-window-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const historyEvents: GatewayEvent[] = [];
    for (let index = 0; index < 100; index += 1) {
      const turnSeq = index * 3 + 1;
      const runId = `run-window-${index + 1}`;
      historyEvents.push(
        {
          id: `window-run-started-${index + 1}`,
          sessionId: "session-1",
          seq: turnSeq,
          kind: "run.started",
          payload: { runId },
          createdAt: new Date(Date.UTC(2026, 5, 12, 0, 0, index)).toISOString()
        },
        {
          id: `window-answer-${index + 1}`,
          sessionId: "session-1",
          seq: turnSeq + 1,
          kind: "assistant.message",
          payload: { runId, content: `Answer ${index + 1}` },
          createdAt: new Date(Date.UTC(2026, 5, 12, 0, 0, index, 100)).toISOString()
        },
        {
          id: `window-run-completed-${index + 1}`,
          sessionId: "session-1",
          seq: turnSeq + 2,
          kind: "run.completed",
          payload: { runId },
          createdAt: new Date(Date.UTC(2026, 5, 12, 0, 0, index, 200)).toISOString()
        }
      );
    }
    gateway.eventsBySession.set("session-1", historyEvents);

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
          req_id: "rpc-snapshot-window",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-snapshot-window");
        expect(frame).toMatchObject({ action: "sessions.snapshot", status: "done" });
        expect(frame?.data?.last_seq).toBeGreaterThanOrEqual(300);
        expect(frame?.data?.recent_events.length).toBeLessThanOrEqual(240);
        expect(frame?.data?.ledger_events.length).toBeLessThanOrEqual(240);
        expect(frame?.data?.recent_events[0].seq).toBeGreaterThan(1);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not attach the full canonical ledger to older message pages without message seq anchors", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-message-page-ledger-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession({
      id: "session-1",
      title: "Paged history",
      status: "idle",
      hostKind: "qclaw",
      runnerCommand: "gateway",
      createdAt: "2026-06-18T08:00:00.000Z",
      updatedAt: "2026-06-18T08:10:00.000Z",
      lastEventSeq: 6
    });
    gateway.messagesBySession.set("session-1", [
      { id: "m1", sessionId: "session-1", role: "user", content: "first", createdAt: "2026-06-18T08:00:00.000Z" },
      { id: "m2", sessionId: "session-1", role: "assistant", content: "first answer", createdAt: "2026-06-18T08:00:01.000Z" },
      { id: "m3", sessionId: "session-1", role: "user", content: "second", createdAt: "2026-06-18T08:01:00.000Z" }
    ]);
    gateway.eventsBySession.set("session-1", [
      ledgerTimelineEvent("session-1", 1, "turn-1", "turn.started", "first"),
      ledgerTimelineEvent("session-1", 2, "turn-1", "part.replace", "first answer"),
      ledgerTimelineEvent("session-1", 3, "turn-1", "turn.completed", ""),
      ledgerTimelineEvent("session-1", 4, "turn-2", "turn.started", "second"),
      ledgerTimelineEvent("session-1", 5, "turn-2", "part.replace", "second answer"),
      ledgerTimelineEvent("session-1", 6, "turn-2", "turn.completed", "")
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
          req_id: "rpc-message-page-ledger",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "session-1", limit: 1, offset: 1 }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-message-page-ledger");
        expect(frame).toMatchObject({ action: "sessions.messages", status: "done" });
        expect(frame?.data?.messages).toEqual([
          expect.objectContaining({ id: "m1", content: "first" }),
          expect.objectContaining({ id: "m2", content: "first answer" })
        ]);
        expect(frame?.data?.events).toEqual([]);
        expect(frame?.data?.ledger_events).toEqual([]);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("merges local 53AIHub user file metadata into sessions.messages history", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-message-file-metadata-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.messagesBySession.set("session-1", [
      {
        id: "gw-user-1",
        sessionId: "session-1",
        role: "user",
        content: [
          "读取附件",
          "<53aihub-openclaw-runtime-context>",
          "Local input files:",
          "@/Users/y65ng/.qclaw/input-files/request/probe.md",
          "Selected skill: /openclaw_pdf_probe",
          "</53aihub-openclaw-runtime-context>"
        ].join("\n"),
        createdAt: "2026-06-18T08:00:00.000Z"
      },
      {
        id: "gw-assistant-1",
        sessionId: "session-1",
        role: "assistant",
        content: "已读取",
        createdAt: "2026-06-18T08:00:01.000Z"
      }
    ]);

    const localMessages: SessionMessage[] = [
      {
        id: "hub53ai-user-req-1",
        sessionId: "session-1",
        role: "user",
        content: "读取附件",
        createdAt: "2026-06-18T08:00:00.200Z",
        metadata: {
          openclaw_client_message_id: "client-1",
          openclaw_skill: {
            skill_name: "openclaw_pdf_probe",
            display_name: "PDF Probe"
          },
          openclaw_input_files: [
            {
              file_name: "probe.md",
              mime_type: "text/markdown",
              preview_url: "http://localhost:9001/api/preview/probe.md"
            }
          ]
        }
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
        listSessionMessages: () => localMessages,
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
          req_id: "rpc-message-file-metadata",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "session-1", limit: 2, offset: 0 }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-message-file-metadata");
        expect(frame).toMatchObject({ action: "sessions.messages", status: "done" });
        expect(frame?.data?.messages?.[0]).toMatchObject({
          id: "gw-user-1",
          content: "读取附件",
          metadata: {
            openclaw_client_message_id: "client-1",
            openclaw_skill: { skill_name: "openclaw_pdf_probe" },
            openclaw_input_files: [
              expect.objectContaining({
                file_name: "probe.md",
                preview_url: "http://localhost:9001/api/preview/probe.md"
              })
            ]
          }
        });
        expect(frame?.data?.messages?.[0]?.content).not.toContain("<53aihub-openclaw-runtime-context>");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("expands sessions.messages pages to complete user and assistant turn boundaries", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-message-turn-window-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);
    const gateway = new FakeGateway();
    gateway.messagesBySession.set("session-1", [
      { id: "u1", sessionId: "session-1", role: "user", content: "first question", createdAt: "2026-06-18T08:00:00.000Z" },
      { id: "a1", sessionId: "session-1", role: "assistant", content: "first answer", createdAt: "2026-06-18T08:00:01.000Z" },
      { id: "u2", sessionId: "session-1", role: "user", content: "second question", createdAt: "2026-06-18T08:01:00.000Z" },
      { id: "a2", sessionId: "session-1", role: "assistant", content: "second answer", createdAt: "2026-06-18T08:01:01.000Z" }
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
          req_id: "rpc-assistant-boundary",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "session-1", limit: 1, offset: 2 }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-user-boundary",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "session-1", limit: 1, offset: 1 }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-assistant-boundary")).toMatchObject({
          action: "sessions.messages",
          status: "done",
          data: {
            messages: [
              { id: "u1", content: "first question" },
              { id: "a1", content: "first answer" }
            ],
            pagination: { limit: 1, offset: 2, total: 4, hasMore: true, nextOffset: 3 }
          }
        });
        expect(frameByReq(server.frames, "rpc-user-boundary")).toMatchObject({
          action: "sessions.messages",
          status: "done",
          data: {
            messages: [
              { id: "u2", content: "second question" },
              { id: "a2", content: "second answer" }
            ],
            pagination: { limit: 1, offset: 1, total: 4, hasMore: true, nextOffset: 2 }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("backfills delayed history thinking by message sequence instead of physical event order", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-history-backfill-delayed-thinking-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "first-run-started",
        sessionId: "session-1",
        seq: 3,
        kind: "run.started",
        payload: { runId: "run-one" },
        createdAt: "2026-06-11T09:00:00.000Z"
      },
      {
        id: "first-message-status",
        sessionId: "session-1",
        seq: 14,
        kind: "status.update",
        payload: { phase: "message", messageSeq: 2, runId: "run-one" },
        createdAt: "2026-06-11T09:00:01.000Z"
      },
      {
        id: "first-answer",
        sessionId: "session-1",
        seq: 15,
        kind: "assistant.message",
        payload: { content: "Answer one.", runId: "run-one" },
        createdAt: "2026-06-11T09:00:02.000Z"
      },
      {
        id: "first-run-completed",
        sessionId: "session-1",
        seq: 16,
        kind: "run.completed",
        payload: { runId: "run-one" },
        createdAt: "2026-06-11T09:00:03.000Z"
      },
      {
        id: "first-message-done-status",
        sessionId: "session-1",
        seq: 18,
        kind: "status.update",
        payload: { phase: "message", status: "done", messageSeq: 2 },
        createdAt: "2026-06-11T09:00:04.000Z"
      },
      {
        id: "second-run-started",
        sessionId: "session-1",
        seq: 19,
        kind: "run.started",
        payload: { runId: "run-two" },
        createdAt: "2026-06-11T09:01:00.000Z"
      },
      {
        id: "session-1:history:2:thinking",
        sessionId: "session-1",
        seq: 20,
        kind: "assistant.thinking",
        payload: { rawSeq: 2, content: "Thinking for one." },
        createdAt: "2026-06-11T09:01:01.000Z"
      },
      {
        id: "second-message-status",
        sessionId: "session-1",
        seq: 25,
        kind: "status.update",
        payload: { phase: "message", messageSeq: 4, runId: "run-two" },
        createdAt: "2026-06-11T09:01:02.000Z"
      },
      {
        id: "second-answer",
        sessionId: "session-1",
        seq: 27,
        kind: "assistant.message",
        payload: { content: "Answer two.", runId: "run-two" },
        createdAt: "2026-06-11T09:01:03.000Z"
      },
      {
        id: "second-run-completed",
        sessionId: "session-1",
        seq: 28,
        kind: "run.completed",
        payload: { runId: "run-two" },
        createdAt: "2026-06-11T09:01:04.000Z"
      },
      {
        id: "second-message-done-status",
        sessionId: "session-1",
        seq: 30,
        kind: "status.update",
        payload: { phase: "message", status: "done", messageSeq: 4 },
        createdAt: "2026-06-11T09:01:05.000Z"
      },
      {
        id: "third-run-started",
        sessionId: "session-1",
        seq: 31,
        kind: "run.started",
        payload: { runId: "run-three" },
        createdAt: "2026-06-11T09:02:00.000Z"
      },
      {
        id: "session-1:history:4:thinking",
        sessionId: "session-1",
        seq: 40,
        kind: "assistant.thinking",
        payload: { rawSeq: 4, content: "Thinking for two." },
        createdAt: "2026-06-11T09:02:01.000Z"
      },
      {
        id: "third-answer",
        sessionId: "session-1",
        seq: 42,
        kind: "assistant.message",
        payload: { content: "Answer three.", runId: "run-three" },
        createdAt: "2026-06-11T09:02:02.000Z"
      },
      {
        id: "third-run-completed",
        sessionId: "session-1",
        seq: 43,
        kind: "run.completed",
        payload: { runId: "run-three" },
        createdAt: "2026-06-11T09:02:03.000Z"
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
          req_id: "rpc-history-backfill-delayed-thinking-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-history-backfill-delayed-thinking-snapshot");
        const events = frame?.data?.recent_events || [];
        const firstDelayedThinking = events.find((event: any) => String(event.raw_event_ref || "").includes("history:2:thinking"));
        const secondDelayedThinking = events.find((event: any) => String(event.raw_event_ref || "").includes("history:4:thinking"));

        expect(firstDelayedThinking).toMatchObject({
          protocol_version: "openclaw.ledger.v1",
          run_id: "run-one",
          part_type: "thinking",
          event_type: "part.replace",
          text: "Thinking for one."
        });
        expect(secondDelayedThinking).toMatchObject({
          protocol_version: "openclaw.ledger.v1",
          run_id: "run-two",
          part_type: "thinking",
          event_type: "part.replace",
          text: "Thinking for two."
        });
        expect(firstDelayedThinking.turn_id).not.toBe(secondDelayedThinking.turn_id);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("rewrites stale historical canonical thinking events when the mapped turn changes", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-history-backfill-rewrite-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify({
        mappings: {},
        outbox: [],
        canonicalEventsBySession: {
          "session-1": [
            {
              id: "session-1:history:2:thinking",
              sessionId: "session-1",
              seq: 20,
              kind: "assistant.thinking",
              payload: {
                content: "Thinking for one.",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 20,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:history:run-two",
                  run_id: "run-two",
                  active_request_id: "history:run-two",
                  part_id: "session-1:turn:history:run-two:thinking:0",
                  part_type: "thinking",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Thinking for one.",
                  created_at: "2026-06-11T09:01:01.000Z",
                  raw_event_ref: "session-1:20:session-1:history:2:thinking"
                }
              },
              createdAt: "2026-06-11T09:01:01.000Z"
            }
          ]
        }
      })
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "first-run-started",
        sessionId: "session-1",
        seq: 3,
        kind: "run.started",
        payload: { runId: "run-one" },
        createdAt: "2026-06-11T09:00:00.000Z"
      },
      {
        id: "first-message-status",
        sessionId: "session-1",
        seq: 14,
        kind: "status.update",
        payload: { phase: "message", messageSeq: 2, runId: "run-one" },
        createdAt: "2026-06-11T09:00:01.000Z"
      },
      {
        id: "first-answer",
        sessionId: "session-1",
        seq: 15,
        kind: "assistant.message",
        payload: { content: "Answer one.", runId: "run-one" },
        createdAt: "2026-06-11T09:00:02.000Z"
      },
      {
        id: "first-run-completed",
        sessionId: "session-1",
        seq: 16,
        kind: "run.completed",
        payload: { runId: "run-one" },
        createdAt: "2026-06-11T09:00:03.000Z"
      },
      {
        id: "first-message-done-status",
        sessionId: "session-1",
        seq: 18,
        kind: "status.update",
        payload: { phase: "message", status: "done", messageSeq: 2 },
        createdAt: "2026-06-11T09:00:04.000Z"
      },
      {
        id: "second-run-started",
        sessionId: "session-1",
        seq: 19,
        kind: "run.started",
        payload: { runId: "run-two" },
        createdAt: "2026-06-11T09:01:00.000Z"
      },
      {
        id: "session-1:history:2:thinking",
        sessionId: "session-1",
        seq: 20,
        kind: "assistant.thinking",
        payload: { rawSeq: 2, content: "Thinking for one." },
        createdAt: "2026-06-11T09:01:01.000Z"
      },
      {
        id: "second-answer",
        sessionId: "session-1",
        seq: 27,
        kind: "assistant.message",
        payload: { content: "Answer two.", runId: "run-two" },
        createdAt: "2026-06-11T09:01:03.000Z"
      },
      {
        id: "second-run-completed",
        sessionId: "session-1",
        seq: 28,
        kind: "run.completed",
        payload: { runId: "run-two" },
        createdAt: "2026-06-11T09:01:04.000Z"
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
          req_id: "rpc-history-backfill-rewrite-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-history-backfill-rewrite-snapshot");
        const events = frame?.data?.recent_events || [];
        const rewrittenThinking = events.find((event: any) => String(event.raw_event_ref || "").includes("history:2:thinking"));

        expect(rewrittenThinking).toMatchObject({
          protocol_version: "openclaw.ledger.v1",
          run_id: "run-one",
          active_request_id: "history:run-one",
          part_type: "thinking",
          text: "Thinking for one."
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("prunes orphaned historical canonical events that no longer map to a complete turn", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-history-backfill-orphan-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify({
        mappings: {},
        outbox: [],
        canonicalEventsBySession: {
          "session-1": [
            {
              id: "session-1:history:8:thinking",
              sessionId: "session-1",
              seq: 80,
              kind: "assistant.thinking",
              payload: {
                content: "Orphan thinking from a stale history group.",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 80,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:history:run-orphan",
                  run_id: "run-orphan",
                  active_request_id: "history:run-orphan",
                  part_id: "session-1:turn:history:run-orphan:thinking:80",
                  part_type: "thinking",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Orphan thinking from a stale history group.",
                  created_at: "2026-06-11T09:08:01.000Z",
                  raw_event_ref: "session-1:80:session-1:history:8:thinking"
                }
              },
              createdAt: "2026-06-11T09:08:01.000Z"
            }
          ]
        }
      })
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.eventsBySession.set("session-1", [
      {
        id: "run-started",
        sessionId: "session-1",
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-current" },
        createdAt: "2026-06-11T09:00:00.000Z"
      },
      {
        id: "answer-final",
        sessionId: "session-1",
        seq: 2,
        kind: "assistant.message",
        payload: { content: "Current answer.", runId: "run-current" },
        createdAt: "2026-06-11T09:00:01.000Z"
      },
      {
        id: "run-completed",
        sessionId: "session-1",
        seq: 3,
        kind: "run.completed",
        payload: { runId: "run-current" },
        createdAt: "2026-06-11T09:00:02.000Z"
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
          req_id: "rpc-history-backfill-orphan-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-history-backfill-orphan-snapshot");
        expect(frame).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            active_turns: []
          }
        });

        const events = frame?.data?.recent_events || [];
        expect(events.some((event: any) => event.run_id === "run-orphan")).toBe(false);
        expect(events.some((event: any) => String(event.text || "").includes("Orphan thinking"))).toBe(false);
        expect(events.some((event: any) => event.run_id === "run-current" && event.event_type === "turn.completed")).toBe(true);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not expose historical running turn when same run already completed", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-history-running-completed-run-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify({
        mappings: {},
        outbox: [],
        canonicalEventsBySession: {
          "session-1": [
            {
              id: "session-1:46:run.started",
              sessionId: "session-1",
              seq: 46,
              kind: "run.started",
              payload: {
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 46,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-live",
                  run_id: "run-shared",
                  active_request_id: "req-live",
                  part_id: "session-1:turn:req-live:status",
                  part_type: "status",
                  event_type: "turn.started",
                  operation: "noop",
                  visibility: "hidden",
                  terminal_status: "running",
                  created_at: "2026-06-11T09:00:00.000Z",
                  raw_event_ref: "session-1:46:run.started"
                }
              },
              createdAt: "2026-06-11T09:00:00.000Z"
            },
            {
              id: "session-1:53:assistant.message",
              sessionId: "session-1",
              seq: 53,
              kind: "assistant.message",
              payload: {
                content: "Live final answer.",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 53,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-live",
                  run_id: "run-shared",
                  active_request_id: "req-live",
                  part_id: "session-1:turn:req-live:answer:0",
                  part_type: "answer",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Live final answer.",
                  created_at: "2026-06-11T09:00:05.000Z",
                  raw_event_ref: "session-1:53:assistant.message"
                }
              },
              createdAt: "2026-06-11T09:00:05.000Z"
            },
            {
              id: "session-1:57:run.completed",
              sessionId: "session-1",
              seq: 57,
              kind: "run.completed",
              payload: {
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 57,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-live",
                  run_id: "run-shared",
                  active_request_id: "req-live",
                  part_id: "session-1:turn:req-live:status",
                  part_type: "status",
                  event_type: "turn.completed",
                  operation: "close",
                  visibility: "hidden",
                  terminal_status: "completed",
                  created_at: "2026-06-11T09:00:07.000Z",
                  raw_event_ref: "session-1:57:run.completed"
                }
              },
              createdAt: "2026-06-11T09:00:07.000Z"
            },
            {
              id: "session-1:history:8:thinking",
              sessionId: "session-1",
              seq: 80,
              kind: "assistant.thinking",
              payload: {
                content: "Historical thinking replay.",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 80,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:history:run-shared",
                  run_id: "run-shared",
                  active_request_id: "history:run-shared",
                  part_id: "session-1:turn:history:run-shared:thinking:80",
                  part_type: "thinking",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Historical thinking replay.",
                  created_at: "2026-06-11T09:00:08.000Z",
                  raw_event_ref: "session-1:80:session-1:history:8:thinking"
                }
              },
              createdAt: "2026-06-11T09:00:08.000Z"
            }
          ]
        }
      })
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession(fakeGatewaySession("session-1"));
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
          req_id: "rpc-history-running-completed-run-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-history-running-completed-run-snapshot");
        expect(frame).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            active_turns: []
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("dedupes historical answer backfill when a live canonical answer exists for the same run", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-history-answer-dedupe-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify({
        mappings: {},
        outbox: [],
        canonicalEventsBySession: {
          "session-1": [
            {
              id: "session-1:46:run.started",
              sessionId: "session-1",
              seq: 46,
              kind: "run.started",
              payload: {
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 46,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-live",
                  run_id: "run-shared",
                  active_request_id: "req-live",
                  part_id: "session-1:turn:req-live:status",
                  part_type: "status",
                  event_type: "turn.started",
                  operation: "noop",
                  visibility: "hidden",
                  terminal_status: "running",
                  created_at: "2026-06-11T09:00:00.000Z",
                  raw_event_ref: "session-1:46:run.started"
                }
              },
              createdAt: "2026-06-11T09:00:00.000Z"
            },
            {
              id: "session-1:53:assistant.message",
              sessionId: "session-1",
              seq: 53,
              kind: "assistant.message",
              payload: {
                content: "Shared final answer.",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 53,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-live",
                  run_id: "run-shared",
                  active_request_id: "req-live",
                  part_id: "session-1:turn:req-live:answer:0",
                  part_type: "answer",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Shared final answer.",
                  payload: { source_kind: "assistant.message" },
                  created_at: "2026-06-11T09:00:05.000Z",
                  raw_event_ref: "session-1:53:assistant.message"
                }
              },
              createdAt: "2026-06-11T09:00:05.000Z"
            },
            {
              id: "session-1:57:run.completed",
              sessionId: "session-1",
              seq: 57,
              kind: "run.completed",
              payload: {
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 57,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-live",
                  run_id: "run-shared",
                  active_request_id: "req-live",
                  part_id: "session-1:turn:req-live:status",
                  part_type: "status",
                  event_type: "turn.completed",
                  operation: "close",
                  visibility: "hidden",
                  terminal_status: "completed",
                  created_at: "2026-06-11T09:00:07.000Z",
                  raw_event_ref: "session-1:57:run.completed"
                }
              },
              createdAt: "2026-06-11T09:00:07.000Z"
            },
            {
              id: "session-1:history:8:answer",
              sessionId: "session-1",
              seq: 80,
              kind: "assistant.message",
              payload: {
                content: "Shared final answer.",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 80,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:history:run-shared",
                  run_id: "run-shared",
                  active_request_id: "history:run-shared",
                  part_id: "session-1:turn:history:run-shared:answer:0",
                  part_type: "answer",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Shared final answer.",
                  payload: { source_kind: "assistant.message" },
                  created_at: "2026-06-11T09:00:08.000Z",
                  raw_event_ref: "session-1:80:session-1:history:8:answer"
                }
              },
              createdAt: "2026-06-11T09:00:08.000Z"
            }
          ]
        }
      })
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession(fakeGatewaySession("session-1"));
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
          req_id: "rpc-history-answer-dedupe-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-history-answer-dedupe-events",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", after_seq: 57, limit: 20, offset: 0 }
        })
      );

      await waitFor(() => {
        const snapshotFrame = frameByReq(server.frames, "rpc-history-answer-dedupe-snapshot");
        const snapshotAnswers = (snapshotFrame?.data?.recent_events || []).filter(
          (event: any) => event.part_type === "answer"
        );
        expect(snapshotAnswers).toHaveLength(1);
        expect(snapshotAnswers[0]).toMatchObject({
          run_id: "run-shared",
          active_request_id: "req-live",
          part_id: "session-1:turn:req-live:answer:0",
          text: "Shared final answer."
        });

        const eventsFrame = frameByReq(server.frames, "rpc-history-answer-dedupe-events");
        const replayAnswers = (eventsFrame?.data?.ledger_events || []).filter(
          (event: any) => event.part_type === "answer"
        );
        expect(replayAnswers).toHaveLength(0);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("filters stale history turns and mismatched output files when replaying a completed live run", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-history-output-scope-"));
    cleanupPaths.push(stateDir);
    const sessionId = "session-1";
    const liveTurnId = `${sessionId}:turn:req-five`;
    const historyTurnId = `${sessionId}:turn:history:run-five`;
    const fiveFile = {
      id: "local:/tmp/test_document_5words.txt",
      file_name: "test_document_5words.txt",
      mime_type: "text/plain",
      size: 15,
      content: "测试用文档",
      source_kind: "tool.write"
    };
    const staleFiveFile = {
      id: "local-history-five-stale",
      file_name: "test_document_5words.txt",
      mime_type: "text/plain",
      size: 18,
      base64: "5rWL6K+V5paH5qGj5YaF5a65"
    };
    const leakedFifteenFile = {
      id: "local-history-fifteen",
      file_name: "test_document_15words.txt",
      mime_type: "text/plain",
      size: 45,
      base64: "6L+Z5piv5LiA5Liq5YyF5ZCr5Y2B5LqU5Liq5a2X55qE5rWL6K+V5paH5qGj"
    };
    const eventWithLedger = (
      id: string,
      seq: number,
      kind: GatewayEvent["kind"],
      ledger: Record<string, unknown>,
      extraPayload: Record<string, unknown> = {}
    ) => ({
      id,
      sessionId,
      seq,
      kind,
      payload: {
        ...extraPayload,
        openclaw_ledger: {
          protocol_version: "openclaw.ledger.v1",
          seq,
          session_id: sessionId,
          conversation_id: sessionId,
          created_at: "2026-06-12T02:20:00.000Z",
          raw_event_ref: `${sessionId}:${seq}:${id}`,
          ...ledger
        }
      },
      createdAt: "2026-06-12T02:20:00.000Z"
    });
    const outputFileEvent = (
      id: string,
      seq: number,
      ledger: Record<string, unknown>,
      files: unknown[]
    ) =>
      eventWithLedger(
        id,
        seq,
        "process.step",
        {
          part_type: "output_file",
          event_type: "part.replace",
          operation: "replace",
          visibility: "final",
          payload: {
            process_step: {
              step_code: "output_files",
              status: "completed",
              data: { files }
            }
          },
          ...ledger
        },
        {
          process_step: {
            step_code: "output_files",
            status: "completed",
            data: { files }
          }
        }
      );

    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify({
        mappings: {},
        outbox: [],
        canonicalEventsBySession: {
          [sessionId]: [
            eventWithLedger("live-answer", 10, "assistant.message", {
              turn_id: liveTurnId,
              run_id: "run-five",
              active_request_id: "req-five",
              part_id: `${liveTurnId}:answer:1`,
              part_type: "answer",
              event_type: "part.replace",
              operation: "replace",
              visibility: "final",
              text: "完成！我已经为您生成了一个包含5个字的纯文本测试文档。"
            }, { content: "完成！我已经为您生成了一个包含5个字的纯文本测试文档。" }),
            outputFileEvent("live-five-stale-file", 11, {
              turn_id: liveTurnId,
              run_id: "run-five",
              active_request_id: "req-five",
              part_id: `${liveTurnId}:output_files:name:test_document_5words.txt`
            }, [staleFiveFile]),
            outputFileEvent("live-five-final-file", 12, {
              turn_id: liveTurnId,
              run_id: "run-five",
              active_request_id: "req-five",
              part_id: `${liveTurnId}:output_files:name:test_document_5words.txt`
            }, [fiveFile]),
            eventWithLedger("live-completed", 12, "run.completed", {
              turn_id: liveTurnId,
              run_id: "run-five",
              active_request_id: "req-five",
              part_id: `${liveTurnId}:status`,
              part_type: "status",
              event_type: "turn.completed",
              operation: "close",
              visibility: "hidden",
              terminal_status: "completed"
            }),
            eventWithLedger("history-duplicate-answer", 12, "assistant.message", {
              turn_id: historyTurnId,
              run_id: "run-five",
              active_request_id: "history:run-five",
              part_id: `${historyTurnId}:answer:0`,
              part_type: "answer",
              event_type: "part.replace",
              operation: "replace",
              visibility: "final",
              text: "完成！我已经为您生成了一个包含5个字的纯文本测试文档。"
            }, { content: "完成！我已经为您生成了一个包含5个字的纯文本测试文档。" }),
            outputFileEvent("history-leaked-fifteen-file", 12, {
              turn_id: historyTurnId,
              run_id: "run-fifteen",
              active_request_id: "history:run-five",
              part_id: `${historyTurnId}:output_files:name:test_document_15words.txt`
            }, [leakedFifteenFile])
          ]
        }
      })
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession(fakeGatewaySession("session-1"));
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
          req_id: "rpc-history-output-scope-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: sessionId }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-history-output-scope-snapshot");
        const events = frame?.data?.recent_events || [];
        const answers = events.filter((event: any) => event.part_type === "answer");
        const outputFiles = events.filter((event: any) => event.part_type === "output_file");
        expect(answers).toHaveLength(1);
        expect(answers[0]).toMatchObject({
          active_request_id: "req-five",
          part_id: `${liveTurnId}:answer:0`
        });
        expect(events.some((event: any) => event.active_request_id === "history:run-five")).toBe(false);
        expect(JSON.stringify(events)).not.toContain("test_document_15words.txt");
        expect(outputFiles).toHaveLength(1);
        expect(outputFiles[0].payload.process_step.data.files).toEqual([
          expect.objectContaining({
            file_name: "test_document_5words.txt",
            content: "测试用文档"
          })
        ]);
        const seqs = events.map((event: any) => event.seq);
        expect(new Set(seqs).size).toBe(seqs.length);
      });
    } finally {
      await bridge.stop();
    }
  });

  it("synthesizes interrupted terminal for orphaned running turn after host restart", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-orphan-running-turn-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify({
        mappings: {},
        outbox: [],
        canonicalEventsBySession: {
          "session-1": [
            {
              id: "session-1:1:run.started",
              sessionId: "session-1",
              seq: 1,
              kind: "run.started",
              payload: {
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 1,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-orphan",
                  run_id: "run-orphan",
                  active_request_id: "req-orphan",
                  part_id: "session-1:turn:req-orphan:status",
                  part_type: "status",
                  event_type: "turn.started",
                  operation: "noop",
                  visibility: "hidden",
                  terminal_status: "running",
                  created_at: "2026-06-11T08:00:00.000Z",
                  raw_event_ref: "session-1:1:run.started"
                }
              },
              createdAt: "2026-06-11T08:00:00.000Z"
            },
            {
              id: "session-1:2:assistant.message",
              sessionId: "session-1",
              seq: 2,
              kind: "assistant.message",
              payload: {
                content: "Partial answer before QClaw restarted.",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 2,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:req-orphan",
                  run_id: "run-orphan",
                  active_request_id: "req-orphan",
                  part_id: "session-1:turn:req-orphan:answer:0",
                  part_type: "answer",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Partial answer before QClaw restarted.",
                  created_at: "2026-06-11T08:00:01.000Z",
                  raw_event_ref: "session-1:2:assistant.message"
                }
              },
              createdAt: "2026-06-11T08:00:01.000Z"
            }
          ]
        }
      })
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession(fakeGatewaySession("session-1"));
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
          req_id: "rpc-orphan-running-turn-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-orphan-running-turn-snapshot");
        expect(frame).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            active_turns: []
          }
        });

        const terminal = frame?.data?.recent_events?.find(
          (event: any) =>
            event.turn_id === "session-1:turn:req-orphan" &&
            event.event_type === "turn.interrupted" &&
            event.terminal_status === "interrupted"
        );
        expect(terminal).toMatchObject({
          active_request_id: "req-orphan",
          run_id: "run-orphan",
          raw_event_ref: expect.stringContaining("orphaned_running_turn_after_host_restart")
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("hides raw-only placeholder history from sessions.events", async () => {
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
            events: [],
            ledger_events: [],
            pagination: { limit: 10, offset: 0, total: 0, hasMore: false }
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

  it("hides raw-only mismatched protocol segment events from sessions.events", async () => {
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
            events: [],
            ledger_events: [],
            pagination: { limit: 10, offset: 0, total: 0, hasMore: false }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("hides raw-only history message seq metadata from sessions.events", async () => {
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
            events: [],
            ledger_events: [],
            pagination: { limit: 10, offset: 0, total: 0, hasMore: false }
          }
        });
      });
    } finally {
      await bridge.stop();
    }
  });

  it("hides raw-only history thinking snapshots from sessions.events", async () => {
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
            events: [],
            ledger_events: [],
            pagination: { limit: 10, offset: 0, total: 0, hasMore: false }
          }
        });
        expect(frame.data.events.map((event: any) => event.id)).not.toContain("session-1:thinking:1675");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("hides raw-only assistant message echoes from sessions.events", async () => {
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
            events: [],
            ledger_events: [],
            pagination: { limit: 10, offset: 0, total: 0, hasMore: false }
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

  it("does not canonicalize synthetic typed transcript answers during historical snapshot backfill", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-typed-final-history-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const polluted = "Let me try another format.The network seems blocked.抱歉，网络不稳定。";
    const typedText = "抱歉，网络不稳定。";
    gateway.eventsBySession.set("session-1", [
      {
        id: "run-started",
        sessionId: "session-1",
        seq: 20,
        kind: "run.started",
        payload: { runId: "run-history-typed-final" },
        createdAt: "2026-06-12T07:30:00.000Z"
      },
      {
        id: "thinking-history",
        sessionId: "session-1",
        seq: 21,
        kind: "assistant.thinking",
        payload: {
          content: "Let me try another format.",
          runId: "run-history-typed-final"
        },
        createdAt: "2026-06-12T07:30:01.000Z"
      },
      {
        id: "polluted-history-final",
        sessionId: "session-1",
        seq: 22,
        kind: "assistant.message",
        payload: {
          content: polluted,
          runId: "run-history-typed-final",
          state: "final",
          mode: "replace",
          replace: true
        },
        createdAt: "2026-06-12T07:30:02.000Z"
      },
      {
        id: "run-completed",
        sessionId: "session-1",
        seq: 23,
        kind: "run.completed",
        payload: { runId: "run-history-typed-final" },
        createdAt: "2026-06-12T07:30:03.000Z"
      }
    ]);
    gateway.messagesBySession.set("session-1", [
      {
        id: "assistant-history-typed-final",
        sessionId: "session-1",
        role: "assistant",
        content: typedText,
        createdAt: "2026-06-12T07:30:03.000Z",
        seq: 38,
        payload: {
          runId: "run-history-typed-final",
          openclaw_typed_text_segments: [typedText],
          openclaw_typed_text_segment_count: 1
        }
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
          req_id: "rpc-history-typed-final",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-history-typed-final");
        expect(snapshot).toMatchObject({ action: "sessions.snapshot", status: "done" });
        const answerEvents = snapshot?.data?.ledger_events.filter((event: any) => event.part_type === "answer");
        expect(answerEvents).toHaveLength(1);
        expect(answerEvents[0]?.text).toBe(polluted);
        expect(answerEvents[0]?.payload?.source_kind).not.toBe("typed_transcript.final_replace");
        expect(answerEvents[0]?.payload?.typed_final).not.toBe(true);
        expect(JSON.stringify(answerEvents)).not.toContain("assistant-history-typed-final");
        expect(JSON.stringify([...(snapshot?.data?.recent_events || []), ...(snapshot?.data?.ledger_events || [])])).not.toContain(
          "typed_transcript.final_replace"
        );
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not expose persisted synthetic history typed transcript answers through snapshot or events", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-persisted-history-typed-final-"));
    cleanupPaths.push(stateDir);
    await writeFile(
      join(stateDir, "claw-control-center-53aihub.json"),
      JSON.stringify({
        mappings: {},
        outbox: [],
        syntheticEventsBySession: {
          "session-1": [
            {
              id: "typed-final:history:run-stale",
              sessionId: "session-1",
              seq: 88,
              kind: "assistant.message",
              payload: {
                content: "Stale typed final from history.",
                source_kind: "typed_transcript.final_replace",
                typed_final: true,
                active_request_id: "history:run-stale",
                turn_id: "session-1:turn:history:run-stale",
                openclaw_ledger: {
                  protocol_version: "openclaw.ledger.v1",
                  seq: 88,
                  session_id: "session-1",
                  conversation_id: "session-1",
                  turn_id: "session-1:turn:history:run-stale",
                  run_id: "run-stale",
                  active_request_id: "history:run-stale",
                  part_id: "session-1:turn:history:run-stale:answer:0",
                  part_type: "answer",
                  event_type: "part.replace",
                  operation: "replace",
                  visibility: "final",
                  text: "Stale typed final from history.",
                  payload: {
                    source_kind: "typed_transcript.final_replace",
                    typed_final: true
                  },
                  created_at: "2026-06-12T07:31:00.000Z",
                  raw_event_ref: "session-1:88:typed-final:history:run-stale"
                }
              },
              createdAt: "2026-06-12T07:31:00.000Z"
            }
          ]
        }
      })
    );

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.upsertSession(fakeGatewaySession("session-1"));
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
          req_id: "rpc-persisted-history-typed-final-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: "session-1" }
        })
      );
      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-persisted-history-typed-final-events",
          action: "sessions.events",
          status: "request",
          data: { session_id: "session-1", limit: 20, offset: 0 }
        })
      );

      await waitFor(() => {
        const snapshot = frameByReq(server.frames, "rpc-persisted-history-typed-final-snapshot");
        expect(snapshot).toMatchObject({
          action: "sessions.snapshot",
          status: "done",
          data: {
            active_turns: [],
            recent_events: [],
            ledger_events: []
          }
        });
        expect(JSON.stringify(snapshot?.data || {})).not.toContain("typed_transcript.final_replace");
        expect(JSON.stringify(snapshot?.data || {})).not.toContain("Stale typed final from history.");

        const events = frameByReq(server.frames, "rpc-persisted-history-typed-final-events");
        expect(events).toMatchObject({
          action: "sessions.events",
          status: "done",
          data: {
            events: [],
            ledger_events: []
          }
        });
        expect(JSON.stringify(events?.data || {})).not.toContain("typed_transcript.final_replace");
        expect(JSON.stringify(events?.data || {})).not.toContain("Stale typed final from history.");
      });
    } finally {
      await bridge.stop();
    }
  });

  it("normalizes cumulative answer snapshots after write tools and emits canonical output files", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-cumulative-write-output-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    const sessionId = "agent:main:dashboard:cumulative-write";
    const intro = "好的！我来为您生成一个包含15个字的纯文本测试文档。";
    const final = [
      "完成！我已经为您生成了一个包含15个字的纯文本测试文档。",
      "",
      "**文件信息：**",
      "- 文件路径：`/tmp/test_document_15words.txt`",
      "- 文件内容：`这是一个包含十五个字的测试文档`",
    ].join("\n");
    gateway.upsertSession({
      id: sessionId,
      title: "53AI Hub-Alex：测试",
      status: "idle",
      hostKind: "openclaw",
      runnerCommand: "gateway",
      createdAt: "2026-06-12T01:57:00.000Z",
      updatedAt: "2026-06-12T01:57:00.000Z",
      lastEventSeq: 0
    });
    gateway.eventsToEmit = [
      {
        id: "run-started",
        sessionId,
        seq: 1,
        kind: "run.started",
        payload: { runId: "run-write" },
        createdAt: "2026-06-12T01:57:00.000Z"
      },
      {
        id: "answer-intro",
        sessionId,
        seq: 2,
        kind: "assistant.delta",
        payload: { content: intro, runId: "run-write", rawSeq: 53 },
        createdAt: "2026-06-12T01:57:05.000Z"
      },
      {
        id: "thinking-before-intro-but-late",
        sessionId,
        seq: 3,
        kind: "assistant.thinking",
        payload: { content: "先分析十五字文档。", runId: "run-write", rawSeq: 49 },
        createdAt: "2026-06-12T01:57:01.000Z"
      },
      {
        id: "tool-call-write",
        sessionId,
        seq: 4,
        kind: "tool.call",
        payload: {
          runId: "run-write",
          rawSeq: 54,
          data: {
            phase: "start",
            name: "write",
            toolCallId: "tool-write-1",
            args: {
              path: "/tmp/test_document_15words.txt",
              content: "这是一个包含十五个字的测试文档"
            }
          }
        },
        createdAt: "2026-06-12T01:57:06.000Z"
      },
      {
        id: "tool-result-write",
        sessionId,
        seq: 5,
        kind: "tool.result",
        payload: {
          runId: "run-write",
          rawSeq: 56,
          data: {
            phase: "result",
            name: "write",
            toolCallId: "tool-write-1",
            isError: false
          }
        },
        createdAt: "2026-06-12T01:57:07.000Z"
      },
      {
        id: "answer-cumulative",
        sessionId,
        seq: 6,
        kind: "assistant.delta",
        payload: {
          content: `${intro}${final}`,
          runId: "run-write",
          rawSeq: 129,
          mode: "replace",
          replace: true
        },
        createdAt: "2026-06-12T01:57:12.000Z"
      },
      {
        id: "thinking-delayed-before-final",
        sessionId,
        seq: 7,
        kind: "assistant.thinking",
        payload: { content: "复核工具写入结果。", runId: "run-write", rawSeq: 51 },
        createdAt: "2026-06-12T01:57:08.000Z"
      },
      {
        id: "answer-final-cumulative",
        sessionId,
        seq: 8,
        kind: "assistant.message",
        payload: {
          content: `${intro}${final}`,
          runId: "run-write",
          rawSeq: 136,
          mode: "replace"
        },
        createdAt: "2026-06-12T01:57:13.000Z"
      },
      {
        id: "run-completed",
        sessionId,
        seq: 9,
        kind: "run.completed",
        payload: { runId: "run-write" },
        createdAt: "2026-06-12T01:57:14.000Z"
      }
    ];
    gateway.messagesBySession.set(sessionId, [
      {
        id: "assistant-run-write-final",
        sessionId,
        role: "assistant",
        content: `${intro}${final}`,
        createdAt: "2026-06-12T01:57:14.000Z",
        seq: 10,
        payload: {
          runId: "run-write",
          openclaw_typed_text_segments: [`${intro}${final}`],
          openclaw_typed_text_segment_count: 1
        }
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
          req_id: "req-cumulative-write",
          action: "chat",
          data: {
            user: "agenthub_u2001",
            conversation_id: sessionId,
            metadata: { openclaw_client_message_id: "client-cumulative-write" },
            messages: [{ role: "user", content: "生成一个十五字的测试文档" }]
          }
        })
      );

      await waitFor(() => {
        expect(server.frames.some((frame) => frame.req_id === "req-cumulative-write" && frame.status === "done")).toBe(true);
      });

      connection.socket.send(
        JSON.stringify({
          req_id: "rpc-cumulative-write-snapshot",
          action: "sessions.snapshot",
          status: "request",
          data: { session_id: sessionId }
        })
      );

      await waitFor(() => {
        const frame = frameByReq(server.frames, "rpc-cumulative-write-snapshot");
        const events = frame?.data?.recent_events || [];
        const answers = events.filter((event: any) => event.part_type === "answer");
        const finalAnswer = answers.find((event: any) => String(event.part_id).endsWith(":answer:0") && event.visibility === "final");
        expect(answers.map((event: any) => event.part_id.split(":").slice(-2).join(":"))).not.toContain("answer:1");
        expect(finalAnswer).toMatchObject({
          part_type: "answer",
          operation: "replace",
          text: `${intro}${final}`
        });
        expect(finalAnswer.text).toBe(`${intro}${final}`);
        expect(events).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              part_type: "output_file",
              payload: expect.objectContaining({
                process_step: expect.objectContaining({
                  data: expect.objectContaining({
                    files: expect.arrayContaining([
                      expect.objectContaining({
                        file_name: "test_document_15words.txt",
                        content: "这是一个包含十五个字的测试文档"
                      })
                    ])
                  })
                })
              })
            })
          ])
        );
      });
    } finally {
      await bridge.stop();
    }
  });

  it("returns NOT_FOUND for explicit session history RPCs when the gateway list proves the session is missing", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-missing-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.sessionPage = {
      sessions: [],
      pagination: {
        limit: 50,
        offset: 0,
        total: 0,
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
      for (const action of ["sessions.messages", "sessions.events", "sessions.snapshot"] as const) {
        connection.socket.send(
          JSON.stringify({
            req_id: `rpc-missing-${action}`,
            action,
            status: "request",
            data: { session_id: "deleted-session" }
          })
        );
      }

      await waitFor(() => {
        for (const action of ["sessions.messages", "sessions.events", "sessions.snapshot"] as const) {
          expect(frameByReq(server.frames, `rpc-missing-${action}`)).toMatchObject({
            action,
            status: "error",
            data: {
              code: "NOT_FOUND"
            }
          });
        }
      });
    } finally {
      await bridge.stop();
    }
  });

  it("does not convert gateway session-list failures into NOT_FOUND history RPCs", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-53aihub-rpc-list-failure-"));
    cleanupPaths.push(stateDir);

    const server = await createFakeHubServer();
    cleanupServers.push(server.close);

    const gateway = new FakeGateway();
    gateway.failListSessionPageMessage = "gateway unavailable";

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
          req_id: "rpc-list-failure-messages",
          action: "sessions.messages",
          status: "request",
          data: { session_id: "possibly-existing-session" }
        })
      );

      await waitFor(() => {
        expect(frameByReq(server.frames, "rpc-list-failure-messages")).toMatchObject({
          action: "sessions.messages",
          status: "error"
        });
        expect(frameByReq(server.frames, "rpc-list-failure-messages")?.data).not.toMatchObject({
          code: "NOT_FOUND"
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
  sentMessages: Array<{ sessionId: string; content: string; attachments?: any[] }> = [];
  eventsToEmit?: GatewayEvent[];
  disconnectOnNextSend = false;
  disconnectCompletionDelayMs = 0;
  failNextAttachmentSendMessage = "";
  beforeEmit?: (sessionId: string) => void | Promise<void>;
  createdTitles: string[] = [];
  renames: Array<{ sessionId: string; title: string }> = [];
  controls: Array<{ sessionId: string; action: string }> = [];
  private createTitleFailures = new Map<string, string>();
  failListSessionPageMessage = "";
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

  async listSessionPage(options: { limit?: number; offset?: number } = {}): Promise<{
    sessions: GatewaySession[];
    pagination: { limit: number; offset: number; total?: number; hasMore: boolean; nextOffset?: number };
  }> {
    if (this.failListSessionPageMessage) {
      const message = this.failListSessionPageMessage;
      this.failListSessionPageMessage = "";
      throw new Error(message);
    }
    if (this.sessionPage) {
      return this.sessionPage;
    }
    const allSessions = [...this.sessions.values()];
    const knownSessionIds = new Set(allSessions.map((session) => session.id));
    for (const sessionId of new Set([...this.messagesBySession.keys(), ...this.eventsBySession.keys()])) {
      if (knownSessionIds.has(sessionId)) continue;
      knownSessionIds.add(sessionId);
      allSessions.push({
        id: sessionId,
        title: sessionId,
        status: "idle",
        hostKind: "qclaw",
        runnerCommand: "gateway",
        createdAt: "2026-05-20T10:00:00.000Z",
        updatedAt: "2026-05-20T10:00:00.000Z",
        lastEventSeq: 0
      });
    }
    const offset = Math.max(0, options.offset ?? 0);
    const limit = Math.max(1, options.limit ?? (allSessions.length || 1));
    const sessions = allSessions.slice(offset, offset + limit);
    const hasMore = offset + sessions.length < allSessions.length;
    return {
      sessions,
      pagination: {
        limit,
        offset,
        total: allSessions.length,
        hasMore,
        ...(hasMore ? { nextOffset: offset + sessions.length } : {})
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

  async sendMessage(sessionId: string, content: string, options: { attachments?: any[] } = {}): Promise<void> {
    if (options.attachments?.length && this.failNextAttachmentSendMessage) {
      const message = this.failNextAttachmentSendMessage;
      this.failNextAttachmentSendMessage = "";
      throw new Error(message);
    }
    this.sentMessages.push({
      sessionId,
      content,
      ...(options.attachments?.length ? { attachments: options.attachments } : {})
    });
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

async function createFakeHubServer(
  port?: number,
  options?: { artifactUploadResponse?: Record<string, unknown> }
): Promise<{
  url: string;
  port: number;
  frames: Hub53AIOutgoingFrame[];
  connected: Promise<{ socket: WebSocket; headers: Record<string, string | undefined> }>;
  close: () => Promise<void>;
}> {
  const httpServer = createServer((req, res) => {
    if (options?.artifactUploadResponse && req.method === "POST" && req.url === "/api/v1/openclaw/artifacts") {
      req.resume();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ data: options.artifactUploadResponse }));
      return;
    }
    res.writeHead(404);
    res.end("not found");
  });
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

function buildOutputManifestRecord(input: {
  conversationId: string;
  turnId: string;
  activeRequestId: string;
  path: string;
  logicalPath: string;
  mimeType: string;
  content: string;
}) {
  return {
    conversation_id: input.conversationId,
    turn_id: input.turnId,
    active_request_id: input.activeRequestId,
    part_id: `${input.turnId}:output`,
    path: input.path,
    logical_path: input.logicalPath,
    mime_type: input.mimeType,
    size: Buffer.byteLength(input.content),
    sha256: createHash("sha256").update(input.content).digest("hex"),
    created_at: "2026-06-23T00:00:00.000Z",
    source_kind: "tool.write"
  };
}

function visibleSentMessages(gateway: {
  sentMessages: Array<{ sessionId: string; content: string; attachments?: any[] }>;
}) {
  return gateway.sentMessages.map((message) => ({
    ...message,
    content: stripRuntimeContext(message.content)
  }));
}

function stripRuntimeContext(content: string): string {
  return content
    .replace(/\n*<53aihub-openclaw-runtime-context>[\s\S]*?<\/53aihub-openclaw-runtime-context>/g, "")
    .trim();
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
