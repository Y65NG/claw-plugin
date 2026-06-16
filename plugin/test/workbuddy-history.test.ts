import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { loadWorkBuddyHistory, sanitizeWorkBuddyChannelHistory } from "../src/workbuddy-history";

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("WorkBuddy history adapter", () => {
  it("parses WorkBuddy JSONL messages, titles, and timestamps", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "Users__y65ng__Project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      join(projectDir, "session-a.jsonl"),
      [
        JSON.stringify({
          type: "message",
          role: "user",
          timestamp: "2026-06-01T01:00:00.000Z",
          content: [{ type: "input_text", text: "hello workbuddy" }]
        }),
        JSON.stringify({
          type: "message",
          role: "assistant",
          timestamp: "2026-06-01T01:01:00.000Z",
          content: [{ type: "output_text", text: "hello hub" }]
        }),
        JSON.stringify({
          type: "ai-title",
          timestamp: "2026-06-01T01:02:00.000Z",
          title: "Parsed title"
        })
      ].join("\n") + "\n"
    );

    const snapshot = await loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") });

    expect(snapshot.sessions).toEqual([
      expect.objectContaining({
        id: "session-a",
        title: "Parsed title",
        status: "completed",
        hostKind: "workbuddy",
        runnerCommand: "workbuddy",
        createdAt: "2026-06-01T01:00:00.000Z",
        updatedAt: "2026-06-01T01:02:00.000Z",
        lastEventSeq: 2
      })
    ]);
    expect(snapshot.messagesBySessionId.get("session-a")).toEqual([
      expect.objectContaining({ role: "user", content: "hello workbuddy" }),
      expect.objectContaining({ role: "assistant", content: "hello hub" })
    ]);
  });

  it("falls back to JSONL-only history when SQLite cannot be read", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-nosqlite-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(join(workbuddyHome, "workbuddy.db"), "not sqlite");
    await writeFile(
      join(projectDir, "session-b.jsonl"),
      JSON.stringify({
        type: "message",
        role: "user",
        timestamp: "2026-06-01T02:00:00.000Z",
        content: "jsonl survives"
      }) + "\n"
    );

    await expect(loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") })).resolves
      .toMatchObject({
        sessions: [expect.objectContaining({ id: "session-b" })]
      });
  });

  it("maps WorkBuddy reasoning and tool records to OpenClaw timeline events", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-events-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      join(projectDir, "session-events.jsonl"),
      [
        JSON.stringify({
          type: "message",
          role: "user",
          timestamp: "2026-06-01T02:00:00.000Z",
          content: "event test"
        }),
        JSON.stringify({
          id: "reasoning-1",
          type: "reasoning",
          timestamp: "2026-06-01T02:00:10.000Z",
          rawContent: [{ type: "reasoning_text", text: "thinking text" }]
        }),
        JSON.stringify({
          id: "tool-call-1",
          type: "function_call",
          timestamp: "2026-06-01T02:00:20.000Z",
          name: "DeferExecuteTool",
          callId: "call-1",
          arguments: JSON.stringify({
            toolName: "mcp__53aihub-channel__reply",
            params: { chat_id: "chat-a", req_id: "req-a", text: "reply" }
          })
        }),
        JSON.stringify({
          id: "tool-result-1",
          type: "function_call_result",
          timestamp: "2026-06-01T02:00:30.000Z",
          name: "DeferExecuteTool",
          callId: "call-1",
          status: "completed",
          output: { type: "text", text: "sent" }
        })
      ].join("\n") + "\n"
    );

    const snapshot = await loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") });

    expect(snapshot.eventsBySessionId.get("session-events")).toEqual([
      expect.objectContaining({
        id: "reasoning-1",
        seq: 2,
        kind: "assistant.thinking",
        payload: expect.objectContaining({ content: "thinking text" })
      }),
      expect.objectContaining({
        id: "tool-call-1",
        seq: 3,
        kind: "tool.call",
        payload: expect.objectContaining({
          data: expect.objectContaining({
            name: "mcp__53aihub-channel__reply",
            args: { chat_id: "chat-a", req_id: "req-a", text: "reply" }
          })
        })
      }),
      expect.objectContaining({
        id: "tool-call-1:answer",
        seq: 4,
        kind: "assistant.message",
        payload: expect.objectContaining({
          content: "reply",
          openclaw_timeline: expect.objectContaining({
            protocol_version: "openclaw.timeline.v2",
            operation: "replace",
            final: true
          }),
          openclaw_ledger: expect.objectContaining({
            protocol_version: "openclaw.ledger.v1",
            seq: 4,
            event_type: "part.replace",
            part_type: "answer",
            text: "reply",
            payload: expect.objectContaining({
              source_kind: "assistant.message",
              chat_id: "chat-a",
              req_id: "req-a"
            })
          })
        })
      }),
      expect.objectContaining({
        id: "tool-result-1",
        seq: 5,
        kind: "tool.result",
        payload: expect.objectContaining({
          data: expect.objectContaining({
            result: expect.objectContaining({ content: "sent" })
          })
        })
      }),
      expect.objectContaining({
        id: "tool-result-1:completed",
        seq: 6,
        kind: "run.completed",
        payload: expect.objectContaining({
          openclaw_ledger: expect.objectContaining({
            protocol_version: "openclaw.ledger.v1",
            seq: 6,
            event_type: "turn.completed",
            terminal_status: "completed",
            payload: expect.objectContaining({
              source_kind: "run.completed",
              req_id: "req-a"
            })
          })
        })
      })
    ]);
  });

  it("maps incomplete WorkBuddy assistant records to canonical run.failed ledger events", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-failed-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      join(projectDir, "session-failed.jsonl"),
      [
        JSON.stringify({
          type: "message",
          role: "user",
          timestamp: "2026-06-12T08:15:41.000Z",
          content: "need answer"
        }),
        JSON.stringify({
          id: "assistant-429",
          type: "message",
          role: "assistant",
          status: "incomplete",
          timestamp: "2026-06-12T08:15:42.000Z",
          content: "model request failed",
          providerData: {
            error: {
              code: "rate_limit_exceeded",
              message: "429 Too Many Requests"
            }
          }
        })
      ].join("\n") + "\n"
    );

    const snapshot = await loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") });

    expect(snapshot.eventsBySessionId.get("session-failed")).toEqual([
      expect.objectContaining({
        id: "assistant-429:failed",
        seq: 3,
        kind: "run.failed",
        payload: expect.objectContaining({
          content: "model request failed",
          error: expect.objectContaining({ code: "rate_limit_exceeded" }),
          openclaw_ledger: expect.objectContaining({
            protocol_version: "openclaw.ledger.v1",
            seq: 3,
            event_type: "turn.failed",
            terminal_status: "failed",
            text: "model request failed",
            payload: expect.objectContaining({
              source_kind: "run.failed",
              error: expect.objectContaining({
                message: "429 Too Many Requests"
              })
            })
          })
        })
      })
    ]);
  });

  it("extracts the real user query from WorkBuddy system-reminder envelopes", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-query-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      join(projectDir, "session-c.jsonl"),
      JSON.stringify({
        type: "message",
        role: "user",
        timestamp: "2026-06-01T03:00:00.000Z",
        content: [
          {
            type: "input_text",
            text: [
              "<system-reminder data-role=\"user-context\">",
              "<user_info>noise</user_info>",
              "</system-reminder>",
              "<user_query>测试 &amp; 验证</user_query>"
            ].join("\n")
          }
        ]
      }) + "\n"
    );

    const snapshot = await loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") });

    expect(snapshot.messagesBySessionId.get("session-c")).toEqual([
      expect.objectContaining({
        role: "user",
        content: "测试 & 验证"
      })
    ]);
    expect(snapshot.sessions[0]).toMatchObject({
      id: "session-c",
      title: "测试 & 验证"
    });
  });

  it("extracts the real user text from 53AIHub channel envelopes", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-channel-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      join(projectDir, "session-d.jsonl"),
      JSON.stringify({
        type: "message",
        role: "user",
        timestamp: "2026-06-01T04:00:00.000Z",
        content: [
          {
            type: "input_text",
            text: [
              "<channel source=\"53aihub-channel\" chat_id=\"chat-a\" req_id=\"req-a\">",
              "请只回复两个字：收到",
              "",
              "<reply_instruction>",
              "internal instruction",
              "</reply_instruction>",
              "</channel>"
            ].join("\n")
          }
        ]
      }) + "\n"
    );

    const snapshot = await loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") });

    expect(snapshot.messagesBySessionId.get("session-d")).toEqual([
      expect.objectContaining({
        role: "user",
        content: "请只回复两个字：收到"
      })
    ]);
    expect(snapshot.sessions[0]).toMatchObject({
      id: "session-d",
      title: "请只回复两个字：收到"
    });
  });

  it("sanitizes only the matching 53AIHub channel record in WorkBuddy JSONL", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-sanitize-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    const jsonlPath = join(projectDir, "session-clean.jsonl");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      jsonlPath,
      [
        JSON.stringify({
          id: "keep",
          type: "message",
          role: "user",
          content: [
            {
              type: "input_text",
              text: [
                "<channel source=\"53aihub-channel\" chat_id=\"chat-a\" req_id=\"old-req\">",
                "旧消息",
                "<reply_instruction>hidden</reply_instruction>",
                "</channel>"
              ].join("\n")
            }
          ],
          providerData: { agent: "cli" }
        }),
        JSON.stringify({
          id: "clean",
          type: "message",
          role: "user",
          content: [
            {
              type: "input_text",
              text: [
                "<channel source=\"53aihub-channel\" chat_id=\"chat-a\" req_id=\"req-a\" user_name=\"Y65NG\">",
                "请只回复两个字：收到",
                "",
                "<reply_instruction>internal instruction</reply_instruction>",
                "</channel>"
              ].join("\n")
            }
          ],
          providerData: { agent: "cli" }
        })
      ].join("\n") + "\n"
    );

    const result = await sanitizeWorkBuddyChannelHistory({
      workbuddyHome,
      sessionId: "session-clean",
      chatId: "chat-a",
      reqId: "req-a"
    });
    const records = (await readFile(jsonlPath, "utf8")).trim().split(/\r?\n/).map((line) => JSON.parse(line));

    expect(result).toMatchObject({
      updated: true,
      replacements: 1,
      scannedFiles: 1
    });
    expect(records[0].content[0].text).toContain("<channel");
    expect(records[1].content[0].text).toBe("请只回复两个字：收到");
    expect(records[1].providerData.hub53aiChannel).toMatchObject({
      chat_id: "chat-a",
      req_id: "req-a",
      user_name: "Y65NG"
    });
  });

  it("cleans channel envelopes from WorkBuddy generated titles", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-title-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      join(projectDir, "session-title.jsonl"),
      [
        JSON.stringify({
          type: "message",
          role: "user",
          timestamp: "2026-06-01T04:00:00.000Z",
          content: "fallback"
        }),
        JSON.stringify({
          type: "ai-title",
          timestamp: "2026-06-01T04:00:10.000Z",
          title: [
            "53AIHub：<channel source=\"53aihub-channel\" chat_id=\"chat-a\">",
            "真正标题",
            "<reply_instruction>hidden</reply_instruction>",
            "</channel>"
          ].join("\n")
        })
      ].join("\n") + "\n"
    );

    const snapshot = await loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") });

    expect(snapshot.sessions[0]).toMatchObject({
      id: "session-title",
      title: "53AIHub：真正标题"
    });
  });

  it("maps WorkBuddy question requests to interrupted timeline events with options", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-history-question-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const projectDir = join(workbuddyHome, "projects", "project");
    await mkdir(projectDir, { recursive: true });
    await writeFile(
      join(projectDir, "session-question.jsonl"),
      [
        JSON.stringify({
          type: "message",
          role: "user",
          timestamp: "2026-06-01T05:00:00.000Z",
          content: "trigger option"
        }),
        JSON.stringify({
          id: "question-1",
          type: "question_request",
          method: "_codebuddy.ai/question",
          timestamp: "2026-06-01T05:00:10.000Z",
          params: {
            requestId: "q-1",
            question: "请选择下一步",
            options: [
              { id: "a", label: "继续执行", value: "continue" },
              { id: "b", label: "停止", value: "stop" }
            ]
          }
        })
      ].join("\n") + "\n"
    );

    const snapshot = await loadWorkBuddyHistory({ workbuddyHome, sqliteCommand: join(tempRoot, "missing-sqlite") });

    expect(snapshot.eventsBySessionId.get("session-question")).toEqual([
      expect.objectContaining({
        id: "question-1",
        kind: "run.interrupted",
        payload: expect.objectContaining({
          requiresUserInput: true,
          interaction: expect.objectContaining({
            id: "q-1",
            question: "请选择下一步",
            options: [
              expect.objectContaining({ id: "a", label: "继续执行", value: "continue" }),
              expect.objectContaining({ id: "b", label: "停止", value: "stop" })
            ]
          })
        })
      })
    ]);
  });
});
