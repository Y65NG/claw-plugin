import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { loadWorkBuddyHistory } from "../src/workbuddy-history";

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
            params: { chat_id: "chat-a", text: "reply" }
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
            args: { chat_id: "chat-a", text: "reply" }
          })
        })
      }),
      expect.objectContaining({
        id: "tool-result-1",
        seq: 4,
        kind: "tool.result",
        payload: expect.objectContaining({
          data: expect.objectContaining({
            result: expect.objectContaining({ content: "sent" })
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
});
