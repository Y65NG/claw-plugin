import { execFile } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterEach, describe, expect, it } from "vitest";

import { syncWorkBuddySessionIndex } from "../src/workbuddy-session-index";

const execFileAsync = promisify(execFile);
const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("WorkBuddy session index", () => {
  it("upserts the shared 53AIHub session into workbuddy.db", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-session-index-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    await mkdir(workbuddyHome, { recursive: true });
    await writeFile(
      join(workbuddyHome, "settings.json"),
      JSON.stringify({
        claw: {
          legacyOwnerUid: "owner-uid"
        }
      })
    );
    const dbPath = join(workbuddyHome, "workbuddy.db");
    await execFileAsync("sqlite3", [
      dbPath,
      `CREATE TABLE sessions (
        id TEXT PRIMARY KEY,
        cwd TEXT NOT NULL,
        user_id TEXT NOT NULL,
        title TEXT,
        custom_title TEXT,
        status TEXT NOT NULL DEFAULT 'Pending',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        deleted_at INTEGER,
        is_playground INTEGER NOT NULL DEFAULT 0,
        source_mode TEXT,
        is_background_automation INTEGER,
        model TEXT,
        expert_id TEXT,
        expert_locale TEXT,
        expert_runtime_identity TEXT,
        last_activity_at INTEGER,
        expert_marketplace TEXT,
        permission_mode TEXT,
        use_sandbox_cli INTEGER
      );`
    ]);

    await syncWorkBuddySessionIndex({
      workbuddyHome,
      sessionId: "53aihub-workbuddy-shared",
      cwd: "/tmp/workspace",
      title: "53AIHub：hello",
      status: "running",
      now: new Date("2026-06-05T00:00:00.000Z")
    });

    const { stdout } = await execFileAsync("sqlite3", [
      "-json",
      dbPath,
      "select id,cwd,user_id,title,status,is_playground,source_mode,model,last_activity_at from sessions"
    ]);
    expect(JSON.parse(stdout)).toEqual([
      {
        id: "53aihub-workbuddy-shared",
        cwd: "/tmp/workspace",
        user_id: "owner-uid",
        title: "53AIHub：hello",
        status: "running",
        is_playground: 1,
        source_mode: "working",
        model: "auto",
        last_activity_at: 1780617600000
      }
    ]);

    await syncWorkBuddySessionIndex({
      workbuddyHome,
      sessionId: "53aihub-workbuddy-shared",
      cwd: "/tmp/workspace",
      title: "53AIHub：done",
      status: "completed",
      now: new Date("2026-06-05T00:00:01.000Z"),
      preserveTitleOnUpdate: true
    });

    const updated = await execFileAsync("sqlite3", [
      "-json",
      dbPath,
      "select title,status,last_activity_at from sessions where id = '53aihub-workbuddy-shared'"
    ]);
    expect(JSON.parse(updated.stdout)).toEqual([
      {
        title: "53AIHub：hello",
        status: "completed",
        last_activity_at: 1780617601000
      }
    ]);
  });
});
