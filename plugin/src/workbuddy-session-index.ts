import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const DEFAULT_WORKBUDDY_USER_ID = "53aihub-workbuddy";

export type SyncWorkBuddySessionIndexInput = {
  workbuddyHome: string;
  sessionId: string;
  cwd: string;
  title: string;
  status?: "idle" | "running" | "completed" | "stopped";
  now?: Date;
  sqliteCommand?: string;
  preserveTitleOnUpdate?: boolean;
};

export async function syncWorkBuddySessionIndex(input: SyncWorkBuddySessionIndexInput): Promise<void> {
  const dbPath = join(input.workbuddyHome, "workbuddy.db");
  if (!existsSync(dbPath)) {
    return;
  }

  const nowMs = (input.now ?? new Date()).getTime();
  const status = normalizeDbStatus(input.status);
  const userId = await readWorkBuddyOwnerUserId(input.workbuddyHome);
  const title = input.title.trim() || "53AIHub WorkBuddy";
  const sql = `
INSERT INTO sessions (
  id, cwd, user_id, title, custom_title, status, created_at, updated_at,
  deleted_at, is_playground, source_mode, is_background_automation, model,
  expert_id, expert_locale, expert_runtime_identity, last_activity_at,
  expert_marketplace, permission_mode, use_sandbox_cli
) VALUES (
  ${sqlString(input.sessionId)},
  ${sqlString(input.cwd)},
  ${sqlString(userId)},
  ${sqlString(title)},
  NULL,
  ${sqlString(status)},
  ${nowMs},
  ${nowMs},
  NULL,
  1,
  'working',
  0,
  'auto',
  NULL,
  NULL,
  NULL,
  ${nowMs},
  NULL,
  NULL,
  NULL
)
ON CONFLICT(id) DO UPDATE SET
  cwd = excluded.cwd,
  title = CASE
    WHEN ${input.preserveTitleOnUpdate ? 1 : 0} = 1 AND sessions.title IS NOT NULL AND trim(sessions.title) <> ''
    THEN sessions.title
    ELSE excluded.title
  END,
  status = excluded.status,
  updated_at = excluded.updated_at,
  deleted_at = NULL,
  is_playground = 1,
  source_mode = 'working',
  is_background_automation = 0,
  model = COALESCE(sessions.model, excluded.model),
  last_activity_at = excluded.last_activity_at;
`;

  await execFileAsync(input.sqliteCommand ?? "sqlite3", [dbPath, sql], { maxBuffer: 1024 * 1024 });
}

async function readWorkBuddyOwnerUserId(workbuddyHome: string): Promise<string> {
  const settingsPath = join(workbuddyHome, "settings.json");
  try {
    const raw = await readFile(settingsPath, "utf8");
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const claw = toRecord(parsed.claw);
    const legacyOwnerUid = readString(claw, "legacyOwnerUid");
    return legacyOwnerUid || DEFAULT_WORKBUDDY_USER_ID;
  } catch {
    return DEFAULT_WORKBUDDY_USER_ID;
  }
}

function normalizeDbStatus(status: SyncWorkBuddySessionIndexInput["status"]): string {
  if (status === "running") {
    return "running";
  }
  if (status === "stopped") {
    return "stopped";
  }
  if (status === "idle") {
    return "completed";
  }
  return status ?? "completed";
}

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function readString(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  return typeof value === "string" && value.trim() ? value.trim() : "";
}
