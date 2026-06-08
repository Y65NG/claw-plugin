import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { readdir, readFile, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export type WorkBuddyHistoryMessage = {
  id: string;
  sessionId: string;
  role: "user" | "assistant";
  content: string;
  createdAt: string;
};

export type WorkBuddyHistorySession = {
  id: string;
  title: string;
  status: "idle" | "running" | "completed" | "stopped";
  hostKind: "workbuddy";
  runnerCommand: "workbuddy";
  createdAt: string;
  updatedAt: string;
  lastEventSeq: number;
  cwd?: string;
  source?: "jsonl" | "sqlite";
};

export type WorkBuddyHistorySnapshot = {
  sessions: WorkBuddyHistorySession[];
  messagesBySessionId: Map<string, WorkBuddyHistoryMessage[]>;
};

export type LoadWorkBuddyHistoryInput = {
  workbuddyHome?: string;
  sqliteCommand?: string;
};

type MutableSession = WorkBuddyHistorySession & {
  messages: WorkBuddyHistoryMessage[];
};

type SqliteSessionRecord = {
  id?: unknown;
  cwd?: unknown;
  title?: unknown;
  status?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
  last_activity_at?: unknown;
};

export async function loadWorkBuddyHistory(input: LoadWorkBuddyHistoryInput = {}): Promise<WorkBuddyHistorySnapshot> {
  const workbuddyHome = input.workbuddyHome || join(homedir(), ".workbuddy");
  const sessions = new Map<string, MutableSession>();

  await loadJsonlSessions(workbuddyHome, sessions);
  await enrichSqliteSessions(workbuddyHome, sessions, input.sqliteCommand);

  const sortedSessions = [...sessions.values()]
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
    .map(({ messages: _messages, ...session }) => session);
  const messagesBySessionId = new Map<string, WorkBuddyHistoryMessage[]>();
  for (const session of sessions.values()) {
    messagesBySessionId.set(session.id, session.messages);
  }

  return {
    sessions: sortedSessions,
    messagesBySessionId
  };
}

async function loadJsonlSessions(workbuddyHome: string, sessions: Map<string, MutableSession>) {
  const projectsDir = join(workbuddyHome, "projects");
  if (!existsSync(projectsDir)) {
    return;
  }

  const files = await findJsonlFiles(projectsDir);
  for (const filePath of files) {
    await parseJsonlSession(filePath, projectsDir, sessions);
  }
}

async function findJsonlFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  const files: string[] = [];
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...await findJsonlFiles(path));
    } else if (entry.isFile() && entry.name.endsWith(".jsonl")) {
      files.push(path);
    }
  }
  return files;
}

async function parseJsonlSession(filePath: string, projectsDir: string, sessions: Map<string, MutableSession>) {
  const sessionId = basename(filePath, ".jsonl");
  const fallbackTime = await readFileMtime(filePath);
  const session = ensureSession(sessions, sessionId, {
    createdAt: fallbackTime,
    updatedAt: fallbackTime,
    cwd: inferCwdFromProjectFile(filePath, projectsDir),
    source: "jsonl"
  });

  const raw = await readFile(filePath, "utf8").catch(() => "");
  const lines = raw.split(/\r?\n/).filter((line) => line.trim());
  let minRecordTime: string | undefined;
  let maxRecordTime: string | undefined;
  lines.forEach((line, index) => {
    const record = parseJsonLine(line);
    if (!record) {
      return;
    }

    const type = readString(record, "type");
    const timestamp = normalizeTimestamp(record.timestamp ?? record.created_at ?? record.createdAt) || fallbackTime;
    minRecordTime = minRecordTime ? minIso(minRecordTime, timestamp) : timestamp;
    maxRecordTime = maxRecordTime ? maxIso(maxRecordTime, timestamp) : timestamp;
    if (type === "ai-title") {
      const title = extractText(record.title ?? record.message ?? record.content);
      if (title) {
        session.title = title;
      }
      session.updatedAt = maxIso(session.updatedAt, timestamp);
      return;
    }

    if (type !== "message") {
      session.updatedAt = maxIso(session.updatedAt, timestamp);
      return;
    }

    const role = readRole(record.role);
    if (!role) {
      return;
    }
    const rawContent = extractText(record.content ?? record.message ?? record.text);
    const content = role === "user" ? normalizeWorkBuddyUserMessage(rawContent) : rawContent;
    if (!content) {
      return;
    }

    session.messages.push({
      id: readString(record, "id") || `${sessionId}-${index + 1}`,
      sessionId,
      role,
      content,
      createdAt: timestamp
    });
    session.lastEventSeq += 1;
    session.createdAt = minIso(session.createdAt, timestamp);
    session.updatedAt = maxIso(session.updatedAt, timestamp);
    if (!session.title && role === "user") {
      session.title = content.replace(/\s+/g, " ").slice(0, 48);
    }
  });

  if (!session.title) {
    session.title = sessionId;
  }
  if (minRecordTime && maxRecordTime) {
    session.createdAt = minRecordTime;
    session.updatedAt = maxRecordTime;
  }
  if (session.messages.length && session.status === "idle") {
    session.status = "completed";
  }
}

async function enrichSqliteSessions(
  workbuddyHome: string,
  sessions: Map<string, MutableSession>,
  sqliteCommand = "sqlite3"
) {
  const dbPath = join(workbuddyHome, "workbuddy.db");
  if (!existsSync(dbPath)) {
    return;
  }

  let rows: SqliteSessionRecord[] = [];
  try {
    const result = await execFileAsync(sqliteCommand, [
      "-json",
      dbPath,
      "select id,cwd,title,status,created_at,updated_at,last_activity_at from sessions"
    ], { maxBuffer: 1024 * 1024 * 8 });
    const parsed = JSON.parse(String(result.stdout || "[]"));
    rows = Array.isArray(parsed) ? parsed : [];
  } catch {
    return;
  }

  for (const row of rows) {
    const id = typeof row.id === "string" && row.id.trim() ? row.id.trim() : "";
    if (!id) {
      continue;
    }
    const createdAt = normalizeTimestamp(row.created_at) || new Date(0).toISOString();
    const updatedAt = normalizeTimestamp(row.last_activity_at ?? row.updated_at) || createdAt;
    const session = ensureSession(sessions, id, {
      createdAt,
      updatedAt,
      source: "sqlite"
    });
    const title = typeof row.title === "string" && row.title.trim() ? row.title.trim() : "";
    if (title) {
      session.title = title;
    }
    const cwd = typeof row.cwd === "string" && row.cwd.trim() ? row.cwd.trim() : "";
    if (cwd) {
      session.cwd = cwd;
    }
    session.status = normalizeSessionStatus(row.status) ?? session.status;
    session.createdAt = minIso(session.createdAt, createdAt);
    session.updatedAt = maxIso(session.updatedAt, updatedAt);
  }
}

function ensureSession(
  sessions: Map<string, MutableSession>,
  id: string,
  defaults: Pick<WorkBuddyHistorySession, "createdAt" | "updatedAt"> &
    Partial<Pick<WorkBuddyHistorySession, "cwd" | "source">>
): MutableSession {
  const existing = sessions.get(id);
  if (existing) {
    if (defaults.cwd && !existing.cwd) {
      existing.cwd = defaults.cwd;
    }
    existing.createdAt = minIso(existing.createdAt, defaults.createdAt);
    existing.updatedAt = maxIso(existing.updatedAt, defaults.updatedAt);
    return existing;
  }
  const session: MutableSession = {
    id,
    title: "",
    status: "idle",
    hostKind: "workbuddy",
    runnerCommand: "workbuddy",
    createdAt: defaults.createdAt,
    updatedAt: defaults.updatedAt,
    lastEventSeq: 0,
    cwd: defaults.cwd,
    source: defaults.source,
    messages: []
  };
  sessions.set(id, session);
  return session;
}

function parseJsonLine(line: string): Record<string, unknown> | undefined {
  try {
    const parsed = JSON.parse(line);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : undefined;
  } catch {
    return undefined;
  }
}

function extractText(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (Array.isArray(value)) {
    return value.map(extractText).filter(Boolean).join("\n").trim();
  }
  if (!value || typeof value !== "object") {
    return "";
  }
  const record = value as Record<string, unknown>;
  const type = readString(record, "type");
  if (type === "input_text" || type === "output_text" || type === "text") {
    return extractText(record.text);
  }
  return extractText(record.text ?? record.content ?? record.message);
}

export function normalizeWorkBuddyUserMessage(content: string): string {
  const trimmed = content.trim();
  if (trimmed.startsWith("<channel ") && trimmed.includes("</channel>")) {
    const inner = trimmed
      .replace(/^<channel\b[^>]*>\s*/, "")
      .replace(/\s*<\/channel>\s*$/, "")
      .replace(/<reply_instruction>[\s\S]*?<\/reply_instruction>/g, "")
      .trim();
    return decodeXmlEntities(inner || trimmed);
  }
  if (!trimmed.includes("<system-reminder") || !trimmed.includes("<user_query>")) {
    return trimmed;
  }
  const match = trimmed.match(new RegExp("<user_query>([\\s\\S]*?)</user_query>"));
  return decodeXmlEntities(match?.[1]?.trim() || trimmed);
}

function decodeXmlEntities(value: string): string {
  return value
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

function readString(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function readRole(value: unknown): WorkBuddyHistoryMessage["role"] | undefined {
  return value === "user" || value === "assistant" ? value : undefined;
}

function normalizeSessionStatus(value: unknown): WorkBuddyHistorySession["status"] | undefined {
  if (value === "idle" || value === "running" || value === "completed" || value === "stopped") {
    return value;
  }
  return undefined;
}

function inferCwdFromProjectFile(filePath: string, projectsDir: string): string | undefined {
  const parent = dirname(filePath);
  if (parent === projectsDir) {
    return undefined;
  }
  return basename(parent).replace(/__/g, "/");
}

async function readFileMtime(filePath: string): Promise<string> {
  try {
    const info = await stat(filePath);
    return info.mtime.toISOString();
  } catch {
    return new Date(0).toISOString();
  }
}

function normalizeTimestamp(value: unknown): string | undefined {
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value.toISOString();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return normalizeDate(value);
  }
  if (typeof value === "string" && value.trim()) {
    const numeric = Number(value);
    if (Number.isFinite(numeric) && /^\d+$/.test(value.trim())) {
      return normalizeDate(numeric);
    }
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? new Date(parsed).toISOString() : undefined;
  }
  return undefined;
}

function normalizeDate(value: number): string | undefined {
  const millis = value < 10_000_000_000 ? value * 1000 : value;
  const date = new Date(millis);
  return Number.isFinite(date.getTime()) ? date.toISOString() : undefined;
}

function minIso(left: string, right: string): string {
  return left.localeCompare(right) <= 0 ? left : right;
}

function maxIso(left: string, right: string): string {
  return left.localeCompare(right) >= 0 ? left : right;
}
