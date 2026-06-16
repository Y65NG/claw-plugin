import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import type { CodexWorkspaceMapping } from "./codex-workspace";

export type CodexPersistedSessionStatus = "idle" | "running" | "completed" | "stopped" | "failed" | "interrupted";

export type CodexPersistedSessionMessage = {
  id: string;
  sessionId: string;
  role: "user" | "assistant";
  content: string;
  createdAt: string;
  metadata?: Record<string, unknown>;
};

export type CodexPersistedSessionEvent = {
  id: string;
  sessionId: string;
  seq: number;
  kind: string;
  payload: Record<string, unknown>;
  createdAt: string;
};

export type CodexPersistedSession = {
  id: string;
  title: string;
  status: CodexPersistedSessionStatus;
  createdAt: string;
  updatedAt: string;
  lastEventSeq: number;
  messages: CodexPersistedSessionMessage[];
  events: CodexPersistedSessionEvent[];
  threadId?: string;
};

export type CodexSessionStateFile = {
  version: 1;
  session: CodexPersistedSession;
};

export const CODEX_SESSION_STATE_FILE = ".53aihub-codex-session.json";

export async function readCodexSessionState(
  workspace: Pick<CodexWorkspaceMapping, "conversationId" | "workspaceDir">
): Promise<CodexPersistedSession | undefined> {
  const statePath = getCodexSessionStatePath(workspace.workspaceDir);
  if (!existsSync(statePath)) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(await readFile(statePath, "utf8")) as Partial<CodexSessionStateFile>;
    return normalizePersistedSession(parsed.session, workspace.conversationId);
  } catch {
    return undefined;
  }
}

export async function writeCodexSessionState(
  workspace: Pick<CodexWorkspaceMapping, "workspaceDir">,
  session: CodexPersistedSession
): Promise<void> {
  await mkdir(resolve(workspace.workspaceDir), { recursive: true });
  await writeFile(
    getCodexSessionStatePath(workspace.workspaceDir),
    `${JSON.stringify({ version: 1, session }, null, 2)}\n`,
    { mode: 0o600 }
  );
}

export function getCodexSessionStatePath(workspaceDir: string): string {
  return join(resolve(workspaceDir), CODEX_SESSION_STATE_FILE);
}

function normalizePersistedSession(value: unknown, fallbackId: string): CodexPersistedSession | undefined {
  const record = toRecord(value);
  if (!record) {
    return undefined;
  }
  const id = readString(record.id) || fallbackId;
  const messages = Array.isArray(record.messages) ? record.messages.map(normalizeMessage).filter(Boolean) : [];
  const events = Array.isArray(record.events) ? record.events.map(normalizeEvent).filter(Boolean) : [];
  const lastEventSeq = Math.max(positiveInt(record.lastEventSeq, 0), ...events.map((event) => event.seq));
  return {
    id,
    title: readString(record.title) || `53AI Hub-${id}`,
    status: normalizeStatus(readString(record.status)),
    createdAt: readString(record.createdAt) || new Date().toISOString(),
    updatedAt: readString(record.updatedAt) || new Date().toISOString(),
    lastEventSeq,
    messages,
    events,
    ...(readString(record.threadId) ? { threadId: readString(record.threadId) } : {})
  };
}

function normalizeMessage(value: unknown): CodexPersistedSessionMessage | undefined {
  const record = toRecord(value);
  if (!record) {
    return undefined;
  }
  const role = record.role === "user" || record.role === "assistant" ? record.role : undefined;
  const id = readString(record.id);
  const sessionId = readString(record.sessionId);
  if (!role || !id || !sessionId) {
    return undefined;
  }
  return {
    id,
    sessionId,
    role,
    content: readString(record.content),
    createdAt: readString(record.createdAt) || new Date().toISOString(),
    ...(toRecord(record.metadata) ? { metadata: toRecord(record.metadata) as Record<string, unknown> } : {})
  };
}

function normalizeEvent(value: unknown): CodexPersistedSessionEvent | undefined {
  const record = toRecord(value);
  if (!record) {
    return undefined;
  }
  const id = readString(record.id);
  const sessionId = readString(record.sessionId);
  const kind = readString(record.kind);
  if (!id || !sessionId || !kind) {
    return undefined;
  }
  return {
    id,
    sessionId,
    seq: positiveInt(record.seq, 0),
    kind,
    payload: (toRecord(record.payload) as Record<string, unknown>) || {},
    createdAt: readString(record.createdAt) || new Date().toISOString()
  };
}

function normalizeStatus(value: string): CodexPersistedSessionStatus {
  if (value === "completed" || value === "stopped" || value === "failed" || value === "interrupted") {
    return value;
  }
  return value === "running" ? "interrupted" : "idle";
}

function positiveInt(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function readString(value: unknown): string {
  return typeof value === "string" && value ? value : "";
}

function toRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}
