import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { readdir, readFile, stat, writeFile } from "node:fs/promises";
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

export type WorkBuddyHistoryEvent = {
  id: string;
  sessionId: string;
  seq: number;
  kind:
    | "assistant.message"
    | "assistant.thinking"
    | "tool.call"
    | "tool.result"
    | "run.completed"
    | "run.failed"
    | "run.interrupted";
  payload: Record<string, unknown>;
  createdAt: string;
};

export type WorkBuddyReplyEventInput = {
  sessionId: string;
  eventId: string;
  text?: string;
  createdAt: string;
  chatId?: string;
  reqId?: string;
  callId?: string;
  error?: Record<string, unknown>;
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
  model?: string;
  expertId?: string;
  expertMarketplace?: string;
  permissionMode?: string;
  source?: "jsonl" | "sqlite";
};

export type WorkBuddyHistorySnapshot = {
  sessions: WorkBuddyHistorySession[];
  messagesBySessionId: Map<string, WorkBuddyHistoryMessage[]>;
  eventsBySessionId: Map<string, WorkBuddyHistoryEvent[]>;
};

export type LoadWorkBuddyHistoryInput = {
  workbuddyHome?: string;
  sqliteCommand?: string;
};

export type SanitizeWorkBuddyChannelHistoryInput = {
  workbuddyHome?: string;
  sessionId: string;
  chatId?: string;
  reqId?: string;
};

export type SanitizeWorkBuddyChannelHistoryResult = {
  updated: boolean;
  replacements: number;
  scannedFiles: number;
  filePaths: string[];
  errors: string[];
};

type MutableSession = WorkBuddyHistorySession & {
  messages: WorkBuddyHistoryMessage[];
  events: WorkBuddyHistoryEvent[];
};

type WorkBuddyReplyCall = {
  text: string;
  chatId?: string;
  reqId?: string;
  callId: string;
};

type SqliteSessionRecord = {
  id?: unknown;
  cwd?: unknown;
  title?: unknown;
  status?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
  last_activity_at?: unknown;
  model?: unknown;
  expert_id?: unknown;
  expert_marketplace?: unknown;
  permission_mode?: unknown;
};

const HUB53AI_REPLY_TOOL_NAME = "mcp__53aihub-channel__reply";

export async function loadWorkBuddyHistory(input: LoadWorkBuddyHistoryInput = {}): Promise<WorkBuddyHistorySnapshot> {
  const workbuddyHome = input.workbuddyHome || join(homedir(), ".workbuddy");
  const sessions = new Map<string, MutableSession>();

  await loadJsonlSessions(workbuddyHome, sessions);
  await enrichSqliteSessions(workbuddyHome, sessions, input.sqliteCommand);

  const sortedSessions = [...sessions.values()]
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
    .map(({ messages: _messages, events: _events, ...session }) => session);
  const messagesBySessionId = new Map<string, WorkBuddyHistoryMessage[]>();
  const eventsBySessionId = new Map<string, WorkBuddyHistoryEvent[]>();
  for (const session of sessions.values()) {
    messagesBySessionId.set(session.id, session.messages);
    eventsBySessionId.set(session.id, session.events);
  }

  return {
    sessions: sortedSessions,
    messagesBySessionId,
    eventsBySessionId
  };
}

export async function sanitizeWorkBuddyChannelHistory(
  input: SanitizeWorkBuddyChannelHistoryInput
): Promise<SanitizeWorkBuddyChannelHistoryResult> {
  const workbuddyHome = input.workbuddyHome || join(homedir(), ".workbuddy");
  const projectsDir = join(workbuddyHome, "projects");
  const result: SanitizeWorkBuddyChannelHistoryResult = {
    updated: false,
    replacements: 0,
    scannedFiles: 0,
    filePaths: [],
    errors: []
  };
  if (!input.sessionId.trim() || !existsSync(projectsDir)) {
    return result;
  }

  const files = (await findJsonlFiles(projectsDir)).filter((filePath) => basename(filePath, ".jsonl") === input.sessionId);
  for (const filePath of files) {
    result.scannedFiles += 1;
    let raw = "";
    try {
      raw = await readFile(filePath, "utf8");
    } catch (error) {
      result.errors.push(error instanceof Error ? error.message : String(error));
      continue;
    }

    const hadTrailingNewline = raw.endsWith("\n");
    const lines = raw.split(/\r?\n/);
    let fileReplacements = 0;
    const nextLines = lines.map((line) => {
      if (!line.trim()) {
        return line;
      }
      const record = parseJsonLine(line);
      if (!record || readString(record, "type") !== "message" || readString(record, "role") !== "user") {
        return line;
      }
      const sanitized = sanitizeChannelRecord(record, input);
      if (!sanitized.replaced) {
        return line;
      }
      fileReplacements += sanitized.replaced;
      return JSON.stringify(sanitized.record);
    });

    if (!fileReplacements) {
      continue;
    }

    try {
      await writeFile(filePath, normalizeJsonlOutput(nextLines, hadTrailingNewline), "utf8");
      result.updated = true;
      result.replacements += fileReplacements;
      result.filePaths.push(filePath);
    } catch (error) {
      result.errors.push(error instanceof Error ? error.message : String(error));
    }
  }

  return result;
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
  const replyCallsByCallId = new Map<string, WorkBuddyReplyCall>();
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
      const title = normalizeWorkBuddySessionTitle(extractText(record.title ?? record.message ?? record.content));
      if (title) {
        session.title = title;
      }
      session.updatedAt = maxIso(session.updatedAt, timestamp);
      return;
    }

    const interruptionEvent = buildInterruptionEvent(record, sessionId, index, timestamp);
    if (interruptionEvent) {
      appendHistoryEvent(session, interruptionEvent);
      return;
    }

    if (type === "reasoning") {
      const content = extractText(record.rawContent ?? record.raw_content ?? record.content ?? record.text);
      if (content) {
        appendHistoryEvent(session, {
          id: readString(record, "id") || `${sessionId}-reasoning-${index + 1}`,
          kind: "assistant.thinking",
          createdAt: timestamp,
          payload: {
            content,
            reasoning: content,
            reasoning_content: content,
            summary: content
          }
        });
      } else {
        session.updatedAt = maxIso(session.updatedAt, timestamp);
      }
      return;
    }

    if (type === "function_call") {
      const event = buildToolCallEvent(record, sessionId, index, timestamp);
      if (event) {
        appendHistoryEvent(session, event);
        const replyCall = extractHub53AIReplyCall(record, sessionId, index);
        if (replyCall) {
          replyCallsByCallId.set(replyCall.callId, replyCall);
          appendHistoryEvent(session, buildWorkBuddyReplyAnswerEvent({
            sessionId,
            eventId: `${event.id}:answer`,
            text: replyCall.text,
            createdAt: timestamp,
            chatId: replyCall.chatId,
            reqId: replyCall.reqId,
            callId: replyCall.callId
          }));
        }
      } else {
        session.updatedAt = maxIso(session.updatedAt, timestamp);
      }
      return;
    }

    if (type === "function_call_result") {
      const event = buildToolResultEvent(record, sessionId, index, timestamp);
      if (event) {
        appendHistoryEvent(session, event);
        const callId = readToolCallId(record, sessionId, index);
        const replyCall = replyCallsByCallId.get(callId);
        if (replyCall) {
          const status = readString(record, "status");
          const output = record.output;
          const outputText = extractText(output) || stringifyValue(output);
          if (status && status !== "completed") {
            appendHistoryEvent(session, buildWorkBuddyRunFailedEvent({
              sessionId,
              eventId: `${event.id}:failed`,
              text: outputText || `WorkBuddy reply tool failed: ${status}`,
              createdAt: timestamp,
              chatId: replyCall.chatId,
              reqId: replyCall.reqId,
              callId: replyCall.callId,
              error: {
                code: status,
                message: outputText || `WorkBuddy reply tool failed: ${status}`
              }
            }));
          } else {
            appendHistoryEvent(session, buildWorkBuddyRunCompletedEvent({
              sessionId,
              eventId: `${event.id}:completed`,
              createdAt: timestamp,
              chatId: replyCall.chatId,
              reqId: replyCall.reqId,
              callId: replyCall.callId
            }));
          }
        }
      } else {
        session.updatedAt = maxIso(session.updatedAt, timestamp);
      }
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
    if (role === "assistant" && readString(record, "status") === "incomplete") {
      const providerData = toRecord(record.providerData);
      const error = toRecord(providerData.error);
      appendHistoryEvent(session, buildWorkBuddyRunFailedEvent({
        sessionId,
        eventId: `${readString(record, "id") || `${sessionId}-${index + 1}`}:failed`,
        text: content,
        createdAt: timestamp,
        error: Object.keys(error).length ? error : undefined
      }));
    }
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
      "select id,cwd,title,status,created_at,updated_at,last_activity_at,model,expert_id,expert_marketplace,permission_mode from sessions"
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
    const title = normalizeWorkBuddySessionTitle(typeof row.title === "string" && row.title.trim() ? row.title.trim() : "");
    if (title) {
      session.title = title;
    }
    const cwd = typeof row.cwd === "string" && row.cwd.trim() ? row.cwd.trim() : "";
    if (cwd) {
      session.cwd = cwd;
    }
    const model = readSqliteString(row.model);
    const expertId = readSqliteString(row.expert_id);
    const expertMarketplace = readSqliteString(row.expert_marketplace);
    const permissionMode = readSqliteString(row.permission_mode);
    if (model) {
      session.model = model;
    }
    if (expertId) {
      session.expertId = expertId;
    }
    if (expertMarketplace) {
      session.expertMarketplace = expertMarketplace;
    }
    if (permissionMode) {
      session.permissionMode = permissionMode;
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
    messages: [],
    events: []
  };
  sessions.set(id, session);
  return session;
}

function appendHistoryEvent(
  session: MutableSession,
  event: Omit<WorkBuddyHistoryEvent, "sessionId" | "seq">
): WorkBuddyHistoryEvent {
  const next = finalizeWorkBuddyHistoryEvent(session.id, session.lastEventSeq + 1, event);
  session.lastEventSeq = next.seq;
  session.events.push(next);
  session.updatedAt = maxIso(session.updatedAt, next.createdAt);
  return next;
}

export function finalizeWorkBuddyHistoryEvent(
  sessionId: string,
  seq: number,
  event: Omit<WorkBuddyHistoryEvent, "sessionId" | "seq">
): WorkBuddyHistoryEvent {
  const next: WorkBuddyHistoryEvent = {
    ...event,
    sessionId,
    seq
  };
  return {
    ...next,
    payload: finalizeCanonicalPayload(next)
  };
}

export function buildWorkBuddyReplyAnswerEvent(
  input: WorkBuddyReplyEventInput
): Omit<WorkBuddyHistoryEvent, "sessionId" | "seq"> {
  return {
    id: input.eventId,
    kind: "assistant.message",
    createdAt: input.createdAt,
    payload: buildCanonicalPayload({
      ...input,
      sourceKind: "assistant.message",
      eventType: "part.replace",
      operation: "replace",
      partType: "answer",
      segmentType: "answer",
      segmentIndex: 1,
      terminalStatus: undefined
    })
  };
}

export function buildWorkBuddyRunCompletedEvent(
  input: WorkBuddyReplyEventInput
): Omit<WorkBuddyHistoryEvent, "sessionId" | "seq"> {
  return {
    id: input.eventId,
    kind: "run.completed",
    createdAt: input.createdAt,
    payload: buildCanonicalPayload({
      ...input,
      text: input.text || "",
      sourceKind: "run.completed",
      eventType: "turn.completed",
      operation: "close",
      partType: "status",
      segmentType: "run",
      segmentIndex: 2,
      terminalStatus: "completed"
    })
  };
}

export function buildWorkBuddyRunFailedEvent(
  input: WorkBuddyReplyEventInput
): Omit<WorkBuddyHistoryEvent, "sessionId" | "seq"> {
  const text = input.text || readString(input.error ?? {}, "message") || "WorkBuddy run failed";
  return {
    id: input.eventId,
    kind: "run.failed",
    createdAt: input.createdAt,
    payload: buildCanonicalPayload({
      ...input,
      text,
      sourceKind: "run.failed",
      eventType: "turn.failed",
      operation: "close",
      partType: "status",
      segmentType: "run",
      segmentIndex: 2,
      terminalStatus: "failed",
      error: input.error
    })
  };
}

type CanonicalPayloadInput = WorkBuddyReplyEventInput & {
  sourceKind: "assistant.message" | "run.completed" | "run.failed";
  eventType: "part.replace" | "turn.completed" | "turn.failed";
  operation: "replace" | "close";
  partType: "answer" | "status";
  segmentType: "answer" | "run";
  segmentIndex: number;
  terminalStatus?: "completed" | "failed";
};

function buildCanonicalPayload(input: CanonicalPayloadInput): Record<string, unknown> {
  const requestId = input.reqId || input.callId || input.eventId;
  const turnId = `${input.sessionId}:turn:${sanitizeIdentityPart(requestId)}`;
  const partId = input.partType === "answer"
    ? `${turnId}:answer:0`
    : `${turnId}:status`;
  const text = input.text || "";
  const ledgerPayload: Record<string, unknown> = {
    source_kind: input.sourceKind,
    chat_id: input.chatId,
    req_id: input.reqId,
    call_id: input.callId
  };
  if (input.error && Object.keys(input.error).length) {
    ledgerPayload.error = input.error;
  }

  return {
    content: text,
    summary: text,
    source_kind: input.sourceKind,
    chat_id: input.chatId,
    req_id: input.reqId,
    call_id: input.callId,
    error: input.error && Object.keys(input.error).length ? input.error : undefined,
    openclaw_timeline: {
      protocol_version: "openclaw.timeline.v2",
      turn_id: turnId,
      segment_id: partId,
      segment_type: input.segmentType,
      segment_index: input.segmentIndex,
      delta_index: 0,
      operation: input.operation,
      visibility: "final",
      final: true
    },
    openclaw_ledger: {
      protocol_version: "openclaw.ledger.v1",
      seq: 0,
      session_id: input.sessionId,
      conversation_id: input.sessionId,
      turn_id: turnId,
      active_request_id: requestId,
      part_id: partId,
      part_type: input.partType,
      event_type: input.eventType,
      operation: input.operation,
      visibility: "final",
      text,
      payload: ledgerPayload,
      terminal_status: input.terminalStatus,
      created_at: input.createdAt,
      raw_event_ref: `${input.sessionId}:pending:${input.eventId}`
    }
  };
}

function finalizeCanonicalPayload(event: WorkBuddyHistoryEvent): Record<string, unknown> {
  const ledger = toRecord(event.payload.openclaw_ledger);
  if (readString(ledger, "protocol_version") !== "openclaw.ledger.v1") {
    return event.payload;
  }
  return {
    ...event.payload,
    openclaw_ledger: {
      ...ledger,
      seq: event.seq,
      session_id: event.sessionId,
      conversation_id: readString(ledger, "conversation_id") || event.sessionId,
      raw_event_ref: `${event.sessionId}:${event.seq}:${event.id}`
    }
  };
}

function extractHub53AIReplyCall(
  record: Record<string, unknown>,
  sessionId: string,
  index: number
): WorkBuddyReplyCall | undefined {
  const args = parseJsonObject(record.arguments);
  const requestedTool = readString(args, "toolName") || readString(record, "name");
  if (requestedTool !== HUB53AI_REPLY_TOOL_NAME) {
    return undefined;
  }
  const params = toRecord(args.params);
  const text = readString(params, "text");
  if (!text) {
    return undefined;
  }
  return {
    text,
    chatId: readString(params, "chat_id") || readString(params, "chatId"),
    reqId: readString(params, "req_id") || readString(params, "reqId"),
    callId: readToolCallId(record, sessionId, index)
  };
}

function readToolCallId(record: Record<string, unknown>, sessionId: string, index: number): string {
  return readString(record, "callId") || readString(record, "toolCallId") || `${sessionId}-tool-${index + 1}`;
}

function sanitizeIdentityPart(value: string): string {
  return value.replace(/[^a-zA-Z0-9._:-]+/g, "_").slice(0, 128) || "unknown";
}

function buildToolCallEvent(
  record: Record<string, unknown>,
  sessionId: string,
  index: number,
  timestamp: string
): Omit<WorkBuddyHistoryEvent, "sessionId" | "seq"> | undefined {
  const args = parseJsonObject(record.arguments);
  const requestedTool = readString(args, "toolName") || readString(record, "name");
  if (!requestedTool) {
    return undefined;
  }
  const params = toRecord(args.params);
  const input = Object.keys(params).length ? params : args;
  const callId = readString(record, "callId") || readString(record, "toolCallId") || `${sessionId}-tool-${index + 1}`;
  return {
    id: readString(record, "id") || `${callId}:call`,
    kind: "tool.call",
    createdAt: timestamp,
    payload: {
      summary: `Used ${requestedTool}`,
      data: {
        name: requestedTool,
        toolName: requestedTool,
        callId,
        args: input,
        arguments: input,
        meta: readString(toRecord(record.providerData), "reasoning")
      }
    }
  };
}

function buildToolResultEvent(
  record: Record<string, unknown>,
  sessionId: string,
  index: number,
  timestamp: string
): Omit<WorkBuddyHistoryEvent, "sessionId" | "seq"> | undefined {
  const name = readString(record, "name") || "tool";
  const callId = readString(record, "callId") || readString(record, "toolCallId") || `${sessionId}-tool-${index + 1}`;
  const output = record.output;
  const outputText = extractText(output) || stringifyValue(output);
  const status = readString(record, "status");
  return {
    id: readString(record, "id") || `${callId}:result`,
    kind: "tool.result",
    createdAt: timestamp,
    payload: {
      summary: outputText || `Tool output: ${name}`,
      data: {
        name,
        toolName: name,
        callId,
        result: {
          content: outputText,
          output: outputText,
          raw: output,
          isError: status !== "" && status !== "completed"
        }
      }
    }
  };
}

function buildInterruptionEvent(
  record: Record<string, unknown>,
  sessionId: string,
  index: number,
  timestamp: string
): Omit<WorkBuddyHistoryEvent, "sessionId" | "seq"> | undefined {
  const type = readString(record, "type");
  const method = readString(record, "method") ||
    readString(toRecord(record.message), "method") ||
    readString(toRecord(record.data), "method") ||
    readString(toRecord(record.payload), "method");
  const normalizedMethod = method.toLowerCase();
  const normalizedType = type.toLowerCase();
  const isQuestion =
    normalizedMethod === "_codebuddy.ai/question" ||
    normalizedMethod === "session/request_permission" ||
    normalizedType === "question" ||
    normalizedType === "question_request" ||
    normalizedType === "permission_request" ||
    normalizedType === "request_permission";
  if (!isQuestion) {
    return undefined;
  }

  const data = toRecord(record.data);
  const message = toRecord(record.message);
  const params = firstRecord(record.params, data.params, record.payload, message.params, record.data, record);
  const requestId =
    readString(params, "requestId") ||
    readString(params, "request_id") ||
    readString(params, "questionId") ||
    readString(params, "question_id") ||
    readString(params, "permissionId") ||
    readString(params, "permission_id") ||
    readString(record, "id") ||
    `${sessionId}-question-${index + 1}`;
  const toolCallId = readString(params, "toolCallId") || readString(params, "tool_call_id") || readString(params, "callId");
  const question =
    extractText(params.question) ||
    extractText(params.prompt) ||
    extractText(params.message) ||
    extractText(params.content) ||
    (normalizedMethod === "session/request_permission" ? "WorkBuddy 请求权限确认" : "WorkBuddy 等待用户选择");
  const options = normalizeQuestionOptions(
    params.options ?? params.choices ?? params.answers ?? params.actions,
    normalizedMethod === "session/request_permission"
  );
  const interactionType = normalizedMethod === "session/request_permission" ||
    normalizedType.includes("permission")
    ? "permission"
    : "question";

  return {
    id: readString(record, "id") || `${requestId}:interrupted`,
    kind: "run.interrupted",
    createdAt: timestamp,
    payload: {
      reason: "workbuddy.input_required",
      summary: question,
      message: question,
      requiresUserInput: true,
      interaction: {
        id: requestId,
        requestId,
        type: interactionType,
        method: method || type,
        question,
        options,
        toolCallId
      },
      questions: [
        {
          id: requestId,
          requestId,
          type: interactionType,
          method: method || type,
          question,
          options,
          toolCallId
        }
      ]
    }
  };
}

function normalizeQuestionOptions(value: unknown, permissionFallback: boolean): Array<Record<string, unknown>> {
  const rawOptions = Array.isArray(value) ? value : [];
  const options = rawOptions
    .map((option, index) => {
      if (typeof option === "string" && option.trim()) {
        return {
          id: option.trim(),
          label: option.trim(),
          value: option.trim()
        };
      }
      const record = toRecord(option);
      const label =
        readString(record, "label") ||
        readString(record, "title") ||
        readString(record, "name") ||
        readString(record, "value") ||
        readString(record, "id") ||
        `选项 ${index + 1}`;
      return {
        ...record,
        id: readString(record, "id") || readString(record, "value") || label,
        label,
        value: record.value ?? (readString(record, "id") || label),
        description: readString(record, "description") || undefined
      };
    })
    .filter((option) => readString(option, "label"));

  if (options.length || !permissionFallback) {
    return options;
  }
  return [
    { id: "allow", label: "允许", value: "allow" },
    { id: "deny", label: "拒绝", value: "deny" }
  ];
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
  const channelMatch = trimmed.match(/<channel\b[^>]*>([\s\S]*?)(?:<\/channel>|$)/);
  if (channelMatch) {
    const inner = channelMatch[1]
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

export function normalizeWorkBuddySessionTitle(title: string): string {
  const trimmed = title.trim();
  if (!trimmed) {
    return "";
  }
  const prefixMatch = trimmed.match(/^(53AIHub|53AI Hub)\s*[：:]\s*/i);
  const body = prefixMatch ? trimmed.slice(prefixMatch[0].length).trim() : trimmed;
  const normalized = normalizeWorkBuddyUserMessage(body).replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  if (prefixMatch && normalized !== body) {
    return `53AIHub：${normalized.slice(0, 48)}`;
  }
  return normalized;
}

function sanitizeChannelRecord(
  record: Record<string, unknown>,
  input: SanitizeWorkBuddyChannelHistoryInput
): { record: Record<string, unknown>; replaced: number } {
  const content = record.content;
  let replaced = 0;
  let channelMeta: Record<string, string> | undefined;
  const replaceText = (value: string) => {
    const parsed = parse53AIHubChannelEnvelope(value);
    if (!parsed || !matchesChannelTarget(parsed.meta, input)) {
      return value;
    }
    replaced += 1;
    channelMeta = parsed.meta;
    return parsed.text;
  };

  if (typeof content === "string") {
    const text = replaceText(content);
    if (text !== content) {
      record = { ...record, content: text };
    }
  } else if (Array.isArray(content)) {
    const nextContent = content.map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) {
        return item;
      }
      const entry = item as Record<string, unknown>;
      if (typeof entry.text !== "string") {
        return item;
      }
      const text = replaceText(entry.text);
      return text === entry.text ? item : { ...entry, text };
    });
    if (replaced > 0) {
      record = { ...record, content: nextContent };
    }
  }

  if (replaced > 0 && channelMeta) {
    const providerData = toRecord(record.providerData);
    record.providerData = {
      ...providerData,
      hub53aiChannel: {
        ...toRecord(providerData.hub53aiChannel),
        ...channelMeta,
        sanitizedAt: new Date().toISOString()
      }
    };
  }

  return { record, replaced };
}

function parse53AIHubChannelEnvelope(value: string): { text: string; meta: Record<string, string> } | undefined {
  const trimmed = value.trim();
  const match = trimmed.match(/<channel\b([^>]*)>([\s\S]*?)(?:<\/channel>|$)/);
  if (!match) {
    return undefined;
  }
  const meta = parseChannelAttributes(match[1] || "");
  const source = meta.source || "";
  if (!/53aihub/i.test(source) && !/53aihub/i.test(trimmed)) {
    return undefined;
  }
  return {
    text: normalizeWorkBuddyUserMessage(trimmed),
    meta
  };
}

function parseChannelAttributes(value: string): Record<string, string> {
  const meta: Record<string, string> = {};
  const pattern = /([A-Za-z0-9_]+)="([^"]*)"/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(value))) {
    meta[match[1]] = decodeXmlEntities(match[2]);
  }
  return meta;
}

function matchesChannelTarget(meta: Record<string, string>, input: SanitizeWorkBuddyChannelHistoryInput): boolean {
  if (input.reqId?.trim() && meta.req_id !== input.reqId.trim()) {
    return false;
  }
  if (input.chatId?.trim() && meta.chat_id !== input.chatId.trim()) {
    return false;
  }
  return true;
}

function normalizeJsonlOutput(lines: string[], hadTrailingNewline: boolean): string {
  const body = hadTrailingNewline && lines[lines.length - 1] === "" ? lines.slice(0, -1).join("\n") : lines.join("\n");
  return hadTrailingNewline ? `${body}\n` : body;
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

function readSqliteString(value: unknown): string {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function firstRecord(...values: unknown[]): Record<string, unknown> {
  for (const value of values) {
    const record = toRecord(value);
    if (Object.keys(record).length) {
      return record;
    }
  }
  return {};
}

function parseJsonObject(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  if (typeof value !== "string" || !value.trim()) {
    return {};
  }
  try {
    const parsed = JSON.parse(value);
    return toRecord(parsed);
  } catch {
    return {};
  }
}

function stringifyValue(value: unknown): string {
  if (value === undefined || value === null) {
    return "";
  }
  if (typeof value === "string") {
    return value.trim();
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
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
