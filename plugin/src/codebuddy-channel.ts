import { createServer, createConnection, type Server as NetServer } from "node:net";
import { mkdir, rm } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { homedir } from "node:os";

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import WebSocket from "ws";

import {
  buildHub53AIRPCFrame,
  buildHub53AIOutgoingChunk,
  buildHub53AIPrompt,
  checkHub53AIAccessPolicy,
  createHub53AIAuthHeaders,
  DEFAULT_HUB53AI_THINKING_MESSAGE,
  inferHub53AIErrorCode,
  maskHub53AIBotId,
  parseHub53AIHeartbeat,
  parseHub53AIRPCRequest,
  parseIncomingMessage,
  sanitizeHub53AIWsUrl,
  validateHub53AIConfig,
  type Hub53AIBaseConfig,
  type Hub53AIIncomingMessage,
  type Hub53AIOutgoingChunk,
  type Hub53AIRPCRequest
} from "./53aihub-protocol";
import {
  loadWorkBuddyHistory,
  type WorkBuddyHistoryMessage,
  type WorkBuddyHistorySession,
  type WorkBuddyHistorySnapshot
} from "./workbuddy-history";
import { syncWorkBuddySessionIndex } from "./workbuddy-session-index";

export type Hub53AIChannelConfig = Hub53AIBaseConfig & {
  reconnectBaseMs: number;
  maxReconnectAttempts: number;
  workbuddyHome: string;
  workbuddyHistoryScope: "all" | "channel";
  workbuddySessionId: string;
};

export type Hub53AIChannelStatusSnapshot = {
  configured: boolean;
  connectionStatus: "connecting" | "connected" | "disconnected" | "error";
  botId?: string;
  wsUrl?: string;
  lastHeartbeatAt?: string;
  lastConnectedAt?: string;
  lastError?: string;
  receivedMessageCount: number;
  sentMessageCount: number;
  pendingOutboundCount: number;
  knownChatCount: number;
};

export type CodeBuddyChannelNotification = {
  content: string;
  meta: Record<string, string>;
};

export type CodeBuddyChannelBridgeInput = {
  config: Hub53AIChannelConfig;
  notifyChannel(notification: CodeBuddyChannelNotification): Promise<void>;
  historyLoader?: () => Promise<WorkBuddyHistorySnapshot>;
  logger?: {
    info?(message: string): void;
    warn?(message: string): void;
    error?(message: string): void;
  };
};

export type CodeBuddyChannelBroker = {
  role: "leader" | "follower";
  requestReply(input: { chatId: string; text: string; reqId?: string }): Promise<void>;
  requestStatus(): Promise<unknown>;
  close(): Promise<void>;
};

type ChannelMessageRecord = {
  id: string;
  sessionId: string;
  role: "user" | "assistant";
  content: string;
  createdAt: string;
};

type ChannelSessionRecord = {
  id: string;
  title: string;
  status: "idle" | "running" | "completed" | "stopped";
  hostKind: "workbuddy";
  runnerCommand: "codebuddy-channel";
  createdAt: string;
  updatedAt: string;
  lastEventSeq: number;
  messages: ChannelMessageRecord[];
};

type ChannelRPCErrorCode = "FEATURE_NOT_AVAILABLE" | "PARAM_ERROR" | "INTERNAL_ERROR";

class ChannelRPCError extends Error {
  constructor(
    readonly code: ChannelRPCErrorCode,
    message: string
  ) {
    super(message);
  }
}

const HEARTBEAT_INTERVAL_MS = 30_000;
const MAX_OUTBOX_FRAMES = 200;
const DEFAULT_RECONNECT_BASE_MS = 2_000;
const DEFAULT_MAX_RECONNECT_ATTEMPTS = 10;
const BROKER_REQUEST_TIMEOUT_MS = 2_000;
const REPLY_TOOL_INSTRUCTION =
  "处理完这条 53AIHub 消息后，必须调用 53aihub-channel 的 reply 工具，把最终回复写入 text，并使用下面 meta 中的 chat_id 与 req_id。";

export function createCodeBuddyChannelBridge(input: CodeBuddyChannelBridgeInput) {
  let socket: WebSocket | null = null;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let reconnectTimer: NodeJS.Timeout | null = null;
  let stopped = false;
  let reconnectAttempts = 0;
  let connectionStatus: Hub53AIChannelStatusSnapshot["connectionStatus"] = "disconnected";
  let lastHeartbeatAt: string | undefined;
  let lastConnectedAt: string | undefined;
  let lastError: string | undefined;
  let receivedMessageCount = 0;
  let sentMessageCount = 0;
  const latestReqByChat = new Map<string, string>();
  const latestSessionByChat = new Map<string, string>();
  const sessions = new Map<string, ChannelSessionRecord>();
  const outbox: Hub53AIOutgoingChunk[] = [];

  async function start() {
    validateHub53AIConfig(input.config);
    stopped = false;
    connect();
  }

  async function stop() {
    stopped = true;
    clearHeartbeat();
    clearReconnect();
    if (socket) {
      socket.close();
      socket = null;
    }
  }

  function getStatus(): Hub53AIChannelStatusSnapshot {
    return {
      configured: Boolean(input.config.botId && input.config.secret && input.config.wsUrl),
      connectionStatus,
      botId: maskHub53AIBotId(input.config.botId),
      wsUrl: sanitizeHub53AIWsUrl(input.config.wsUrl),
      lastHeartbeatAt,
      lastConnectedAt,
      lastError,
      receivedMessageCount,
      sentMessageCount,
      pendingOutboundCount: outbox.length,
      knownChatCount: latestReqByChat.size
    };
  }

  async function reply(inputReply: {
    chatId: string;
    text: string;
    reqId?: string;
    status?: Hub53AIOutgoingChunk["status"];
  }) {
    const chatId = inputReply.chatId.trim();
    const reqId = inputReply.reqId?.trim() || latestReqByChat.get(chatId);
    if (!chatId) {
      throw new Error("chat_id is required");
    }
    if (!inputReply.text.trim()) {
      throw new Error("text is required");
    }
    if (!reqId) {
      throw new Error(`no recent 53AIHub request is known for chat_id: ${chatId}`);
    }
    await sendReply({
      reqId,
      chatId,
      text: inputReply.text,
      status: inputReply.status ?? "done"
    });
    appendAssistantMessage(chatId, inputReply.text);
      await syncSharedWorkBuddySessionIndex(inputReply.text, "completed", { preserveTitleOnUpdate: true });
  }

  function connect() {
    if (stopped) {
      return;
    }
    connectionStatus = "connecting";
    lastError = undefined;

    socket = new WebSocket(input.config.wsUrl, {
      headers: createHub53AIAuthHeaders(input.config)
    });

    socket.on("open", () => {
      reconnectAttempts = 0;
      connectionStatus = "connected";
      lastConnectedAt = new Date().toISOString();
      input.logger?.info?.(`[53aihub-channel] connected to ${sanitizeHub53AIWsUrl(input.config.wsUrl)}`);
      sendAppPing();
      void replayOutbox();
      clearHeartbeat();
      heartbeatTimer = setInterval(() => {
        if (socket?.readyState === WebSocket.OPEN) {
          (socket as WebSocket & { ping?: () => void }).ping?.();
          sendAppPing();
        }
      }, HEARTBEAT_INTERVAL_MS);
    });

    socket.on("message", (raw) => {
      void handleRawMessage(String(raw)).catch((error) => {
        lastError = error instanceof Error ? error.message : String(error);
        input.logger?.error?.(`[53aihub-channel] failed to process message: ${lastError}`);
      });
    });

    socket.on("error", (error) => {
      lastError = error instanceof Error ? error.message : String(error);
      connectionStatus = "error";
      input.logger?.error?.(`[53aihub-channel] websocket error: ${lastError}`);
    });

    socket.on("close", (code, reason) => {
      clearHeartbeat();
      if (stopped) {
        return;
      }
      lastError = `WebSocket closed: ${code}${reason.length ? ` ${reason.toString()}` : ""}`;
      connectionStatus = "disconnected";
      scheduleReconnect();
    });
  }

  async function handleRawMessage(rawPayload: string) {
    const heartbeat = parseHub53AIHeartbeat(rawPayload);
    if (heartbeat === "ping") {
      sendRaw(JSON.stringify({ action: "pong", data: { botId: input.config.botId } }));
      return;
    }
    if (heartbeat === "pong") {
      lastHeartbeatAt = new Date().toISOString();
      return;
    }

    const rpcRequest = parseHub53AIRPCRequest(rawPayload);
    if (rpcRequest) {
      await processRPCRequest(rpcRequest);
      return;
    }

    const message = parseIncomingMessage(rawPayload);
    if (!message) {
      return;
    }
    receivedMessageCount += 1;
    latestReqByChat.set(message.chatId, message.reqId);
    upsertUserSession(message);
    await syncSharedWorkBuddySessionIndex(message.text, "running");

    const accessResult = checkHub53AIAccessPolicy(input.config, message);
    if (!accessResult.allowed) {
      await sendReply({
        reqId: message.reqId,
        chatId: message.chatId,
        text: `访问被拒绝: ${accessResult.reason}`,
        status: "error",
        error: {
          code: "ACCESS_DENIED",
          message: accessResult.reason
        }
      });
      return;
    }

    if (input.config.sendThinkingMessage) {
      await sendReply({
        reqId: message.reqId,
        chatId: message.chatId,
        text: DEFAULT_HUB53AI_THINKING_MESSAGE,
        status: "thinking"
      });
    }

    await input.notifyChannel(buildChannelNotification(message));
  }

  function buildChannelNotification(message: Hub53AIIncomingMessage): CodeBuddyChannelNotification {
    return {
      content: buildChannelContent(message),
      meta: sanitizeChannelMeta({
        source: "53aihub",
        sender: message.userName || message.userId,
        chat_id: message.chatId,
        req_id: message.reqId,
        msg_id: message.msgId,
        user_id: message.userId,
        ...(message.userName ? { user_name: message.userName } : {}),
        ...(message.conversationTitle ? { conversation_title: message.conversationTitle } : {})
      })
    };
  }

  function buildChannelContent(message: Hub53AIIncomingMessage): string {
    return [
      buildHub53AIPrompt(message),
      "",
      "<reply_instruction>",
      REPLY_TOOL_INSTRUCTION,
      "</reply_instruction>"
    ].join("\n").trim();
  }

  async function sendReply(inputReply: {
    reqId: string;
    chatId: string;
    text: string;
    status: Hub53AIOutgoingChunk["status"];
    error?: Hub53AIOutgoingChunk["data"]["error"];
  }) {
    const frame = buildHub53AIOutgoingChunk(
      inputReply.reqId,
      inputReply.text,
      inputReply.status,
      inputReply.error,
      inputReply.chatId
    );
    if (!sendRaw(JSON.stringify(frame), true)) {
      outbox.push(frame);
      outbox.splice(0, Math.max(0, outbox.length - MAX_OUTBOX_FRAMES));
    }
  }

  async function processRPCRequest(request: Hub53AIRPCRequest) {
    try {
      const data = await resolveRPCRequest(request);
      sendRaw(JSON.stringify(buildHub53AIRPCFrame(request, "done", data)));
    } catch (error) {
      const rpcError = normalizeRPCError(error);
      sendRaw(JSON.stringify(buildHub53AIRPCFrame(request, "error", rpcError)));
    }
  }

  async function resolveRPCRequest(request: Hub53AIRPCRequest): Promise<unknown> {
    if (request.action === "sessions.list") {
      const pagination = readRPCPagination(request.data, 50);
      const allSessions = await loadMergedSessionPayloads();
      const page = allSessions.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        sessions: page,
        pagination: buildPagination(pagination.limit, pagination.offset, allSessions.length, page.length)
      };
    }

    if (request.action === "sessions.current") {
      const chatId = readRPCChatId(request.data);
      const sessionId = latestSessionByChat.get(chatId);
      if (sessionId) {
        return toSessionPayload(sessions.get(sessionId));
      }
      const history = await loadHistorySnapshot();
      return toHistorySessionPayload(
        history.sessions.find((session) => session.id === input.config.workbuddySessionId)
      );
    }

    if (request.action === "sessions.messages") {
      const payload = toRecord(request.data);
      const session = await readRPCSessionMessages(payload);
      const pagination = readRPCPagination(payload, 100);
      const page = session.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        messages: page,
        events: [],
        pagination: buildPagination(pagination.limit, pagination.offset, session.length, page.length)
      };
    }

    if (request.action === "sessions.events") {
      const pagination = readRPCPagination(request.data, 100);
      return {
        events: [],
        pagination: buildPagination(pagination.limit, pagination.offset, 0, 0)
      };
    }

    if (request.action === "sessions.control") {
      const payload = toRecord(request.data);
      const session = readMutableRPCSession(payload);
      const action = readOptionalString(payload, "action");
      if (action !== "stop") {
        throw new ChannelRPCError("PARAM_ERROR", "unsupported sessions.control action");
      }
      session.status = "stopped";
      session.updatedAt = new Date().toISOString();
      return {
        ok: true,
        action,
        session_id: session.id,
        conversation_id: session.id
      };
    }

    if (request.action === "runtime.get") {
      return resolveRuntimeRPC(request.data);
    }

    if (request.action === "cron.tasks") {
      const pagination = readRPCPagination(request.data, 100);
      return {
        tasks: [],
        cronTasks: [],
        pagination: buildPagination(pagination.limit, pagination.offset, 0, 0)
      };
    }

    throw new ChannelRPCError("FEATURE_NOT_AVAILABLE", `Unsupported RPC action: ${request.action}`);
  }

  function resolveRuntimeRPC(payload: unknown): unknown {
    const include = (readOptionalString(toRecord(payload), "include") ?? "all").toLowerCase();
    const status = {
      ...getStatus(),
      healthy: connectionStatus === "connected",
      connectionHealthy: connectionStatus === "connected",
      hostKind: "workbuddy",
      runnerCommand: "codebuddy-channel"
    };
    const config = {
      hub53ai: {
        enabled: true,
        botId: maskHub53AIBotId(input.config.botId),
        wsUrl: sanitizeHub53AIWsUrl(input.config.wsUrl),
        accessPolicy: input.config.accessPolicy,
        sendThinkingMessage: input.config.sendThinkingMessage,
        workbuddyHistoryScope: input.config.workbuddyHistoryScope,
        workbuddySessionId: input.config.workbuddySessionId,
        secret: "[redacted]"
      }
    };
    const skills = {
      skills: [],
      enabledSkills: [],
      hostKind: "workbuddy"
    };

    if (include === "status") {
      return status;
    }
    if (include === "config") {
      return config;
    }
    if (include === "skills") {
      return skills;
    }
    return {
      status,
      config,
      ...skills,
      cronTasks: []
    };
  }

  function sendRaw(payload: string, countMessage = false): boolean {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    try {
      socket.send(payload);
      if (countMessage) {
        sentMessageCount += 1;
      }
      return true;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      return false;
    }
  }

  async function replayOutbox() {
    if (!outbox.length) {
      return;
    }
    const pending = outbox.splice(0, outbox.length);
    for (const frame of pending) {
      if (!sendRaw(JSON.stringify(frame), true)) {
        outbox.push(frame);
      }
    }
    outbox.splice(0, Math.max(0, outbox.length - MAX_OUTBOX_FRAMES));
  }

  function sendAppPing() {
    if (sendRaw(JSON.stringify({ action: "ping", data: { botId: input.config.botId } }))) {
      lastHeartbeatAt = new Date().toISOString();
    }
  }

  function upsertUserSession(message: Hub53AIIncomingMessage) {
    const sessionId = message.chatId;
    const now = new Date().toISOString();
    const existing = sessions.get(sessionId);
    const session =
      existing ??
      ({
        id: sessionId,
        title: buildSessionTitle(message),
        status: "running",
        hostKind: "workbuddy",
        runnerCommand: "codebuddy-channel",
        createdAt: now,
        updatedAt: now,
        lastEventSeq: 0,
        messages: []
      } satisfies ChannelSessionRecord);

    session.status = "running";
    session.updatedAt = now;
    session.title = session.title || buildSessionTitle(message);
    session.lastEventSeq += 1;
    session.messages.push({
      id: message.msgId || message.reqId,
      sessionId,
      role: "user",
      content: buildHub53AIPrompt(message),
      createdAt: now
    });
    sessions.set(sessionId, session);
    latestSessionByChat.set(message.chatId, sessionId);
  }

  function appendAssistantMessage(chatId: string, text: string) {
    const sessionId = latestSessionByChat.get(chatId) ?? chatId;
    const session = sessions.get(sessionId);
    if (!session) {
      return;
    }
    const now = new Date().toISOString();
    session.status = "completed";
    session.updatedAt = now;
    session.lastEventSeq += 1;
    session.messages.push({
      id: `assistant-${session.lastEventSeq}`,
      sessionId,
      role: "assistant",
      content: text,
      createdAt: now
    });
  }

  function buildSessionTitle(message: Hub53AIIncomingMessage): string {
    if (message.conversationTitle?.trim()) {
      return message.conversationTitle.trim();
    }
    const sender = message.userName || message.userId || message.chatId;
    const summary = message.text.trim().replace(/\s+/g, " ").slice(0, 32) || "新会话";
    return `53AI Hub-${sender}：${summary}`;
  }

  async function syncSharedWorkBuddySessionIndex(
    text: string,
    status: "running" | "completed",
    options?: { preserveTitleOnUpdate?: boolean }
  ) {
    try {
      await syncWorkBuddySessionIndex({
        workbuddyHome: input.config.workbuddyHome,
        sessionId: input.config.workbuddySessionId,
        cwd: join(input.config.workbuddyHome, "channels", "53aihub-workspace"),
        title: buildSharedWorkBuddySessionTitle(text),
        status,
        preserveTitleOnUpdate: options?.preserveTitleOnUpdate
      });
    } catch (error) {
      input.logger?.warn?.(
        `[53aihub-channel] failed to sync WorkBuddy session index: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  function buildSharedWorkBuddySessionTitle(text: string): string {
    const summary = text.trim().replace(/\s+/g, " ").slice(0, 36);
    return summary ? `53AIHub：${summary}` : "53AIHub WorkBuddy";
  }

  async function loadMergedSessionPayloads() {
    const merged = new Map<string, ReturnType<typeof toSessionPayload> | ReturnType<typeof toHistorySessionPayload>>();
    const history = await loadHistorySnapshot();
    for (const session of history.sessions) {
      merged.set(session.id, toHistorySessionPayload(session));
    }
    for (const session of sessions.values()) {
      merged.set(session.id, toSessionPayload(session));
    }
    return [...merged.values()]
      .filter((session): session is NonNullable<typeof session> => Boolean(session))
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }

  async function loadHistorySnapshot(): Promise<WorkBuddyHistorySnapshot> {
    if (input.config.workbuddyHistoryScope !== "all") {
      return {
        sessions: [],
        messagesBySessionId: new Map()
      };
    }
    return input.historyLoader
      ? input.historyLoader()
      : loadWorkBuddyHistory({ workbuddyHome: input.config.workbuddyHome });
  }

  function toSessionPayload(session?: ChannelSessionRecord) {
    if (!session) {
      return null;
    }
    return {
      id: session.id,
      session_id: session.id,
      conversation_id: session.id,
      title: session.title,
      status: session.status,
      hostKind: session.hostKind,
      runnerCommand: session.runnerCommand,
      createdAt: session.createdAt,
      updatedAt: session.updatedAt,
      lastEventSeq: session.lastEventSeq
    };
  }

  function toHistorySessionPayload(session?: WorkBuddyHistorySession) {
    if (!session) {
      return null;
    }
    return {
      id: session.id,
      session_id: session.id,
      conversation_id: session.id,
      title: session.title,
      status: session.status,
      hostKind: session.hostKind,
      runnerCommand: session.runnerCommand,
      createdAt: session.createdAt,
      updatedAt: session.updatedAt,
      lastEventSeq: session.lastEventSeq,
      ...(session.cwd ? { cwd: session.cwd } : {})
    };
  }

  function readRPCPagination(payload: unknown, defaultLimit: number) {
    const record = toRecord(payload);
    const limit = positiveInt(record.limit, defaultLimit);
    const offset = positiveInt(record.offset, 0);
    return {
      limit: Math.min(limit, 200),
      offset
    };
  }

  function buildPagination(limit: number, offset: number, total: number, pageLength: number) {
    const nextOffset = offset + pageLength;
    const hasMore = nextOffset < total;
    return {
      limit,
      offset,
      total,
      hasMore,
      ...(hasMore ? { nextOffset } : {})
    };
  }

  function readRPCChatId(payload: unknown): string {
    const record = toRecord(payload);
    const user = toRecord(record.user);
    const chatId =
      readOptionalString(record, "chat_id") ??
      readOptionalString(record, "chatId") ??
      readOptionalString(record, "user") ??
      readOptionalString(user, "id") ??
      readOptionalString(user, "userId") ??
      readOptionalString(record, "user_id") ??
      readOptionalString(record, "userId");
    if (!chatId) {
      throw new ChannelRPCError("PARAM_ERROR", "chat_id or user is required");
    }
    return chatId;
  }

  function readMutableRPCSession(payload: Record<string, unknown>): ChannelSessionRecord {
    const sessionId = readOptionalString(payload, "session_id") ?? readOptionalString(payload, "conversation_id");
    if (!sessionId) {
      throw new ChannelRPCError("PARAM_ERROR", "session_id or conversation_id is required");
    }
    const session = sessions.get(sessionId);
    if (!session) {
      throw new ChannelRPCError("PARAM_ERROR", `unknown session: ${sessionId}`);
    }
    return session;
  }

  async function readRPCSessionMessages(
    payload: Record<string, unknown>
  ): Promise<Array<ChannelMessageRecord | WorkBuddyHistoryMessage>> {
    const sessionId = readOptionalString(payload, "session_id") ?? readOptionalString(payload, "conversation_id");
    if (!sessionId) {
      throw new ChannelRPCError("PARAM_ERROR", "session_id or conversation_id is required");
    }
    const session = sessions.get(sessionId);
    if (session) {
      return session.messages;
    }
    const history = await loadHistorySnapshot();
    return history.messagesBySessionId.get(sessionId) ?? [];
  }

  function positiveInt(value: unknown, fallback: number): number {
    const parsed = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
    if (!Number.isFinite(parsed) || parsed < 0) {
      return fallback;
    }
    return Math.floor(parsed);
  }

  function normalizeRPCError(error: unknown) {
    if (error instanceof ChannelRPCError) {
      return {
        code: error.code,
        message: error.message
      };
    }
    return {
      code: "INTERNAL_ERROR",
      message: error instanceof Error ? error.message : String(error)
    };
  }

  function scheduleReconnect() {
    clearReconnect();
    if (input.config.maxReconnectAttempts >= 0 && reconnectAttempts >= input.config.maxReconnectAttempts) {
      connectionStatus = "error";
      input.logger?.error?.("[53aihub-channel] reconnect attempts exhausted");
      return;
    }
    reconnectAttempts += 1;
    const delayMs = Math.min(input.config.reconnectBaseMs * 2 ** (reconnectAttempts - 1), 30_000);
    reconnectTimer = setTimeout(connect, delayMs);
  }

  function clearHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function clearReconnect() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  return {
    start,
    stop,
    reply,
    getStatus
  };
}

export function resolveCodeBuddyChannelBrokerSocketPath(config: Hub53AIChannelConfig): string {
  const stateDir =
    readEnv(process.env, "HUB53AI_STATE_DIR", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_STATE_DIR") ||
    join(config.workbuddyHome, "channels", "53aihub");
  const identity = `${config.botId || "default"}-${config.workbuddySessionId || "shared"}`
    .replace(/[^A-Za-z0-9_.-]+/g, "-")
    .slice(0, 80);
  return join(stateDir, `${identity}.sock`);
}

export async function createCodeBuddyChannelBroker(input: {
  socketPath: string;
  handlers: {
    reply(reply: { chatId: string; text: string; reqId?: string }): Promise<void>;
    status(): unknown;
  };
  logger?: CodeBuddyChannelBridgeInput["logger"];
}): Promise<CodeBuddyChannelBroker> {
  await mkdir(dirname(input.socketPath), { recursive: true });
  const server = createServer((connection) => {
    let buffered = "";
    connection.setEncoding("utf8");
    connection.on("data", (chunk) => {
      buffered += chunk;
      const lines = buffered.split("\n");
      buffered = lines.pop() ?? "";
      for (const line of lines) {
        void handleBrokerLine(input.handlers, connection, line);
      }
    });
  });

  try {
    await listenOnUnixSocket(server, input.socketPath);
    input.logger?.info?.(`[53aihub-channel] local broker leader listening at ${input.socketPath}`);
    return {
      role: "leader",
      requestReply: input.handlers.reply,
      requestStatus: async () => input.handlers.status(),
      close: async () => {
        await closeNetServer(server);
        await rm(input.socketPath, { force: true }).catch(() => {});
      }
    };
  } catch (error) {
    await closeNetServer(server).catch(() => {});
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== "EADDRINUSE") {
      throw error;
    }
  }

  try {
    await sendBrokerRequest(input.socketPath, { action: "ping" });
    input.logger?.info?.(`[53aihub-channel] local broker follower connected to ${input.socketPath}`);
    return createFollowerBroker(input.socketPath);
  } catch {
    await rm(input.socketPath, { force: true }).catch(() => {});
  }

  const retryServer = createServer((connection) => {
    let buffered = "";
    connection.setEncoding("utf8");
    connection.on("data", (chunk) => {
      buffered += chunk;
      const lines = buffered.split("\n");
      buffered = lines.pop() ?? "";
      for (const line of lines) {
        void handleBrokerLine(input.handlers, connection, line);
      }
    });
  });
  await listenOnUnixSocket(retryServer, input.socketPath);
  input.logger?.info?.(`[53aihub-channel] local broker leader recovered stale socket at ${input.socketPath}`);
  return {
    role: "leader",
    requestReply: input.handlers.reply,
    requestStatus: async () => input.handlers.status(),
    close: async () => {
      await closeNetServer(retryServer);
      await rm(input.socketPath, { force: true }).catch(() => {});
    }
  };
}

function createFollowerBroker(socketPath: string): CodeBuddyChannelBroker {
  return {
    role: "follower",
    async requestReply(input) {
      await sendBrokerRequest(socketPath, {
        action: "reply",
        payload: {
          chatId: input.chatId,
          text: input.text,
          ...(input.reqId ? { reqId: input.reqId } : {})
        }
      });
    },
    async requestStatus() {
      const response = await sendBrokerRequest(socketPath, { action: "status" });
      return response.data;
    },
    async close() {}
  };
}

async function handleBrokerLine(
  handlers: {
    reply(reply: { chatId: string; text: string; reqId?: string }): Promise<void>;
    status(): unknown;
  },
  connection: NodeJS.WritableStream,
  line: string
) {
  const request = parseBrokerRequest(line);
  if (!request) {
    return;
  }
  try {
    if (request.action === "ping") {
      writeBrokerResponse(connection, { id: request.id, ok: true, data: { pong: true } });
      return;
    }
    if (request.action === "status") {
      writeBrokerResponse(connection, { id: request.id, ok: true, data: handlers.status() });
      return;
    }
    if (request.action === "reply") {
      const payload = toRecord(request.payload);
      const chatId = readRequiredString(payload, "chatId");
      const text = readRequiredString(payload, "text");
      const reqId = readOptionalString(payload, "reqId");
      await handlers.reply({ chatId, text, reqId });
      writeBrokerResponse(connection, { id: request.id, ok: true });
      return;
    }
    writeBrokerResponse(connection, { id: request.id, ok: false, error: `unknown broker action: ${request.action}` });
  } catch (error) {
    writeBrokerResponse(connection, {
      id: request.id,
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
}

function parseBrokerRequest(line: string):
  | {
      id: string;
      action: string;
      payload?: unknown;
    }
  | null {
  try {
    const parsed = JSON.parse(line);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return null;
    }
    const record = parsed as Record<string, unknown>;
    const id = readOptionalString(record, "id");
    const action = readOptionalString(record, "action");
    return id && action ? { id, action, payload: record.payload } : null;
  } catch {
    return null;
  }
}

function sendBrokerRequest(
  socketPath: string,
  request: {
    action: string;
    payload?: unknown;
  }
): Promise<Record<string, unknown>> {
  const id = `${Date.now()}-${process.pid}-${Math.random().toString(36).slice(2)}`;
  const payload = `${JSON.stringify({ id, ...request })}\n`;
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath);
    let buffered = "";
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error(`local broker request timed out: ${request.action}`));
    }, BROKER_REQUEST_TIMEOUT_MS);

    socket.setEncoding("utf8");
    socket.on("connect", () => {
      socket.write(payload);
    });
    socket.on("data", (chunk) => {
      buffered += chunk;
      const lines = buffered.split("\n");
      buffered = lines.pop() ?? "";
      for (const line of lines) {
        const response = parseBrokerResponse(line);
        if (response?.id !== id) {
          continue;
        }
        clearTimeout(timer);
        socket.end();
        if (!response.ok) {
          reject(new Error(response.error || `local broker request failed: ${request.action}`));
          return;
        }
        resolve(response);
      }
    });
    socket.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    socket.on("close", () => {
      clearTimeout(timer);
    });
  });
}

function parseBrokerResponse(line: string):
  | {
      id: string;
      ok: boolean;
      data?: unknown;
      error?: string;
    }
  | null {
  try {
    const parsed = JSON.parse(line);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return null;
    }
    const record = parsed as Record<string, unknown>;
    const id = readOptionalString(record, "id");
    if (!id || typeof record.ok !== "boolean") {
      return null;
    }
    return {
      id,
      ok: record.ok,
      data: record.data,
      error: readOptionalString(record, "error")
    };
  } catch {
    return null;
  }
}

function writeBrokerResponse(connection: NodeJS.WritableStream, response: Record<string, unknown>) {
  connection.write(`${JSON.stringify(response)}\n`);
}

function listenOnUnixSocket(server: NetServer, socketPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.off("error", reject);
      resolve();
    });
  });
}

function closeNetServer(server: NetServer): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!server.listening) {
      resolve();
      return;
    }
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

export function loadCodeBuddyChannelConfig(env: NodeJS.ProcessEnv = process.env): Hub53AIChannelConfig {
  return {
    wsUrl: readEnv(env, "HUB53AI_WS_URL", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WS_URL"),
    botId: readEnv(env, "HUB53AI_BOT_ID", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_BOT_ID"),
    secret: readEnv(env, "HUB53AI_SECRET", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_SECRET"),
    accessPolicy: parseAccessPolicy(readEnv(env, "HUB53AI_ACCESS_POLICY", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_ACCESS_POLICY")),
    allowFrom: parseAllowFrom(readEnv(env, "HUB53AI_ALLOW_FROM", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_ALLOW_FROM")),
    sendThinkingMessage: parseOptionalBoolean(
      readEnv(env, "HUB53AI_SEND_THINKING_MESSAGE", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_SEND_THINKING_MESSAGE"),
      true
    ),
    reconnectBaseMs: parseOptionalNumber(readEnv(env, "HUB53AI_RECONNECT_BASE_MS"), DEFAULT_RECONNECT_BASE_MS),
    maxReconnectAttempts: parseOptionalNumber(
      readEnv(env, "HUB53AI_MAX_RECONNECT_ATTEMPTS"),
      DEFAULT_MAX_RECONNECT_ATTEMPTS
    ),
    workbuddyHome:
      readEnv(env, "HUB53AI_WORKBUDDY_HOME", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_HOME") ||
      join(homedir(), ".workbuddy"),
    workbuddyHistoryScope: parseWorkBuddyHistoryScope(
      readEnv(env, "HUB53AI_WORKBUDDY_HISTORY_SCOPE", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_HISTORY_SCOPE")
    ),
    workbuddySessionId:
      readEnv(env, "HUB53AI_WORKBUDDY_SESSION_ID", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_WORKBUDDY_SESSION_ID") ||
      "53aihub-workbuddy-shared"
  };
}

export async function startCodeBuddyChannelServer(input?: {
  config?: Hub53AIChannelConfig;
  logger?: CodeBuddyChannelBridgeInput["logger"];
}) {
  const config = input?.config ?? loadCodeBuddyChannelConfig();
  validateHub53AIConfig(config);

  const logger = input?.logger ?? stderrLogger;
  const mcp = new Server(
    { name: "53aihub-channel", version: "0.1.13" },
    {
      capabilities: {
        experimental: {
          "claude/channel": {}
        },
        tools: {}
      } as any,
      instructions:
        "53AIHub messages arrive as <channel source=\"53aihub\" ...> events. Use the reply tool to send responses back to the same 53AIHub chat."
    }
  );

  const bridge = createCodeBuddyChannelBridge({
    config,
    logger,
    notifyChannel: async (notification) => {
      await mcp.notification({
        method: "notifications/claude/channel",
        params: notification
      } as any);
    }
  });
  const broker = await createCodeBuddyChannelBroker({
    socketPath: resolveCodeBuddyChannelBrokerSocketPath(config),
    handlers: {
      reply: (reply) => bridge.reply(reply),
      status: () => bridge.getStatus()
    },
    logger
  });

  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      {
        name: "reply",
        description: "Send a reply back to a 53AIHub chat through this channel.",
        inputSchema: {
          type: "object",
          properties: {
            chat_id: {
              type: "string",
              description: "53AIHub chat/conversation ID to reply to."
            },
            text: {
              type: "string",
              description: "Reply text to send."
            },
            req_id: {
              type: "string",
              description: "Optional 53AIHub request ID. Defaults to the latest request for chat_id."
            }
          },
          required: ["chat_id", "text"]
        }
      }
    ]
  }));

  mcp.setRequestHandler(CallToolRequestSchema, async (request) => {
    if (request.params.name !== "reply") {
      throw new Error(`unknown tool: ${request.params.name}`);
    }
    const args = toRecord(request.params.arguments);
    const chatId = readRequiredString(args, "chat_id");
    const text = readRequiredString(args, "text");
    const reqId = readOptionalString(args, "req_id");
    if (broker.role === "leader") {
      await bridge.reply({ chatId, text, reqId });
    } else {
      await broker.requestReply({ chatId, text, reqId });
    }
    return {
      content: [
        {
          type: "text",
          text: "sent"
        }
      ]
    };
  });

  if (broker.role === "leader") {
    await bridge.start();
  } else {
    logger.info?.("[53aihub-channel] running as local broker follower; WebSocket is owned by the leader process");
  }
  await mcp.connect(new StdioServerTransport());

  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    await bridge.stop();
    await broker.close();
    await mcp.close();
  };
  const shutdownAndExit = () => {
    void shutdown().finally(() => process.exit(0));
  };
  process.once("SIGINT", () => {
    shutdownAndExit();
  });
  process.once("SIGTERM", () => {
    shutdownAndExit();
  });
  process.stdin.once("end", shutdownAndExit);
  process.stdin.once("close", shutdownAndExit);

  return {
    mcp,
    bridge,
    stop: shutdown
  };
}

function sanitizeChannelMeta(meta: Record<string, string>): Record<string, string> {
  const sanitized: Record<string, string> = {};
  for (const [key, value] of Object.entries(meta)) {
    if (/^[A-Za-z0-9_]+$/.test(key) && value.trim()) {
      sanitized[key] = value;
    }
  }
  return sanitized;
}

function readEnv(env: NodeJS.ProcessEnv, ...keys: string[]): string {
  for (const key of keys) {
    const value = env[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function parseAccessPolicy(value: string): Hub53AIChannelConfig["accessPolicy"] {
  return value === "allowlist" ? "allowlist" : "open";
}

function parseWorkBuddyHistoryScope(value: string): Hub53AIChannelConfig["workbuddyHistoryScope"] {
  return value === "channel" ? "channel" : "all";
}

function parseAllowFrom(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseOptionalBoolean(value: string, fallback: boolean): boolean {
  if (!value) {
    return fallback;
  }
  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function parseOptionalNumber(value: string, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readRequiredString(record: Record<string, unknown>, key: string): string {
  const value = readOptionalString(record, key);
  if (!value) {
    throw new Error(`${key} is required`);
  }
  return value;
}

function readOptionalString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

const stderrLogger = {
  info(message: string) {
    process.stderr.write(`${message}\n`);
  },
  warn(message: string) {
    process.stderr.write(`${message}\n`);
  },
  error(message: string) {
    process.stderr.write(`${message}\n`);
  }
};

const stateDir = readEnv(process.env, "HUB53AI_STATE_DIR", "CODEBUDDY_PLUGIN_OPTION_HUB53AI_STATE_DIR");
if (stateDir) {
  process.env.HUB53AI_STATE_DIR = stateDir;
} else if (!process.env.HUB53AI_STATE_DIR) {
  process.env.HUB53AI_STATE_DIR = join(homedir(), ".workbuddy", "channels", "53aihub");
}

if (/^codebuddy-channel\.(?:cjs|js|ts)$/.test(basename(process.argv[1] ?? ""))) {
  startCodeBuddyChannelServer().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[53aihub-channel] fatal: ${message} (${inferHub53AIErrorCode(message)})\n`);
    process.exit(1);
  });
}
