import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { homedir } from "node:os";
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
  createCodexAppServerTurnRunner,
  type CodexAppServerNotification,
  type CodexRunTurnResult,
  type CodexTurnRunner
} from "./codex-app-server";
import { detectCodexInstallation } from "./codex-runtime";
import {
  readCodexSessionState,
  writeCodexSessionState,
  type CodexPersistedSession,
  type CodexPersistedSessionEvent,
  type CodexPersistedSessionMessage,
  type CodexPersistedSessionStatus
} from "./codex-session-store";
import {
  DEFAULT_CODEX_WORKSPACE_ROOT,
  ensureCodexConversationWorkspace,
  readCodexWorkspaceMappings,
  updateCodexWorkspaceThread,
  type CodexWorkspaceMapping
} from "./codex-workspace";

export type CodexChannelConfig = Hub53AIBaseConfig & {
  reconnectBaseMs: number;
  maxReconnectAttempts: number;
  codexBinPath: string;
  codexVersion: string;
  workspaceRoot: string;
  configPath?: string;
  diagnosticLogs?: boolean;
  traceEvents?: boolean;
};

export type CodexChannelStatusSnapshot = {
  configured: boolean;
  healthy: boolean;
  connectionHealthy: boolean;
  connectionStatus: "connecting" | "connected" | "disconnected" | "error";
  hostKind: "codex";
  runnerCommand: "codex-app-server";
  botId?: string;
  wsUrl?: string;
  codexBinPath: string;
  codexVersion: string;
  workspaceRoot: string;
  lastHeartbeatAt?: string;
  lastConnectedAt?: string;
  lastError?: string;
  receivedMessageCount: number;
  sentMessageCount: number;
  pendingOutboundCount: number;
  knownChatCount: number;
};

export type CodexChannelBridgeInput = {
  config: CodexChannelConfig;
  runner?: CodexTurnRunner;
  logger?: {
    info?(message: string): void;
    warn?(message: string): void;
    error?(message: string): void;
  };
};

type CodexChannelMessageRecord = CodexPersistedSessionMessage;
type CodexChannelEventRecord = CodexPersistedSessionEvent;

type CodexChannelSessionRecord = {
  id: string;
  title: string;
  status: CodexPersistedSessionStatus;
  hostKind: "codex";
  runnerCommand: "codex-app-server";
  createdAt: string;
  updatedAt: string;
  lastEventSeq: number;
  messages: CodexChannelMessageRecord[];
  events: CodexChannelEventRecord[];
  workspace: CodexWorkspaceMapping;
  threadId?: string;
  activeTurn?: {
    reqId: string;
    turnId: string;
    status: "running" | "interrupted" | "failed" | "completed";
    answerBuffer: string;
    startedAt: string;
  };
};

type ChannelRPCErrorCode = "FEATURE_NOT_AVAILABLE" | "PARAM_ERROR" | "INTERNAL_ERROR" | "NETWORK_ERROR";

class ChannelRPCError extends Error {
  constructor(
    readonly code: ChannelRPCErrorCode,
    message: string
  ) {
    super(message);
  }
}

type LedgerPartType = "answer" | "thinking" | "tool" | "output_file" | "status";
type LedgerEventType =
  | "turn.started"
  | "part.delta"
  | "part.replace"
  | "part.done"
  | "turn.completed"
  | "turn.interrupted"
  | "turn.failed";
type LedgerTerminalStatus = "running" | "completed" | "interrupted" | "failed" | "cancelled";

type CodexTurnTraceSummary = {
  reqId: string;
  sessionId: string;
  turnId: string;
  notificationCount: number;
  methodCounts: Record<string, number>;
  agentMessageDeltaCount: number;
  reasoningDeltaCount: number;
  processDeltaCount: number;
  mcpToolProgressCount: number;
  turnCompletedCount: number;
  mappedEventCounts: Record<string, number>;
  answerTextLength: number;
  reasoningTextLength: number;
  processTextLength: number;
  ignoredCount: number;
  emptyDeltaCount: number;
  startedAt: string;
};

const HEARTBEAT_INTERVAL_MS = 30_000;
const DEFAULT_RECONNECT_BASE_MS = 2_000;
const DEFAULT_MAX_RECONNECT_ATTEMPTS = 10;
const MAX_OUTBOX_FRAMES = 200;
const DEFAULT_CODEX_CHANNEL_CONFIG_PATH = join(homedir(), ".53ai", "codex-channel", "config.json");

export function createCodexChannelBridge(input: CodexChannelBridgeInput) {
  let socket: WebSocket | null = null;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let reconnectTimer: NodeJS.Timeout | null = null;
  let stopped = false;
  let reconnectAttempts = 0;
  let connectionStatus: CodexChannelStatusSnapshot["connectionStatus"] = "disconnected";
  let lastHeartbeatAt: string | undefined;
  let lastConnectedAt: string | undefined;
  let lastError: string | undefined;
  let receivedMessageCount = 0;
  let sentMessageCount = 0;
  const latestReqByChat = new Map<string, string>();
  const latestSessionByChat = new Map<string, string>();
  const sessions = new Map<string, CodexChannelSessionRecord>();
  const outbox: Hub53AIOutgoingChunk[] = [];
  const runner =
    input.runner ??
    createCodexAppServerTurnRunner({
      binPath: input.config.codexBinPath,
      logger: input.logger
    });
  const turnTraceSummaries = new Map<string, CodexTurnTraceSummary>();

  async function start() {
    validateHub53AIConfig(input.config);
    await mkdir(input.config.workspaceRoot, { recursive: true });
    await hydrateSessionsFromWorkspaceMappings();
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
    await runner.close?.();
  }

  function getStatus(): CodexChannelStatusSnapshot {
    return {
      configured: Boolean(input.config.botId && input.config.secret && input.config.wsUrl && input.config.codexBinPath),
      healthy: connectionStatus === "connected",
      connectionHealthy: connectionStatus === "connected",
      connectionStatus,
      hostKind: "codex",
      runnerCommand: "codex-app-server",
      botId: maskHub53AIBotId(input.config.botId),
      wsUrl: sanitizeHub53AIWsUrl(input.config.wsUrl),
      codexBinPath: input.config.codexBinPath,
      codexVersion: input.config.codexVersion,
      workspaceRoot: input.config.workspaceRoot,
      lastHeartbeatAt,
      lastConnectedAt,
      lastError,
      receivedMessageCount,
      sentMessageCount,
      pendingOutboundCount: outbox.length,
      knownChatCount: latestSessionByChat.size
    };
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
      input.logger?.info?.(`[53aihub-codex] connected to ${sanitizeHub53AIWsUrl(input.config.wsUrl)}`);
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
        input.logger?.error?.(`[53aihub-codex] failed to process message: ${lastError}`);
      });
    });

    socket.on("error", (error) => {
      lastError = error instanceof Error ? error.message : String(error);
      connectionStatus = "error";
      input.logger?.error?.(`[53aihub-codex] websocket error: ${lastError}`);
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
    await handleIncomingChat(message);
  }

  async function handleIncomingChat(message: Hub53AIIncomingMessage) {
    receivedMessageCount += 1;
    latestReqByChat.set(message.chatId, message.reqId);

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

    const session = await upsertUserSession(message);
    if (session.status === "running" && session.activeTurn?.status === "running") {
      await sendReply({
        reqId: message.reqId,
        chatId: session.id,
        text: "当前 Codex 对话仍在运行，请稍后再发送新消息或先停止当前任务。",
        status: "error",
        error: {
          code: "SESSION_BUSY",
          message: "Codex conversation already has a running turn"
        }
      });
      return;
    }

    if (input.config.sendThinkingMessage) {
      await sendReply({
        reqId: message.reqId,
        chatId: session.id,
        text: DEFAULT_HUB53AI_THINKING_MESSAGE,
        status: "thinking",
        metadata: {
          eventKind: "assistant.thinking",
          payload: buildWorkspacePayload(session, { content: DEFAULT_HUB53AI_THINKING_MESSAGE })
        }
      });
    }

    try {
      session.status = "running";
      session.updatedAt = new Date().toISOString();
      const result = await runner.runTurn({
        prompt: buildHub53AIPrompt(message),
        cwd: session.workspace.workspaceDir,
        threadId: session.threadId,
        conversationId: session.id,
        onThreadStarted: async ({ threadId }) => {
          session.threadId = threadId;
          session.workspace.threadId = threadId;
          await updateCodexWorkspaceThread(input.config.workspaceRoot, session.id, threadId);
          await persistSession(session);
        },
        onTurnStarted: async ({ turnId }) => {
          await markTurnStarted(session, message.reqId, turnId);
        },
        onEvent: async (event) => {
          await handleCodexNotification(session, message.reqId, event);
        }
      });
      await finishTurn(session, message.reqId, result);
    } catch (error) {
      await failTurn(session, message.reqId, error);
    }
  }

  async function markTurnStarted(session: CodexChannelSessionRecord, reqId: string, turnId: string) {
    if (!turnId) {
      return;
    }
    if (session.activeTurn?.turnId === turnId) {
      return;
    }
    const now = new Date().toISOString();
    session.activeTurn = {
      reqId,
      turnId,
      status: "running",
      answerBuffer: "",
      startedAt: now
    };
    const event = appendLedgerEvent(session, {
      kind: "run.started",
      reqId,
      turnId,
      partId: `${turnId}:status`,
      partType: "status",
      eventType: "turn.started",
      operation: "noop",
      visibility: "hidden",
      terminalStatus: "running",
      payload: {
        source_kind: "run.started",
        workspace: buildWorkspacePayload(session)
      }
    });
    await persistSession(session);
    await sendEventChunk(reqId, session.id, "", "thinking", event);
    traceCodexMappedEvent(session, reqId, turnId, "turn.started", event, 0);
  }

  async function handleCodexNotification(
    session: CodexChannelSessionRecord,
    reqId: string,
    notification: CodexAppServerNotification
  ) {
    const params = notification.params || {};
    const turnId = readNotificationTurnId(notification) || session.activeTurn?.turnId || "codex-turn";
    traceCodexNotification(session, reqId, turnId, notification);
    if (notification.method === "turn/started") {
      await markTurnStarted(session, reqId, turnId);
      return;
    }

    if (!session.activeTurn || session.activeTurn.turnId !== turnId) {
      session.activeTurn = {
        reqId,
        turnId,
        status: "running",
        answerBuffer: "",
        startedAt: new Date().toISOString()
      };
    }

    if (notification.method === "item/agentMessage/delta") {
      const delta = readString(params.delta);
      if (!delta) {
        traceCodexSkippedDelta(session, reqId, turnId, notification.method);
        return;
      }
      session.activeTurn.answerBuffer += delta;
      const event = appendLedgerEvent(session, {
        kind: "assistant.delta",
        reqId,
        turnId,
        partId: `${turnId}:answer`,
        partType: "answer",
        eventType: "part.delta",
        operation: "append",
        visibility: "stream",
        text: delta,
        rawEventRef: buildRawEventRef(notification, session),
        payload: {
          source_kind: "assistant.delta",
          codex_method: notification.method,
          codex_item_id: readString(params.itemId)
        }
      });
      await persistSession(session);
      await sendEventChunk(reqId, session.id, delta, "streaming", event);
      traceCodexMappedEvent(session, reqId, turnId, "answer.delta", event, delta.length);
      return;
    }

    if (isReasoningDelta(notification.method)) {
      const delta = readString(params.delta);
      if (!delta) {
        traceCodexSkippedDelta(session, reqId, turnId, notification.method);
        return;
      }
      const event = appendLedgerEvent(session, {
        kind: "assistant.thinking",
        reqId,
        turnId,
        partId: `${turnId}:thinking:${readString(params.itemId) || "reasoning"}`,
        partType: "thinking",
        eventType: "part.delta",
        operation: "append",
        visibility: "stream",
        text: delta,
        rawEventRef: buildRawEventRef(notification, session),
        payload: {
          source_kind: "assistant.thinking",
          codex_method: notification.method,
          codex_item_id: readString(params.itemId)
        }
      });
      await persistSession(session);
      await sendEventChunk(reqId, session.id, delta, "thinking", event);
      traceCodexMappedEvent(session, reqId, turnId, "reasoning.delta", event, delta.length);
      return;
    }

    const processDelta = readProcessDelta(notification);
    if (processDelta) {
      const event = appendLedgerEvent(session, {
        kind: "process.step",
        reqId,
        turnId,
        partId: `${turnId}:process:${processDelta.itemId}`,
        partType: "status",
        eventType: "part.delta",
        operation: "append",
        visibility: "stream",
        text: processDelta.text,
        rawEventRef: buildRawEventRef(notification, session),
        payload: {
          source_kind: "process.step",
          object: "process.step",
          codex_method: notification.method,
          process_step: {
            step_code: processDelta.stepCode,
            title: processDelta.title,
            message: processDelta.text,
            status: "running",
            data: {
              content: processDelta.text,
              stream: processDelta.stream,
              codex_item_id: processDelta.itemId
            },
            timestamp: Date.now()
          }
        }
      });
      await persistSession(session);
      await sendEventChunk(reqId, session.id, processDelta.text, "thinking", event);
      traceCodexMappedEvent(session, reqId, turnId, "process.delta", event, processDelta.text.length);
      return;
    }

    if (notification.method === "turn/completed") {
      const turn = params.turn && typeof params.turn === "object" ? params.turn : {};
      const status = readString(turn.status) || "completed";
      if (status === "interrupted") {
        session.activeTurn.status = "interrupted";
      } else if (status === "failed") {
        session.activeTurn.status = "failed";
      } else {
        session.activeTurn.status = "completed";
      }
      await persistSession(session);
      traceCodexMappedEvent(session, reqId, turnId, "turn.completed.notification", undefined, 0, { codex_status: status });
    }
  }

  async function finishTurn(session: CodexChannelSessionRecord, reqId: string, result: CodexRunTurnResult) {
    const turnId = result.turnId || session.activeTurn?.turnId || "codex-turn";
    if (!session.threadId) {
      session.threadId = result.threadId;
      session.workspace.threadId = result.threadId;
      await updateCodexWorkspaceThread(input.config.workspaceRoot, session.id, result.threadId);
    }

    const finalText = result.finalText.trim();
    const streamedText = session.activeTurn?.answerBuffer || "";
    if (finalText && !streamedText) {
      const event = appendLedgerEvent(session, {
        kind: "assistant.delta",
        reqId,
        turnId,
        partId: `${turnId}:answer`,
        partType: "answer",
        eventType: "part.delta",
        operation: "append",
        visibility: "stream",
        text: finalText,
        payload: {
          source_kind: "assistant.delta",
          codex_method: "turn/completed"
        }
      });
      session.activeTurn = session.activeTurn ?? {
        reqId,
        turnId,
        status: "running",
        answerBuffer: "",
        startedAt: new Date().toISOString()
      };
      session.activeTurn.answerBuffer = finalText;
      await persistSession(session);
      await sendEventChunk(reqId, session.id, finalText, "streaming", event);
    }

    const terminalStatus = normalizeTerminalStatus(result.status);
    const eventType =
      terminalStatus === "interrupted" || terminalStatus === "cancelled"
        ? "turn.interrupted"
        : terminalStatus === "failed"
          ? "turn.failed"
          : "turn.completed";
    const kind =
      eventType === "turn.interrupted"
        ? "run.interrupted"
        : eventType === "turn.failed"
          ? "run.failed"
          : "run.completed";
    const event = appendLedgerEvent(session, {
      kind,
      reqId,
      turnId,
      partId: `${turnId}:status`,
      partType: "status",
      eventType,
      operation: "close",
      visibility: "final",
      terminalStatus,
      payload: {
        source_kind: kind,
        codex_status: result.status,
        workspace: buildWorkspacePayload(session)
      }
    });
    const answer = session.activeTurn?.answerBuffer || finalText;
    if (answer.trim()) {
      appendAssistantMessage(session, answer);
    }
    session.status = terminalStatus === "failed" ? "failed" : terminalStatus === "interrupted" ? "interrupted" : "completed";
    session.updatedAt = new Date().toISOString();
    if (session.activeTurn) {
      session.activeTurn.status = session.status === "completed" ? "completed" : session.status === "failed" ? "failed" : "interrupted";
    }
    await persistSession(session);
    await sendEventChunk(reqId, session.id, "", terminalStatus === "failed" ? "error" : "done", event);
    traceCodexMappedEvent(session, reqId, turnId, "turn.terminal", event, 0, {
      terminal_status: terminalStatus,
      result_status: result.status
    });
    traceCodexTurnSummary(session, reqId, turnId, terminalStatus);
  }

  async function failTurn(session: CodexChannelSessionRecord, reqId: string, error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    const turnId = session.activeTurn?.turnId || `codex-turn-${session.lastEventSeq + 1}`;
    const event = appendLedgerEvent(session, {
      kind: "run.failed",
      reqId,
      turnId,
      partId: `${turnId}:status`,
      partType: "status",
      eventType: "turn.failed",
      operation: "close",
      visibility: "final",
      text: message,
      terminalStatus: "failed",
      payload: {
        source_kind: "run.failed",
        codex_error: message,
        workspace: buildWorkspacePayload(session)
      }
    });
    session.status = "failed";
    session.updatedAt = new Date().toISOString();
    await persistSession(session);
    await sendEventChunk(reqId, session.id, message, "error", event, {
      code: inferHub53AIErrorCode(message),
      message
    });
    traceCodexMappedEvent(session, reqId, turnId, "turn.failed", event, message.length, {
      error_length: message.length,
      error_hash: shortTraceHash(message)
    });
    traceCodexTurnSummary(session, reqId, turnId, "failed");
  }

  async function sendEventChunk(
    reqId: string,
    chatId: string,
    text: string,
    status: Hub53AIOutgoingChunk["status"],
    event: CodexChannelEventRecord,
    error?: Hub53AIOutgoingChunk["data"]["error"]
  ) {
    await sendReply({
      reqId,
      chatId,
      text,
      status,
      error,
      metadata: {
        eventKind: event.kind,
        payload: event.payload
      }
    });
  }

  async function sendReply(inputReply: {
    reqId: string;
    chatId: string;
    text: string;
    status: Hub53AIOutgoingChunk["status"];
    error?: Hub53AIOutgoingChunk["data"]["error"];
    metadata?: {
      eventKind?: string;
      payload?: Record<string, unknown>;
    };
  }) {
    const frame = buildHub53AIOutgoingChunk(
      inputReply.reqId,
      inputReply.text,
      inputReply.status,
      inputReply.error,
      inputReply.chatId,
      inputReply.metadata
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
      const allSessions = [...sessions.values()]
        .map(toSessionPayload)
        .filter((session): session is Record<string, unknown> => Boolean(session))
        .sort((left, right) => String(right.updatedAt).localeCompare(String(left.updatedAt)));
      const page = allSessions.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        sessions: page,
        pagination: buildPagination(pagination.limit, pagination.offset, allSessions.length, page.length)
      };
    }

    if (request.action === "sessions.current") {
      const payload = toRecord(request.data);
      const chatId = readRPCChatId(payload);
      const sessionId = latestSessionByChat.get(chatId);
      return sessionId ? toSessionPayload(sessions.get(sessionId)) : null;
    }

    if (request.action === "sessions.messages") {
      const payload = toRecord(request.data);
      const session = readRPCSession(payload);
      const pagination = readRPCPagination(payload, 100);
      const page = session.messages.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        messages: page,
        events: [],
        pagination: buildPagination(pagination.limit, pagination.offset, session.messages.length, page.length)
      };
    }

    if (request.action === "sessions.events") {
      const payload = toRecord(request.data);
      const session = readRPCSession(payload);
      const afterSeq = positiveInt(payload.after_seq ?? payload.afterSeq, 0);
      const events = afterSeq > 0 ? session.events.filter((event) => event.seq > afterSeq) : session.events;
      const pagination = readRPCPagination(payload, 100);
      const page = events.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        events: page,
        pagination: buildPagination(pagination.limit, pagination.offset, events.length, page.length)
      };
    }

    if (request.action === "sessions.snapshot") {
      const payload = toRecord(request.data);
      const session = readRPCSession(payload);
      const afterSeq = positiveInt(payload.after_seq ?? payload.afterSeq, 0);
      const recentEvents = afterSeq > 0 ? session.events.filter((event) => event.seq > afterSeq) : session.events;
      return {
        session: toSessionPayload(session),
        messages: session.messages,
        events: recentEvents,
        recent_events: recentEvents,
        recentEvents,
        ledger_events: session.events.map((event) => event.payload.openclaw_ledger).filter(Boolean),
        ledgerEvents: session.events.map((event) => event.payload.openclaw_ledger).filter(Boolean),
        active_turns: session.activeTurn?.status === "running" ? [toActiveTurnPayload(session)] : [],
        last_seq: session.lastEventSeq,
        session_id: session.id,
        conversation_id: session.id,
        workspace: buildWorkspacePayload(session)
      };
    }

    if (request.action === "sessions.control") {
      const payload = toRecord(request.data);
      const action = readOptionalString(payload, "action");
      if (action !== "stop") {
        throw new ChannelRPCError("PARAM_ERROR", "unsupported sessions.control action");
      }
      const session = readRPCSession(payload);
      if (session.activeTurn?.status === "running" && session.threadId && runner.interruptTurn) {
        await runner.interruptTurn(session.threadId, session.activeTurn.turnId);
      }
      session.status = "stopped";
      session.updatedAt = new Date().toISOString();
      if (session.activeTurn) {
        session.activeTurn.status = "interrupted";
      }
      await persistSession(session);
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

  async function hydrateSessionsFromWorkspaceMappings() {
    const mappingFile = await readCodexWorkspaceMappings(input.config.workspaceRoot);
    const now = new Date().toISOString();
    for (const mapping of Object.values(mappingFile.conversations)) {
      const sessionId = mapping.conversationId.trim();
      if (!sessionId || sessions.has(sessionId)) {
        continue;
      }
      const workspace: CodexWorkspaceMapping = {
        ...mapping,
        workspaceRoot: input.config.workspaceRoot
      };
      const persisted = await readCodexSessionState(workspace);
      const threadId = mapping.threadId || persisted?.threadId;
      const session: CodexChannelSessionRecord = {
        id: sessionId,
        title: persisted?.title || buildRestoredSessionTitle(workspace),
        status: persisted?.status || "idle",
        hostKind: "codex",
        runnerCommand: "codex-app-server",
        createdAt: persisted?.createdAt || mapping.createdAt || now,
        updatedAt: persisted?.updatedAt || mapping.updatedAt || now,
        lastEventSeq: persisted?.lastEventSeq || 0,
        messages: persisted?.messages || [],
        events: persisted?.events || [],
        workspace: {
          ...workspace,
          ...(threadId ? { threadId } : {})
        },
        threadId
      };
      sessions.set(sessionId, session);
      latestSessionByChat.set(sessionId, sessionId);
      if (mapping.userId) {
        latestSessionByChat.set(mapping.userId, sessionId);
      }
    }
  }

  function resolveRuntimeRPC(payload: unknown): unknown {
    const include = (readOptionalString(toRecord(payload), "include") ?? "all").toLowerCase();
    const status = getStatus();
    const config = {
      gateway: {
        hostKind: "codex",
        runnerCommand: "codex-app-server"
      },
      codex: {
        binPath: input.config.codexBinPath,
        version: input.config.codexVersion,
        workspaceRoot: input.config.workspaceRoot
      },
      hub53ai: {
        enabled: true,
        botId: maskHub53AIBotId(input.config.botId),
        wsUrl: sanitizeHub53AIWsUrl(input.config.wsUrl),
        accessPolicy: input.config.accessPolicy,
        sendThinkingMessage: input.config.sendThinkingMessage,
        diagnosticLogs: Boolean(input.config.diagnosticLogs),
        traceEvents: Boolean(input.config.traceEvents),
        secret: "[redacted]"
      }
    };
    const skills = {
      skills: [],
      enabledSkills: [],
      plugins: [],
      hostKind: "codex"
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

  async function upsertUserSession(message: Hub53AIIncomingMessage): Promise<CodexChannelSessionRecord> {
    const sessionId = message.chatId || message.reqId;
    const now = new Date().toISOString();
    const existing = sessions.get(sessionId);
    if (existing) {
      existing.updatedAt = now;
      existing.status = existing.status === "running" ? existing.status : "idle";
      existing.messages.push({
        id: message.msgId || message.reqId,
        sessionId,
        role: "user",
        content: buildHub53AIPrompt(message),
        createdAt: now,
        metadata: buildWorkspacePayload(existing)
      });
      latestSessionByChat.set(message.chatId, sessionId);
      await persistSession(existing);
      return existing;
    }

    const workspace = await ensureCodexConversationWorkspace({
      conversationId: sessionId,
      userId: message.userId,
      userName: message.userName,
      workspaceRoot: input.config.workspaceRoot
    });
    const session: CodexChannelSessionRecord = {
      id: sessionId,
      title: buildSessionTitle(message),
      status: "idle",
      hostKind: "codex",
      runnerCommand: "codex-app-server",
      createdAt: now,
      updatedAt: now,
      lastEventSeq: 0,
      messages: [
        {
          id: message.msgId || message.reqId,
          sessionId,
          role: "user",
          content: buildHub53AIPrompt(message),
          createdAt: now,
          metadata: {
            hostKind: "codex",
            runnerCommand: "codex-app-server",
            workspace: {
              root: workspace.workspaceRoot,
              name: workspace.workspaceName,
              path: workspace.workspaceDir
            }
          }
        }
      ],
      events: [],
      workspace,
      threadId: workspace.threadId
    };
    sessions.set(sessionId, session);
    latestSessionByChat.set(message.chatId, sessionId);
    await persistSession(session);
    return session;
  }

  function appendAssistantMessage(session: CodexChannelSessionRecord, text: string) {
    const now = new Date().toISOString();
    session.messages.push({
      id: `assistant-${session.lastEventSeq}`,
      sessionId: session.id,
      role: "assistant",
      content: text,
      createdAt: now,
      metadata: buildWorkspacePayload(session)
    });
  }

  function appendLedgerEvent(
    session: CodexChannelSessionRecord,
    inputEvent: {
      kind: string;
      reqId: string;
      turnId: string;
      partId: string;
      partType: LedgerPartType;
      eventType: LedgerEventType;
      operation: "append" | "replace" | "close" | "noop";
      visibility: "stream" | "final" | "hidden";
      text?: string;
      payload?: Record<string, unknown>;
      terminalStatus?: LedgerTerminalStatus;
      rawEventRef?: string;
    }
  ): CodexChannelEventRecord {
    const seq = session.lastEventSeq + 1;
    const createdAt = new Date().toISOString();
    const rawEventRef =
      inputEvent.rawEventRef || `${session.id}:${seq}:${inputEvent.turnId}:${inputEvent.partId}:${inputEvent.eventType}`;
    const ledger = {
      protocol_version: "openclaw.ledger.v1",
      seq,
      session_id: session.id,
      conversation_id: session.id,
      turn_id: inputEvent.turnId,
      run_id: inputEvent.turnId,
      active_request_id: inputEvent.reqId,
      part_id: inputEvent.partId,
      part_type: inputEvent.partType,
      event_type: inputEvent.eventType,
      operation: inputEvent.operation,
      visibility: inputEvent.visibility,
      ...(inputEvent.text !== undefined ? { text: inputEvent.text } : {}),
      payload: {
        ...(inputEvent.payload || {}),
        hostKind: "codex",
        runnerCommand: "codex-app-server",
        workspace: buildWorkspacePayload(session)
      },
      ...(inputEvent.terminalStatus ? { terminal_status: inputEvent.terminalStatus } : {}),
      created_at: createdAt,
      raw_event_ref: rawEventRef
    };
    const payload = {
      ...(inputEvent.payload || {}),
      ...(inputEvent.text !== undefined ? { content: inputEvent.text } : {}),
      hostKind: "codex",
      runnerCommand: "codex-app-server",
      workspace: buildWorkspacePayload(session),
      openclaw_ledger: ledger
    };
    const event: CodexChannelEventRecord = {
      id: rawEventRef,
      sessionId: session.id,
      seq,
      kind: inputEvent.kind,
      payload,
      createdAt
    };
    session.lastEventSeq = seq;
    session.updatedAt = createdAt;
    session.events.push(event);
    return event;
  }

  async function persistSession(session: CodexChannelSessionRecord) {
    try {
      await writeCodexSessionState(session.workspace, toPersistedSession(session));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      input.logger?.warn?.(`[53aihub-codex] failed to persist session ${session.id}: ${message}`);
    }
  }

  function toPersistedSession(session: CodexChannelSessionRecord): CodexPersistedSession {
    return {
      id: session.id,
      title: session.title,
      status: session.status,
      createdAt: session.createdAt,
      updatedAt: session.updatedAt,
      lastEventSeq: session.lastEventSeq,
      messages: session.messages,
      events: session.events,
      ...(session.threadId ? { threadId: session.threadId } : {})
    };
  }

  function toSessionPayload(session: CodexChannelSessionRecord | undefined): Record<string, unknown> | null {
    if (!session) {
      return null;
    }
    return {
      id: session.id,
      session_id: session.id,
      conversation_id: session.id,
      title: session.title,
      status: session.status,
      hostKind: "codex",
      runnerCommand: "codex-app-server",
      createdAt: session.createdAt,
      updatedAt: session.updatedAt,
      lastEventSeq: session.lastEventSeq,
      cwd: session.workspace.workspaceDir,
      threadId: session.threadId,
      workspace: buildWorkspacePayload(session)
    };
  }

  function toActiveTurnPayload(session: CodexChannelSessionRecord): Record<string, unknown> | null {
    if (!session.activeTurn) {
      return null;
    }
    return {
      turn_id: session.activeTurn.turnId,
      run_id: session.activeTurn.turnId,
      active_request_id: session.activeTurn.reqId,
      status: session.activeTurn.status === "running" ? "running" : session.activeTurn.status,
      terminal_seq: 0,
      last_seq: session.lastEventSeq,
      part_ids: [`${session.activeTurn.turnId}:answer`, `${session.activeTurn.turnId}:status`]
    };
  }

  function buildWorkspacePayload(session: CodexChannelSessionRecord, extra?: Record<string, unknown>) {
    return {
      hostKind: "codex",
      runnerCommand: "codex-app-server",
      threadId: session.threadId,
      codex_thread_id: session.threadId,
      workspaceRoot: session.workspace.workspaceRoot,
      workspaceName: session.workspace.workspaceName,
      workspaceDir: session.workspace.workspaceDir,
      codex_workspace_root: session.workspace.workspaceRoot,
      codex_workspace_name: session.workspace.workspaceName,
      codex_workspace_path: session.workspace.workspaceDir,
      ...extra
    };
  }

  function readRPCSession(payload: Record<string, unknown>): CodexChannelSessionRecord {
    const sessionId = readRPCSessionId(payload);
    const session = sessions.get(sessionId);
    if (session) {
      return session;
    }

    const chatId = readRPCChatId(payload);
    const fallbackSessionId = latestSessionByChat.get(chatId);
    const fallbackSession = fallbackSessionId ? sessions.get(fallbackSessionId) : undefined;
    if (fallbackSession) {
      return fallbackSession;
    }

    const userId =
      readOptionalString(payload, "user") ??
      readOptionalString(payload, "user_id") ??
      readOptionalString(payload, "userId") ??
      "";
    const userFallbackSessionId = userId ? latestSessionByChat.get(userId) : undefined;
    const userFallbackSession = userFallbackSessionId ? sessions.get(userFallbackSessionId) : undefined;
    if (userFallbackSession) {
      return userFallbackSession;
    }

    throw new ChannelRPCError("PARAM_ERROR", `unknown Codex session: ${sessionId}`);
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

  function scheduleReconnect() {
    clearReconnect();
    if (stopped || reconnectAttempts >= input.config.maxReconnectAttempts) {
      return;
    }
    reconnectAttempts += 1;
    const delayMs = Math.min(input.config.reconnectBaseMs * reconnectAttempts, 30_000);
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

  function traceCodexNotification(
    session: CodexChannelSessionRecord,
    reqId: string,
    turnId: string,
    notification: CodexAppServerNotification
  ) {
    if (!codexEventTraceEnabled(input.config)) {
      return;
    }
    const params = notification.params || {};
    const method = notification.method;
    const summary = ensureCodexTurnTraceSummary(session, reqId, turnId);
    summary.notificationCount += 1;
    summary.methodCounts[method] = (summary.methodCounts[method] || 0) + 1;
    if (method === "item/agentMessage/delta") {
      summary.agentMessageDeltaCount += 1;
    } else if (isReasoningDelta(method)) {
      summary.reasoningDeltaCount += 1;
    } else if (method === "item/mcpToolCall/progress") {
      summary.mcpToolProgressCount += 1;
      summary.processDeltaCount += 1;
    } else if (readProcessDelta(notification)) {
      summary.processDeltaCount += 1;
    } else if (method === "turn/completed") {
      summary.turnCompletedCount += 1;
    } else if (method !== "turn/started") {
      summary.ignoredCount += 1;
    }

    traceCodexEvent("appserver.notification", {
      req_id: reqId,
      session_id: session.id,
      turn_id: turnId,
      thread_id: readString(params.threadId),
      method,
      item_id: readString(params.itemId),
      delta_length: readTraceDeltaLength(notification),
      has_turn_payload: Boolean(params.turn && typeof params.turn === "object")
    });
  }

  function traceCodexSkippedDelta(
    session: CodexChannelSessionRecord,
    reqId: string,
    turnId: string,
    method: string
  ) {
    if (!codexEventTraceEnabled(input.config)) {
      return;
    }
    const summary = ensureCodexTurnTraceSummary(session, reqId, turnId);
    summary.emptyDeltaCount += 1;
    traceCodexEvent("appserver.skip_empty_delta", {
      req_id: reqId,
      session_id: session.id,
      turn_id: turnId,
      method
    });
  }

  function traceCodexMappedEvent(
    session: CodexChannelSessionRecord,
    reqId: string,
    turnId: string,
    label: string,
    event?: CodexChannelEventRecord,
    textLength = 0,
    extra: Record<string, unknown> = {}
  ) {
    if (!codexEventTraceEnabled(input.config)) {
      return;
    }
    const summary = ensureCodexTurnTraceSummary(session, reqId, turnId);
    const kind = event?.kind || String(extra.source_kind || label);
    summary.mappedEventCounts[kind] = (summary.mappedEventCounts[kind] || 0) + 1;
    if (kind === "assistant.delta") {
      summary.answerTextLength += textLength;
    } else if (kind === "assistant.thinking") {
      summary.reasoningTextLength += textLength;
    } else if (kind === "process.step") {
      summary.processTextLength += textLength;
    }
    traceCodexEvent(`appserver.map.${label}`, {
      req_id: reqId,
      session_id: session.id,
      turn_id: turnId,
      event_kind: event?.kind,
      seq: event?.seq,
      text_length: textLength,
      ...extra
    });
  }

  function ensureCodexTurnTraceSummary(
    session: CodexChannelSessionRecord,
    reqId: string,
    turnId: string
  ): CodexTurnTraceSummary {
    const key = codexTurnTraceKey(reqId, turnId);
    const existing = turnTraceSummaries.get(key);
    if (existing) {
      return existing;
    }
    const summary: CodexTurnTraceSummary = {
      reqId,
      sessionId: session.id,
      turnId,
      notificationCount: 0,
      methodCounts: {},
      agentMessageDeltaCount: 0,
      reasoningDeltaCount: 0,
      processDeltaCount: 0,
      mcpToolProgressCount: 0,
      turnCompletedCount: 0,
      mappedEventCounts: {},
      answerTextLength: 0,
      reasoningTextLength: 0,
      processTextLength: 0,
      ignoredCount: 0,
      emptyDeltaCount: 0,
      startedAt: new Date().toISOString()
    };
    turnTraceSummaries.set(key, summary);
    return summary;
  }

  function traceCodexTurnSummary(
    session: CodexChannelSessionRecord,
    reqId: string,
    turnId: string,
    terminalStatus: LedgerTerminalStatus
  ) {
    if (!codexEventTraceEnabled(input.config)) {
      return;
    }
    const key = codexTurnTraceKey(reqId, turnId);
    const summary = ensureCodexTurnTraceSummary(session, reqId, turnId);
    traceCodexEvent("appserver.turn_summary", {
      req_id: summary.reqId,
      session_id: summary.sessionId,
      turn_id: summary.turnId,
      terminal_status: terminalStatus,
      notification_count: summary.notificationCount,
      method_counts: summary.methodCounts,
      agent_message_delta_count: summary.agentMessageDeltaCount,
      reasoning_delta_count: summary.reasoningDeltaCount,
      process_delta_count: summary.processDeltaCount,
      mcp_tool_progress_count: summary.mcpToolProgressCount,
      turn_completed_count: summary.turnCompletedCount,
      mapped_event_counts: summary.mappedEventCounts,
      answer_text_length: summary.answerTextLength,
      reasoning_text_length: summary.reasoningTextLength,
      process_text_length: summary.processTextLength,
      ignored_count: summary.ignoredCount,
      empty_delta_count: summary.emptyDeltaCount,
      started_at: summary.startedAt,
      completed_at: new Date().toISOString(),
      session_event_count: session.events.length
    });
    turnTraceSummaries.delete(key);
  }

  function traceCodexEvent(label: string, payload: Record<string, unknown>) {
    if (!codexEventTraceEnabled(input.config)) {
      return;
    }
    input.logger?.info?.(`[53aihub-codex-trace] ${label} ${safeTraceJson(payload)}`);
  }

  return {
    start,
    stop,
    getStatus
  };
}

export async function loadCodexChannelConfig(env: NodeJS.ProcessEnv = process.env): Promise<CodexChannelConfig> {
  const configPath = readEnv(env, "HUB53AI_CODEX_CHANNEL_CONFIG") || DEFAULT_CODEX_CHANNEL_CONFIG_PATH;
  const fileConfig = await readCodexChannelConfigFile(configPath);
  const configuredBinPath = readSetting(env, fileConfig, "codexBinPath", "HUB53AI_CODEX_BIN", "CODEX_BIN", "CODEX_PATH");
  const detected = await detectCodexInstallation(
    configuredBinPath ? { env, candidatePaths: [configuredBinPath] } : { env }
  );

  return {
    wsUrl: readSetting(env, fileConfig, "wsUrl", "HUB53AI_WS_URL"),
    botId: readSetting(env, fileConfig, "botId", "HUB53AI_BOT_ID"),
    secret: readSetting(env, fileConfig, "secret", "HUB53AI_SECRET"),
    accessPolicy: parseAccessPolicy(readSetting(env, fileConfig, "accessPolicy", "HUB53AI_ACCESS_POLICY")),
    allowFrom: parseAllowFrom(readSetting(env, fileConfig, "allowFrom", "HUB53AI_ALLOW_FROM")),
    sendThinkingMessage: parseOptionalBoolean(
      readSetting(env, fileConfig, "sendThinkingMessage", "HUB53AI_SEND_THINKING_MESSAGE"),
      true
    ),
    reconnectBaseMs: parseOptionalNumber(
      readSetting(env, fileConfig, "reconnectBaseMs", "HUB53AI_RECONNECT_BASE_MS"),
      DEFAULT_RECONNECT_BASE_MS
    ),
    maxReconnectAttempts: parseOptionalNumber(
      readSetting(env, fileConfig, "maxReconnectAttempts", "HUB53AI_MAX_RECONNECT_ATTEMPTS"),
      DEFAULT_MAX_RECONNECT_ATTEMPTS
    ),
    codexBinPath: detected.binPath,
    codexVersion: detected.version,
    workspaceRoot:
      readSetting(env, fileConfig, "workspaceRoot", "HUB53AI_CODEX_WORKSPACE_ROOT") ||
      DEFAULT_CODEX_WORKSPACE_ROOT,
    diagnosticLogs: parseOptionalBoolean(
      readSetting(env, fileConfig, "diagnosticLogs", "HUB53AI_CODEX_DIAG_LOGS", "OPENCLAW_DIAG_LOGS"),
      false
    ),
    traceEvents: parseOptionalBoolean(
      readSetting(env, fileConfig, "traceEvents", "HUB53AI_CODEX_TRACE_EVENTS"),
      false
    ),
    configPath
  };
}

export async function startCodexChannelServer(input?: {
  config?: CodexChannelConfig;
  runner?: CodexTurnRunner;
  logger?: CodexChannelBridgeInput["logger"];
}) {
  const config = input?.config ?? await loadCodexChannelConfig();
  validateHub53AIConfig(config);
  const bridge = createCodexChannelBridge({
    config,
    runner: input?.runner,
    logger: input?.logger ?? stderrLogger
  });
  await bridge.start();

  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    await bridge.stop();
  };
  const shutdownAndExit = () => {
    void shutdown().finally(() => process.exit(0));
  };
  process.once("SIGINT", shutdownAndExit);
  process.once("SIGTERM", shutdownAndExit);
  process.stdin.once("end", shutdownAndExit);
  process.stdin.once("close", shutdownAndExit);

  return {
    bridge,
    stop: shutdown
  };
}

async function readCodexChannelConfigFile(path: string): Promise<Record<string, unknown>> {
  if (!existsSync(path)) {
    return {};
  }
  try {
    const parsed = JSON.parse(await readFile(path, "utf8"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function readSetting(
  env: NodeJS.ProcessEnv,
  fileConfig: Record<string, unknown>,
  fileKey: string,
  ...envKeys: string[]
): string {
  for (const key of envKeys) {
    const value = env[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  const value = fileConfig[fileKey];
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry)).join(",");
  }
  return "";
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

function parseAccessPolicy(value: string): CodexChannelConfig["accessPolicy"] {
  return value === "allowlist" ? "allowlist" : "open";
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

function codexEventTraceEnabled(config: Pick<CodexChannelConfig, "diagnosticLogs" | "traceEvents">): boolean {
  return Boolean(config.traceEvents || config.diagnosticLogs);
}

function codexTurnTraceKey(reqId: string, turnId: string): string {
  return `${reqId}\0${turnId}`;
}

function readTraceDeltaLength(notification: CodexAppServerNotification): number {
  const params = notification.params || {};
  if (typeof params.delta === "string") {
    return params.delta.length;
  }
  if (typeof params.message === "string") {
    return params.message.length;
  }
  if (typeof params.deltaBase64 === "string") {
    return params.deltaBase64.length;
  }
  return 0;
}

function safeTraceJson(payload: Record<string, unknown>): string {
  try {
    return JSON.stringify(payload);
  } catch {
    return "{}";
  }
}

function shortTraceHash(value: string): string {
  if (!value) {
    return "";
  }
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

function readRPCPagination(payload: unknown, defaultLimit: number): { limit: number; offset: number } {
  const record = toRecord(payload);
  const limit = positiveInt(record.limit, defaultLimit);
  const offset = positiveInt(record.offset, 0);
  return {
    limit: Math.min(Math.max(limit, 1), 200),
    offset
  };
}

function buildPagination(limit: number, offset: number, total: number, count: number) {
  return {
    limit,
    offset,
    total,
    hasMore: offset + count < total
  };
}

function readRPCChatId(payload: Record<string, unknown>): string {
  return readOptionalString(payload, "chat_id") ??
    readOptionalString(payload, "chatId") ??
    readOptionalString(payload, "conversation_id") ??
    readOptionalString(payload, "conversationId") ??
    "";
}

function readRPCSessionId(payload: Record<string, unknown>): string {
  const value =
    readOptionalString(payload, "session_id") ??
    readOptionalString(payload, "sessionId") ??
    readOptionalString(payload, "conversation_id") ??
    readOptionalString(payload, "conversationId") ??
    readRPCChatId(payload);
  if (!value) {
    throw new ChannelRPCError("PARAM_ERROR", "session_id or conversation_id is required");
  }
  return value;
}

function positiveInt(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function normalizeRPCError(error: unknown): { code: ChannelRPCErrorCode; message: string } {
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

function buildSessionTitle(message: Hub53AIIncomingMessage): string {
  if (message.conversationTitle?.trim()) {
    return message.conversationTitle.trim();
  }
  const sender = message.userName || message.userId || message.chatId;
  const summary = message.text.trim().replace(/\s+/g, " ").slice(0, 32) || "新会话";
  return `53AI Hub-${sender}：${summary}`;
}

function buildRestoredSessionTitle(mapping: CodexWorkspaceMapping): string {
  const sender = mapping.userName?.trim() || mapping.userId?.trim() || mapping.conversationId;
  return `53AI Hub-${sender}`;
}

function readNotificationTurnId(notification: CodexAppServerNotification): string {
  const params = notification.params || {};
  if (typeof params.turnId === "string") {
    return params.turnId;
  }
  const turn = params.turn && typeof params.turn === "object" ? params.turn as Record<string, unknown> : {};
  return typeof turn.id === "string" ? turn.id : "";
}

function isReasoningDelta(method: string): boolean {
  return method === "item/reasoning/textDelta" ||
    method === "item/reasoning/summaryTextDelta" ||
    method === "item/plan/delta";
}

function readProcessDelta(notification: CodexAppServerNotification): {
  text: string;
  itemId: string;
  stream?: string;
  title: string;
  stepCode: string;
} | null {
  const params = notification.params || {};
  if (notification.method === "item/commandExecution/outputDelta" || notification.method === "item/fileChange/outputDelta") {
    const text = readString(params.delta);
    if (!text) {
      return null;
    }
    return {
      text,
      itemId: readString(params.itemId) || "command",
      title: notification.method === "item/fileChange/outputDelta" ? "File output" : "Command output",
      stepCode: notification.method === "item/fileChange/outputDelta" ? "file_output" : "command_output"
    };
  }
  if (notification.method === "command/exec/outputDelta" || notification.method === "process/outputDelta") {
    const text = decodeBase64(readString(params.deltaBase64));
    if (!text) {
      return null;
    }
    return {
      text,
      itemId: readString(params.processId) || readString(params.processHandle) || "process",
      stream: readString(params.stream),
      title: "Process output",
      stepCode: "process_output"
    };
  }
  if (notification.method === "item/mcpToolCall/progress") {
    const text = readString(params.message);
    if (!text) {
      return null;
    }
    return {
      text,
      itemId: readString(params.itemId) || "mcp-tool",
      title: "Tool progress",
      stepCode: "tool_progress"
    };
  }
  return null;
}

function buildRawEventRef(notification: CodexAppServerNotification, session: CodexChannelSessionRecord): string {
  const params = notification.params || {};
  return [
    session.id,
    notification.method,
    readString(params.turnId),
    readString(params.itemId),
    session.lastEventSeq + 1
  ].filter(Boolean).join(":");
}

function normalizeTerminalStatus(status: string): LedgerTerminalStatus {
  if (status === "interrupted" || status === "cancelled") {
    return "interrupted";
  }
  if (status === "failed") {
    return "failed";
  }
  return "completed";
}

function readString(value: unknown): string {
  return typeof value === "string" && value ? value : "";
}

function readOptionalString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function decodeBase64(value: string): string {
  if (!value) {
    return "";
  }
  try {
    return Buffer.from(value, "base64").toString("utf8");
  } catch {
    return "";
  }
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

if (/^codex-channel\.(?:cjs|js|ts)$/.test(basename(process.argv[1] ?? ""))) {
  startCodexChannelServer().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[53aihub-codex] fatal: ${message} (${inferHub53AIErrorCode(message)})\n`);
    process.exit(1);
  });
}

export async function writeCodexChannelInstallConfig(
  configPath: string,
  values: {
    wsUrl: string;
    botId: string;
    secret: string;
    codexBinPath: string;
    codexVersion: string;
    workspaceRoot: string;
    channelEntryPath: string;
  }
) {
  await mkdir(dirname(configPath), { recursive: true });
  await mkdir(values.workspaceRoot, { recursive: true });
  await writeFile(
    configPath,
    `${JSON.stringify(
      {
        wsUrl: values.wsUrl,
        botId: values.botId,
        secret: values.secret,
        codexBinPath: values.codexBinPath,
        codexVersion: values.codexVersion,
        workspaceRoot: values.workspaceRoot,
        channelEntryPath: values.channelEntryPath,
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: true,
        diagnosticLogs: false,
        traceEvents: false,
        runnerCommand: "codex-app-server",
        hostKind: "codex"
      },
      null,
      2
    )}\n`,
    { mode: 0o600 }
  );
}
