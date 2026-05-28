import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import WebSocket from "ws";

import type {
  GatewayClient,
  GatewayEvent,
  GatewayRuntimeInfo,
  GatewaySession
} from "./gateway-client";
import type { SessionMessage, SessionStatus, TimelineEvent } from "./models";

export type Hub53AIConfig = {
  enabled: boolean;
  botId: string;
  secret: string;
  wsUrl: string;
  accessPolicy: "open" | "allowlist";
  allowFrom: string[];
  sendThinkingMessage: boolean;
  reconnectBaseMs: number;
  maxReconnectAttempts: number;
};

export type Hub53AIStatusSnapshot = {
  enabled: boolean;
  configured: boolean;
  connectionStatus: "disabled" | "connecting" | "connected" | "disconnected" | "error";
  botId?: string;
  wsUrl?: string;
  lastHeartbeatAt?: string;
  lastConnectedAt?: string;
  lastError?: string;
  receivedMessageCount: number;
  sentMessageCount: number;
  pendingOutboundCount: number;
};

export type Hub53AIIncomingMessage = {
  type: string;
  msgId: string;
  reqId: string;
  chatId: string;
  userId: string;
  userName?: string;
  text: string;
  imageUrls?: string[];
  fileUrls?: string[];
  quoteContent?: string;
};

export type Hub53AIOutgoingChunk = {
  req_id: string;
  action: "chat";
  status: "streaming" | "thinking" | "done" | "error";
  data: {
    id: string;
    object: "chat.completion.chunk";
    created: number;
    model: "openclaw-agent";
    status?: "streaming" | "thinking" | "done" | "error";
    mode?: string;
    replace?: boolean;
    event_kind?: string;
    payload?: Record<string, unknown>;
    session_id?: string;
    conversation_id?: string;
    choices: Array<{
      index: number;
      delta: {
        content: string;
        role: "assistant";
      };
      finish_reason: "stop" | "error" | null;
    }>;
    error?: {
      code: string;
      message: string;
      details?: string;
    };
  };
};

export type Hub53AIOutgoingRPCFrame = {
  req_id: string;
  action: string;
  status: "done" | "error";
  data: unknown;
};

export type Hub53AIOutgoingFrame = Hub53AIOutgoingChunk | Hub53AIOutgoingRPCFrame;

type StoredHubState = {
  mappings: Record<string, string>;
  outbox: Hub53AIOutgoingChunk[];
};

type HubBridgeCallbacks = {
  onSessionUpsert(session: GatewaySession): Promise<void>;
  onUserMessage(message: SessionMessage): Promise<void>;
  onSessionStatus(sessionId: string, status: SessionStatus): Promise<void>;
  onBridgeThinkingEvent?(event: TimelineEvent): Promise<void>;
  listSessionEvents?(sessionId: string): TimelineEvent[] | Promise<TimelineEvent[]>;
  onEnsureSessionStream(sessionId: string): Promise<void>;
  getLastEventSeq(sessionId: string): number;
  onStatusChange(): void;
};

type HubBridgeInput = {
  stateDir: string;
  config: Hub53AIConfig;
  gateway: GatewayClient;
  rpcContext?: {
    getStatusSnapshot?: () => unknown | Promise<unknown>;
    getConfigSnapshot?: () => unknown | Promise<unknown>;
  };
  callbacks: HubBridgeCallbacks;
  logger?: {
    info?(message: string): void;
    warn?(message: string): void;
    error?(message: string): void;
  };
};

const HEARTBEAT_INTERVAL_MS = 30_000;
const DEFAULT_THINKING_MESSAGE = "正在处理您的请求...";
const MAX_OUTBOX_FRAMES = 200;
const RUN_WAIT_TIMEOUT_MS = 30 * 60 * 1000;
const HUB_SESSION_TITLE_PREFIX = "53AI Hub-";
const HUB_TITLE_SUMMARY_LENGTH = 40;
const HUB_RPC_ACTIONS = new Set([
  "sessions.list",
  "sessions.current",
  "sessions.messages",
  "sessions.events",
  "sessions.control",
  "runtime.get",
  "cron.tasks"
]);

type Hub53AIRPCRequest = {
  reqId: string;
  action: string;
  data: unknown;
};

type RPCPagination = {
  limit: number;
  offset: number;
};

class HubRPCError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly details?: string
  ) {
    super(message);
    this.name = "HubRPCError";
  }
}

export function createHub53AIBridge(input: HubBridgeInput) {
  const statePath = join(input.stateDir, "claw-control-center-53aihub.json");
  let state: StoredHubState = { mappings: {}, outbox: [] };
  let socket: WebSocket | null = null;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let reconnectTimer: NodeJS.Timeout | null = null;
  let stopped = false;
  let reconnectAttempts = 0;
  let connectionStatus: Hub53AIStatusSnapshot["connectionStatus"] = input.config.enabled ? "disconnected" : "disabled";
  let lastHeartbeatAt: string | undefined;
  let lastConnectedAt: string | undefined;
  let lastError: string | undefined;
  let receivedMessageCount = 0;
  let sentMessageCount = 0;
  const chatQueues = new Map<string, Promise<void>>();
  const lastReplyByReq = new Map<string, string>();
  let bridgeEventCounter = 0;

  async function start() {
    await loadState();
    if (!input.config.enabled) {
      connectionStatus = "disabled";
      notifyStatus();
      return;
    }
    validateConfig(input.config);
    stopped = false;
    connect();
  }

  async function stop() {
    stopped = true;
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (socket) {
      socket.close();
      socket = null;
    }
    await persistState();
  }

  function getStatus(): Hub53AIStatusSnapshot {
    return {
      enabled: input.config.enabled,
      configured: Boolean(input.config.botId && input.config.secret && input.config.wsUrl),
      connectionStatus,
      botId: maskBotId(input.config.botId),
      wsUrl: sanitizeWsUrl(input.config.wsUrl),
      lastHeartbeatAt,
      lastConnectedAt,
      lastError,
      receivedMessageCount,
      sentMessageCount,
      pendingOutboundCount: state.outbox.length
    };
  }

  function connect() {
    if (stopped) {
      return;
    }
    connectionStatus = "connecting";
    lastError = undefined;
    notifyStatus();

    const authBase64 = Buffer.from(`${input.config.botId}:${input.config.secret}`).toString("base64");
    socket = new WebSocket(input.config.wsUrl, {
      headers: {
        Authorization: `Bearer ${input.config.secret}`,
        "Proxy-Authorization": `Basic ${authBase64}`,
        "X-Bot-Id": input.config.botId,
        "X-Api-Key": input.config.secret
      }
    });

    socket.on("open", () => {
      reconnectAttempts = 0;
      connectionStatus = "connected";
      lastConnectedAt = new Date().toISOString();
      input.logger?.info?.(`[53aihub] connected to ${sanitizeWsUrl(input.config.wsUrl)}`);
      sendAppPing();
      void replayOutbox();
      heartbeatTimer = setInterval(() => {
        if (socket?.readyState === WebSocket.OPEN) {
          (socket as WebSocket & { ping?: () => void }).ping?.();
          sendAppPing();
        }
      }, HEARTBEAT_INTERVAL_MS);
      notifyStatus();
    });

    socket.on("message", (data) => {
      void handleRawMessage(data.toString()).catch((error) => {
        lastError = error instanceof Error ? error.message : String(error);
        input.logger?.error?.(`[53aihub] failed to process message: ${lastError}`);
        notifyStatus();
      });
    });

    socket.on("error", (error) => {
      lastError = error instanceof Error ? error.message : String(error);
      connectionStatus = "error";
      input.logger?.error?.(`[53aihub] websocket error: ${lastError}`);
      notifyStatus();
    });

    socket.on("close", (code, reason) => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      if (socket) {
        socket = null;
      }
      if (stopped) {
        connectionStatus = "disabled";
        notifyStatus();
        return;
      }
      connectionStatus = "disconnected";
      lastError = `WebSocket closed: ${code}${reason.length ? ` ${reason.toString()}` : ""}`;
      notifyStatus();
      scheduleReconnect();
    });
  }

  function scheduleReconnect() {
    if (stopped) {
      return;
    }
    if (reconnectTimer) {
      return;
    }

    const maxReconnectAttempts = Math.max(1, input.config.maxReconnectAttempts);
    const exceededConfiguredAttempts = reconnectAttempts >= maxReconnectAttempts;
    if (exceededConfiguredAttempts) {
      connectionStatus = "error";
      lastError = `Max reconnect attempts (${input.config.maxReconnectAttempts}) reached; continuing background reconnects`;
      notifyStatus();
    }

    const backoffExponent = Math.min(reconnectAttempts, maxReconnectAttempts - 1);
    const backoff = Math.min(input.config.reconnectBaseMs * 2 ** backoffExponent, 30_000);
    reconnectAttempts += 1;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, backoff);
  }

  async function handleRawMessage(rawPayload: string) {
    const heartbeat = parseHeartbeat(rawPayload);
    if (heartbeat === "pong") {
      lastHeartbeatAt = new Date().toISOString();
      notifyStatus();
      return;
    }
    if (heartbeat === "ping") {
      sendRaw(JSON.stringify({ action: "pong", data: { botId: input.config.botId } }));
      lastHeartbeatAt = new Date().toISOString();
      notifyStatus();
      return;
    }

    const rpcRequest = parseRPCRequest(rawPayload);
    if (rpcRequest) {
      await processRPCRequest(rpcRequest);
      return;
    }

    const message = parseIncomingMessage(rawPayload);
    if (!message) {
      return;
    }
    receivedMessageCount += 1;
    notifyStatus();

    const previous = chatQueues.get(message.chatId) ?? Promise.resolve();
    const next = previous
      .catch(() => undefined)
      .then(() => processIncomingMessage(message));
    chatQueues.set(message.chatId, next);
    await next.finally(() => {
      if (chatQueues.get(message.chatId) === next) {
        chatQueues.delete(message.chatId);
      }
    });
  }

  async function processRPCRequest(request: Hub53AIRPCRequest) {
    try {
      const data = await resolveRPCRequest(request);
      sendRPCFrame({
        req_id: request.reqId,
        action: request.action,
        status: "done",
        data
      });
    } catch (error) {
      const rpcError = normalizeRPCError(error);
      sendRPCFrame({
        req_id: request.reqId,
        action: request.action,
        status: "error",
        data: rpcError
      });
    }
  }

  async function resolveRPCRequest(request: Hub53AIRPCRequest): Promise<unknown> {
    if (request.action === "sessions.list") {
      const pagination = readRPCPagination(request.data, 50);
      return input.gateway.listSessionPage({
        limit: pagination.limit,
        offset: pagination.offset
      });
    }

    if (request.action === "sessions.current") {
      return resolveCurrentSessionRPC(request.data);
    }

    if (request.action === "sessions.messages") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      const pagination = readRPCPagination(payload, 100);
      const fetchLimit = pagination.offset + pagination.limit;
      const messages = await input.gateway.getSessionMessages(sessionId, fetchLimit);
      const pageMessages = sliceLatestWindowPage(messages, pagination.limit, pagination.offset);
      const events = await listSessionEvents(sessionId);
      const total = messages.length >= fetchLimit ? fetchLimit + 1 : messages.length;
      return {
        messages: pageMessages,
        events,
        pagination: buildPagination(pagination.limit, pagination.offset, total, pageMessages.length)
      };
    }

    if (request.action === "sessions.events") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      const pagination = readRPCPagination(payload, 100);
      const afterSeq = readRPCAfterSeq(payload);
      const events = (await listSessionEvents(sessionId)).filter((event) => event.seq > afterSeq);
      const pageEvents = events.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        events: pageEvents,
        pagination: buildPagination(pagination.limit, pagination.offset, events.length, pageEvents.length)
      };
    }

    if (request.action === "sessions.control") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      const action = stringOr(payload.action);
      if (action !== "stop") {
        throw new HubRPCError("PARAM_ERROR", "unsupported sessions.control action");
      }
      await input.gateway.controlSession(sessionId, "stop");
      return {
        ok: true,
        action,
        session_id: sessionId,
        conversation_id: sessionId
      };
    }

    if (request.action === "runtime.get") {
      return resolveRuntimeRPC(request.data);
    }

    if (request.action === "cron.tasks") {
      const pagination = readRPCPagination(request.data, 100);
      const runtimeInfo = await input.gateway.getRuntimeInfo();
      const tasks = runtimeInfo.cronTasks ?? [];
      const pageTasks = tasks.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        tasks: pageTasks,
        cronTasks: pageTasks,
        ...(runtimeInfo.cronScheduler ? { scheduler: runtimeInfo.cronScheduler, cronScheduler: runtimeInfo.cronScheduler } : {}),
        pagination: buildPagination(pagination.limit, pagination.offset, tasks.length, pageTasks.length)
      };
    }

    throw new HubRPCError("FEATURE_NOT_AVAILABLE", `Unsupported RPC action: ${request.action}`);
  }

  async function listSessionEvents(sessionId: string): Promise<TimelineEvent[]> {
    const [gatewayEvents, storedEvents] = await Promise.all([
      input.gateway.listEvents(sessionId, 0),
      input.callbacks.listSessionEvents?.(sessionId) ?? []
    ]);
    return dedupeTimelineEvents([...gatewayEvents, ...storedEvents]);
  }

  function dedupeTimelineEvents(events: TimelineEvent[]): TimelineEvent[] {
    const byKey = new Map<string, TimelineEvent>();
    for (const event of events) {
      const key = timelineEventDedupeKey(event);
      const previous = byKey.get(key);
      byKey.set(key, previous ? mergeTimelineEvent(previous, event) : event);
    }
    return [...byKey.values()].sort((left, right) => left.seq - right.seq);
  }

  function timelineEventDedupeKey(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const toolCallId = stringOr(data.toolCallId, data.callId, payload.toolCallId, payload.callId);
    if ((event.kind === "tool.call" || event.kind === "tool.result") && toolCallId) {
      return `${event.sessionId}:${event.kind}:${toolCallId}`;
    }
    return event.id || `${event.sessionId}:${event.seq}:${event.kind}`;
  }

  function mergeTimelineEvent(previous: TimelineEvent, incoming: TimelineEvent): TimelineEvent {
    return {
      ...previous,
      payload: mergeTimelinePayload(previous.payload, incoming.payload)
    };
  }

  function mergeTimelinePayload(previous: Record<string, unknown>, incoming: Record<string, unknown>): Record<string, unknown> {
    const previousData = toRecord(previous.data);
    const incomingData = toRecord(incoming.data);
    return {
      ...previous,
      ...incoming,
      ...(Object.keys(previousData).length > 0 || Object.keys(incomingData).length > 0
        ? {
            data: {
              ...previousData,
              ...incomingData
            }
          }
        : {})
    };
  }

  async function resolveRuntimeRPC(payload: unknown): Promise<unknown> {
    const include = stringOr(toRecord(payload).include, "all").toLowerCase();
    if (include === "status") {
      return input.rpcContext?.getStatusSnapshot ? await input.rpcContext.getStatusSnapshot() : await buildFallbackStatus();
    }
    if (include === "config") {
      return input.rpcContext?.getConfigSnapshot ? await input.rpcContext.getConfigSnapshot() : buildFallbackConfig();
    }
    if (include === "skills") {
      return buildSkillsPayload(await input.gateway.getRuntimeInfo());
    }

    const runtimeInfo = await input.gateway.getRuntimeInfo();
    return {
      status: input.rpcContext?.getStatusSnapshot ? await input.rpcContext.getStatusSnapshot() : await buildFallbackStatus(),
      config: input.rpcContext?.getConfigSnapshot ? await input.rpcContext.getConfigSnapshot() : buildFallbackConfig(),
      skills: buildSkillsPayload(runtimeInfo),
      cronTasks: runtimeInfo.cronTasks ?? []
    };
  }

  async function resolveCurrentSessionRPC(payload: unknown): Promise<GatewaySession | null> {
    const record = toRecord(payload);
    const userObject = toRecord(record.user);
    const chatId = stringOr(
      record.chat_id,
      record.chatId,
      record.user,
      userObject.id,
      userObject.userId,
      record.user_id,
      record.userId
    );
    if (!chatId) {
      throw new HubRPCError("PARAM_ERROR", "chat_id or user is required");
    }

    const mappedSession = await getMappedSession(chatId);
    if (mappedSession) {
      return mappedSession;
    }

    return null;
  }

  async function getMappedSession(chatId: string): Promise<GatewaySession | null> {
    const mappedId = state.mappings[chatId];
    if (!mappedId) {
      return null;
    }
    try {
      return await input.gateway.getSession(mappedId);
    } catch {
      delete state.mappings[chatId];
      await persistState();
      return null;
    }
  }

  async function buildFallbackStatus() {
    const [gatewayHealth, runtimeInfo] = await Promise.allSettled([input.gateway.getHealth(), input.gateway.getRuntimeInfo()]);
    return {
      hub53ai: getStatus(),
      gatewayHealth: gatewayHealth.status === "fulfilled" ? gatewayHealth.value : undefined,
      runtime: runtimeInfo.status === "fulfilled" ? runtimeInfo.value : undefined
    };
  }

  function buildFallbackConfig() {
    return {
      hub53ai: redactHubConfig(input.config)
    };
  }

  function sendRPCFrame(frame: Hub53AIOutgoingRPCFrame): boolean {
    return sendRaw(JSON.stringify(frame), true);
  }

  async function processIncomingMessage(message: Hub53AIIncomingMessage) {
    const accessResult = checkAccessPolicy(message);
    if (!accessResult.allowed) {
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ 访问被拒绝: ${accessResult.reason}`,
        status: "error",
        error: {
          code: "ACCESS_DENIED",
          message: accessResult.reason
        }
      });
      return;
    }

    let close: (() => void) | undefined;
    let sessionId = "";
    try {
      const session = await resolveSession(message);
      sessionId = session.id;
      await input.callbacks.onEnsureSessionStream(session.id);
      await input.callbacks.onUserMessage({
        id: `hub53ai-user-${message.msgId}`,
        sessionId: session.id,
        role: "user",
        content: buildPrompt(message),
        createdAt: new Date().toISOString()
      });
      await input.callbacks.onSessionStatus(session.id, "running");

      if (input.config.sendThinkingMessage) {
        await recordBridgeThinkingEvent(session.id, DEFAULT_THINKING_MESSAGE);
        await sendReply({
          reqId: message.reqId,
          text: DEFAULT_THINKING_MESSAGE,
          status: "thinking",
          sessionId
        });
      }

      const eventScope: GatewayEventScope = {
        eventBoundaryMs: Date.now(),
        currentActivitySeen: false
      };
      const terminalPromise = waitForTerminalEvent(message.reqId);
      close = input.gateway.subscribe(session.id, input.callbacks.getLastEventSeq(session.id), {
        onEvent: (event) => {
          void handleGatewayEvent(message, event, sessionId, eventScope);
        },
        onDisconnect: (error) => {
          const messageText = error instanceof Error ? error.message : "gateway stream disconnected";
          void sendReply({
            reqId: message.reqId,
            text: `⚠️ ${messageText}`,
            status: "error",
            sessionId,
            error: {
              code: "WEBSOCKET_ERROR",
              message: messageText
            }
          }).finally(() => {
            resolveTerminalEvent(message.reqId);
          });
        }
      });

      await input.gateway.sendMessage(session.id, buildPrompt(message));
      await terminalPromise;
    } catch (error) {
      const messageText = error instanceof Error ? error.message : String(error);
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ ${messageText}`,
        status: "error",
        sessionId,
        error: {
          code: inferErrorCode(messageText),
          message: messageText
        }
      });
    } finally {
      close?.();
      clearTerminalResolver(message.reqId);
      lastReplyByReq.delete(message.reqId);
    }
  }

  const terminalResolvers = new Map<
    string,
    {
      resolve: () => void;
      reject: (error: Error) => void;
      timer: NodeJS.Timeout;
    }
  >();
  function waitForTerminalEvent(reqId: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        terminalResolvers.delete(reqId);
        reject(new Error(`Timed out waiting for run completion (${reqId})`));
      }, RUN_WAIT_TIMEOUT_MS);
      terminalResolvers.set(reqId, { resolve, reject, timer });
    });
  }

  function resolveTerminalEvent(reqId: string) {
    const resolver = terminalResolvers.get(reqId);
    if (!resolver) {
      return;
    }
    clearTimeout(resolver.timer);
    resolver.resolve();
    terminalResolvers.delete(reqId);
  }

  function clearTerminalResolver(reqId: string) {
    const resolver = terminalResolvers.get(reqId);
    if (!resolver) {
      return;
    }
    clearTimeout(resolver.timer);
    terminalResolvers.delete(reqId);
  }

  type GatewayEventScope = {
    eventBoundaryMs: number;
    currentActivitySeen: boolean;
  };

  async function handleGatewayEvent(
    message: Hub53AIIncomingMessage,
    event: GatewayEvent,
    sessionId: string,
    eventScope: GatewayEventScope
  ) {
    if (isReplayFromPreviousRun(event, eventScope)) {
      return;
    }
    if (isCurrentRunActivityEvent(event)) {
      eventScope.currentActivitySeen = true;
    }

    if (event.kind === "assistant.delta" || event.kind === "assistant.message") {
      const content = String(event.payload?.content ?? "");
      const delta = extractReplyDelta(message.reqId, content);
      if (delta) {
        await sendReply({
          reqId: message.reqId,
          text: delta,
          status: "streaming",
          sessionId,
          mode: readStringMetadata(event.payload, "mode"),
          replace: readBooleanMetadata(event.payload, "replace"),
          eventKind: event.kind,
          payload: event.payload
        });
      }
      return;
    }

    if (event.kind === "assistant.thinking") {
      const content = String(event.payload?.content ?? "");
      if (content.trim() && input.config.sendThinkingMessage) {
        await sendReply({
          reqId: message.reqId,
          text: content,
          status: "thinking",
          sessionId,
          mode: readStringMetadata(event.payload, "mode"),
          replace: readBooleanMetadata(event.payload, "replace"),
          eventKind: event.kind,
          payload: event.payload
        });
      }
      return;
    }

    if (event.kind === "status.update") {
      return;
    }

    if (event.kind === "tool.call" || event.kind === "tool.result") {
      const summary = summarizeVisibleActivity(event);
      if (summary && input.config.sendThinkingMessage) {
        await recordBridgeThinkingEvent(resolveMappedSessionId(message), summary);
        await sendReply({
          reqId: message.reqId,
          text: summary,
          status: "thinking",
          sessionId,
          mode: "append",
          replace: false,
          eventKind: event.kind,
          payload: event.payload
        });
      }
      return;
    }

    if (event.kind === "run.completed") {
      const finalDelta = extractReplyDelta(message.reqId, String(event.payload?.content ?? ""));
      if (finalDelta) {
        await sendReply({
          reqId: message.reqId,
          text: finalDelta,
          status: "streaming",
          sessionId,
          mode: readStringMetadata(event.payload, "mode"),
          replace: readBooleanMetadata(event.payload, "replace"),
          eventKind: event.kind
        });
      }
      await sendReply({
        reqId: message.reqId,
        text: "",
        status: "done",
        sessionId
      });
      lastReplyByReq.delete(message.reqId);
      resolveTerminalEvent(message.reqId);
      return;
    }

    if (event.kind === "run.failed" || event.kind === "run.interrupted") {
      const errorText = String(event.payload?.error ?? event.payload?.message ?? event.kind);
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ ${errorText}`,
        status: "error",
        sessionId,
        error: {
          code: inferErrorCode(errorText),
          message: errorText
        }
      });
      lastReplyByReq.delete(message.reqId);
      resolveTerminalEvent(message.reqId);
    }
  }

  function isReplayFromPreviousRun(event: GatewayEvent, eventScope: GatewayEventScope): boolean {
    if (eventScope.currentActivitySeen || !isTerminalRunEvent(event)) {
      return false;
    }
    const eventMs = Date.parse(event.createdAt);
    return Number.isFinite(eventMs) && eventMs < eventScope.eventBoundaryMs;
  }

  function isTerminalRunEvent(event: GatewayEvent): boolean {
    return event.kind === "run.completed" || event.kind === "run.failed" || event.kind === "run.interrupted";
  }

  function isCurrentRunActivityEvent(event: GatewayEvent): boolean {
    if (event.kind === "run.started") {
      return true;
    }
    if (
      event.kind === "assistant.delta" ||
      event.kind === "assistant.message" ||
      event.kind === "assistant.thinking" ||
      event.kind === "tool.call" ||
      event.kind === "tool.result"
    ) {
      return true;
    }
    const payload = toRecord(event.payload);
    return event.kind === "status.update" && String(payload.phase ?? payload.status ?? "") === "running";
  }

  function resolveMappedSessionId(message: Hub53AIIncomingMessage): string {
    return state.mappings[message.chatId] ?? message.chatId;
  }

  async function recordBridgeThinkingEvent(sessionId: string, content: string) {
    const normalized = content.trim();
    if (!normalized || !input.callbacks.onBridgeThinkingEvent) {
      return;
    }

    bridgeEventCounter = (bridgeEventCounter + 1) % 1000;
    const createdAt = new Date().toISOString();
    const seq = -8_000_000_000_000_000 + Date.now() * 1000 + bridgeEventCounter;
    await input.callbacks.onBridgeThinkingEvent({
      id: `${sessionId}:hub-thinking:${Math.abs(seq)}`,
      sessionId,
      seq,
      kind: "assistant.thinking",
      payload: {
        content: normalized,
        source: "hub53ai"
      },
      createdAt
    });
  }

  function extractReplyDelta(reqId: string, content: string): string {
    if (!content) {
      return "";
    }

    const previous = lastReplyByReq.get(reqId) ?? "";
    if (!previous) {
      lastReplyByReq.set(reqId, content);
      return content;
    }

    if (content === previous || previous.startsWith(content)) {
      return "";
    }

    if (content.startsWith(previous)) {
      const delta = content.slice(previous.length);
      lastReplyByReq.set(reqId, content);
      return delta;
    }

    lastReplyByReq.set(reqId, `${previous}${content}`);
    return content;
  }

  async function resolveSession(message: Hub53AIIncomingMessage): Promise<GatewaySession> {
    const desiredTitle = buildHubSessionTitle(message);
    const mappedId = state.mappings[message.chatId];
    if (mappedId) {
      const session = await input.gateway.getSession(mappedId);
      const nextSession = await renamePlaceholderSessionIfNeeded(session, message, desiredTitle);
      await input.callbacks.onSessionUpsert(nextSession);
      return nextSession;
    }

    if (isOpenClawSessionId(message.chatId)) {
      const session = await input.gateway.getSession(message.chatId);
      state.mappings[message.chatId] = session.id;
      await persistState();
      await input.callbacks.onSessionUpsert(session);
      return session;
    }

    const session = await createSessionWithUniqueTitle(desiredTitle);
    state.mappings[message.chatId] = session.id;
    await persistState();
    await input.callbacks.onSessionUpsert(session);
    return session;
  }

  async function createSessionWithUniqueTitle(baseTitle: string): Promise<GatewaySession> {
    let lastDuplicateError: unknown;

    for (let attempt = 0; attempt < 6; attempt += 1) {
      const title = buildUniqueHubSessionTitle(baseTitle, attempt);
      try {
        return await input.gateway.createSession(title);
      } catch (error) {
        if (!isDuplicateSessionTitleError(error)) {
          throw error;
        }
        lastDuplicateError = error;
      }
    }

    const fallbackTitle = `${baseTitle} ${Date.now().toString(36)}`;
    try {
      return await input.gateway.createSession(fallbackTitle);
    } catch (error) {
      if (isDuplicateSessionTitleError(error) && lastDuplicateError) {
        throw lastDuplicateError;
      }
      throw error;
    }
  }

  async function renamePlaceholderSessionIfNeeded(
    session: GatewaySession,
    message: Hub53AIIncomingMessage,
    desiredTitle: string
  ): Promise<GatewaySession> {
    if (!isOldHubPlaceholderTitle(session.title, message.chatId)) {
      return session;
    }

    await input.gateway.controlSession(session.id, "rename", desiredTitle);
    return {
      ...session,
      title: desiredTitle,
      updatedAt: new Date().toISOString()
    };
  }

  async function sendReply(inputReply: {
    reqId: string;
    text: string;
    status: "streaming" | "thinking" | "done" | "error";
    sessionId?: string;
    error?: {
      code: string;
      message: string;
      details?: string;
    };
    mode?: string;
    replace?: boolean;
    eventKind?: string;
    payload?: Record<string, unknown>;
  }) {
    const frame = buildOutgoingChunk(
      inputReply.reqId,
      inputReply.text,
      inputReply.status,
      inputReply.error,
      inputReply.sessionId,
      {
        mode: inputReply.mode,
        replace: inputReply.replace,
        eventKind: inputReply.eventKind,
        payload: inputReply.payload
      }
    );
    if (!sendRaw(JSON.stringify(frame), true)) {
      state.outbox.push(frame);
      state.outbox = state.outbox.slice(-MAX_OUTBOX_FRAMES);
      await persistState();
    }
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
      notifyStatus();
      return true;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      notifyStatus();
      return false;
    }
  }

  async function replayOutbox() {
    if (state.outbox.length === 0) {
      return;
    }
    const pending = [...state.outbox];
    state.outbox = [];
    for (const frame of pending) {
      if (!sendRaw(JSON.stringify(frame), true)) {
        state.outbox.push(frame);
      }
    }
    state.outbox = state.outbox.slice(-MAX_OUTBOX_FRAMES);
    await persistState();
  }

  function sendAppPing() {
    if (sendRaw(JSON.stringify({ action: "ping", data: { botId: input.config.botId } }))) {
      lastHeartbeatAt = new Date().toISOString();
    }
  }

  function checkAccessPolicy(message: Hub53AIIncomingMessage): { allowed: boolean; reason: string } {
    if (input.config.accessPolicy === "open") {
      return { allowed: true, reason: "" };
    }
    const allowed = new Set(input.config.allowFrom);
    const candidates = [
      message.userId,
      message.chatId,
      `user:${message.userId}`,
      `53aihub:${message.userId}`,
      `53aihub:${message.chatId}`
    ];
    if (candidates.some((candidate) => allowed.has(candidate))) {
      return { allowed: true, reason: "" };
    }
    return { allowed: false, reason: "user is not in allowlist" };
  }

  async function loadState() {
    await mkdir(dirname(statePath), { recursive: true });
    try {
      const parsed = JSON.parse(await readFile(statePath, "utf8")) as StoredHubState;
      state = {
        mappings: parsed?.mappings && typeof parsed.mappings === "object" ? parsed.mappings : {},
        outbox: Array.isArray(parsed?.outbox) ? parsed.outbox : []
      };
    } catch {
      await persistState();
    }
  }

  async function persistState() {
    await writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`);
  }

  function notifyStatus() {
    input.callbacks.onStatusChange();
  }

  return {
    start,
    stop,
    getStatus
  };
}

export function parseIncomingMessage(rawJson: string): Hub53AIIncomingMessage | null {
  try {
    const wsMsg = JSON.parse(rawJson) as Record<string, any>;
    if (wsMsg.action === "ping" || wsMsg.action === "pong") {
      return null;
    }

    if (wsMsg.action === "chat") {
      const openAIReq = toRecord(wsMsg.data);
      const metadata = toRecord(openAIReq.metadata);
      const messages = Array.isArray(openAIReq.messages) ? openAIReq.messages : [];
      const lastUserMsg = [...messages].reverse().find((message) => toRecord(message).role === "user");
      if (!lastUserMsg) {
        return null;
      }
      const userMessage = toRecord(lastUserMsg);
      const content = userMessage.content;
      const userObject = toRecord(openAIReq.user);
      const userId = stringOr(
        openAIReq.user,
        userObject.id,
        userObject.userId,
        userMessage.userId,
        userMessage.name,
        `user-${String(wsMsg.req_id ?? randomUUID())}`
      );
      const chatId = stringOr(openAIReq.conversation_id, userId);
      return {
        type: "message",
        msgId: String(wsMsg.req_id ?? randomUUID()),
        reqId: String(wsMsg.req_id ?? randomUUID()),
        chatId,
        userId,
        userName: extractUserName(openAIReq, metadata, userObject, userMessage),
        text: extractTextFromContent(content),
        imageUrls: extractImagesFromContent(content),
        fileUrls: extractFilesFromContent(content)
      };
    }

    if (typeof wsMsg.status === "string" && wsMsg.status !== "request") {
      return null;
    }

    const data = toRecord(wsMsg.data);
    const userObject = toRecord(data.user);
    const chatId = stringOr(data.chatId, data.userId, "default-chat");
    const userId = stringOr(data.userId, userObject.id, userObject.userId, data.chatId, "default-user");
    return {
      type: stringOr(data.type, "message"),
      msgId: stringOr(data.msgId, data.id, `msg-${Date.now()}`),
      reqId: String(wsMsg.req_id ?? data.msgId ?? data.id ?? `msg-${Date.now()}`),
      chatId,
      userId,
      userName: extractUserName(data, userObject),
      text: stringOr(data.text, data.content, ""),
      imageUrls: normalizeUrlList(data.imageUrls, data.images),
      fileUrls: normalizeUrlList(data.fileUrls, data.files),
      quoteContent: typeof data.quoteContent === "string" ? data.quoteContent : undefined
    };
  } catch {
    return null;
  }
}

function parseRPCRequest(rawJson: string): Hub53AIRPCRequest | null {
  try {
    const parsed = JSON.parse(rawJson) as Record<string, unknown>;
    const reqId = stringOr(parsed.req_id, parsed.reqId);
    const action = stringOr(parsed.action);
    const status = stringOr(parsed.status);
    if (!reqId || !action) {
      return null;
    }
    if (status === "request") {
      return {
        reqId,
        action,
        data: parsed.data
      };
    }
    if (status) {
      return null;
    }
    if (!HUB_RPC_ACTIONS.has(action)) {
      return null;
    }
    return {
      reqId,
      action,
      data: parsed.data
    };
  } catch {
    return null;
  }
}

function readRPCPagination(payload: unknown, defaultLimit: number): RPCPagination {
  const record = toRecord(payload);
  return {
    limit: clampPositiveInteger(record.limit, defaultLimit, 200),
    offset: clampNonNegativeInteger(record.offset, 0)
  };
}

function readRPCSessionId(payload: Record<string, unknown>): string {
  const sessionId = stringOr(payload.session_id, payload.sessionId, payload.conversation_id, payload.conversationId);
  if (!sessionId) {
    throw new HubRPCError("PARAM_ERROR", "session_id or conversation_id is required");
  }
  return sessionId;
}

function readRPCAfterSeq(payload: Record<string, unknown>): number {
  return clampNonNegativeInteger(payload.after_seq ?? payload.afterSeq, 0);
}

function clampPositiveInteger(value: unknown, fallback: number, max: number): number {
  const numberValue = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  if (!Number.isInteger(numberValue) || numberValue <= 0) {
    return fallback;
  }
  return Math.min(numberValue, max);
}

function clampNonNegativeInteger(value: unknown, fallback: number): number {
  const numberValue = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  if (!Number.isInteger(numberValue) || numberValue < 0) {
    return fallback;
  }
  return numberValue;
}

function buildPagination(limit: number, offset: number, total: number, pageSize: number) {
  const nextOffset = offset + pageSize;
  const hasMore = nextOffset < total;
  return {
    limit,
    offset,
    total,
    hasMore,
    ...(hasMore ? { nextOffset } : {})
  };
}

export function sliceLatestWindowPage<T>(items: T[], limit: number, offset: number): T[] {
  const end = Math.max(0, items.length - offset);
  const start = Math.max(0, end - limit);
  return items.slice(start, end);
}

function buildSkillsPayload(runtimeInfo: GatewayRuntimeInfo) {
  return {
    ...(runtimeInfo.modelPrimary ? { modelPrimary: runtimeInfo.modelPrimary } : {}),
    skills: runtimeInfo.enabledSkills,
    enabledSkills: runtimeInfo.enabledSkills
  };
}

function redactHubConfig(config: Hub53AIConfig): Omit<Hub53AIConfig, "secret"> & { secret: string } {
  return {
    ...config,
    secret: "[redacted]"
  };
}

function normalizeRPCError(error: unknown) {
  if (error instanceof HubRPCError) {
    return {
      code: error.code,
      message: error.message,
      ...(error.details ? { details: error.details } : {})
    };
  }
  const message = error instanceof Error ? error.message : String(error);
  return {
    code: inferErrorCode(message),
    message
  };
}

function extractUserName(...sources: unknown[]): string | undefined {
  const nameKeys = [
    "userName",
    "username",
    "nickName",
    "nickname",
    "displayName",
    "senderName",
    "fromUserName",
    "name"
  ];

  for (const source of sources) {
    const record = toRecord(source);
    const direct = stringFromKeys(record, nameKeys);
    if (direct) {
      return direct;
    }

    const nestedUser = toRecord(record.user);
    const nestedName = stringFromKeys(nestedUser, nameKeys);
    if (nestedName) {
      return nestedName;
    }
  }

  return undefined;
}

function stringFromKeys(record: Record<string, any>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function buildHubSessionTitle(message: Hub53AIIncomingMessage): string {
  const userName = sanitizeTitlePart(message.userName || message.userId || message.chatId || "未知用户");
  const summary = summarizeHubMessageForTitle(message);
  return `${HUB_SESSION_TITLE_PREFIX}${userName}：${summary}`;
}

function buildUniqueHubSessionTitle(baseTitle: string, attempt: number): string {
  if (attempt === 0) {
    return baseTitle;
  }
  return `${baseTitle} (${attempt + 1})`;
}

function isDuplicateSessionTitleError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /label.*already.*in use|already.*in use|duplicate|title.*exists/i.test(message);
}

function summarizeHubMessageForTitle(message: Hub53AIIncomingMessage): string {
  const text = normalizeTitleText(message.text || message.quoteContent || "");
  if (text) {
    return truncateVisibleText(text, HUB_TITLE_SUMMARY_LENGTH);
  }
  if (message.imageUrls?.length && message.fileUrls?.length) {
    return "多媒体消息";
  }
  if (message.imageUrls?.length) {
    return "图片消息";
  }
  if (message.fileUrls?.length) {
    return "文件消息";
  }
  return "新会话";
}

function sanitizeTitlePart(value: string): string {
  return normalizeTitleText(value).replace(/[：:]/g, "").slice(0, 40) || "未知用户";
}

function normalizeTitleText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function truncateVisibleText(value: string, maxLength: number): string {
  const chars = Array.from(value);
  return chars.length > maxLength ? `${chars.slice(0, maxLength).join("")}…` : value;
}

function isOldHubPlaceholderTitle(title: string, chatId: string): boolean {
  const normalized = title.trim();
  return [`53AIHub ${chatId}`, `53AIHub:${chatId}`, `53AIHub-${chatId}`, chatId].includes(normalized);
}

function isOpenClawSessionId(value: string): boolean {
  return value.startsWith("agent:");
}

function buildPrompt(message: Hub53AIIncomingMessage): string {
  const parts = [message.text.trim()].filter(Boolean);
  if (message.imageUrls?.length) {
    parts.push(`Images:\n${message.imageUrls.join("\n")}`);
  }
  if (message.fileUrls?.length) {
    parts.push(`Files:\n${message.fileUrls.join("\n")}`);
  }
  return parts.join("\n\n");
}

function buildOutgoingChunk(
  reqId: string,
  text: string,
  status: Hub53AIOutgoingChunk["status"],
  error?: Hub53AIOutgoingChunk["data"]["error"],
  sessionId?: string,
  metadata?: {
    mode?: string;
    replace?: boolean;
    eventKind?: string;
    payload?: Record<string, unknown>;
  }
): Hub53AIOutgoingChunk {
  return {
    req_id: reqId,
    action: "chat",
    status,
    data: {
      id: reqId,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: "openclaw-agent",
      status,
      ...(metadata?.mode ? { mode: metadata.mode } : {}),
      ...(typeof metadata?.replace === "boolean" ? { replace: metadata.replace } : {}),
      ...(metadata?.eventKind ? { event_kind: metadata.eventKind } : {}),
      ...(metadata?.payload ? { payload: metadata.payload } : {}),
      ...(sessionId ? { session_id: sessionId, conversation_id: sessionId } : {}),
      choices: [
        {
          index: 0,
          delta: {
            content: text,
            role: "assistant"
          },
          finish_reason: status === "done" ? "stop" : status === "error" ? "error" : null
        }
      ],
      ...(error ? { error } : {})
    }
  };
}

function readStringMetadata(payload: unknown, key: string): string | undefined {
  if (!payload || typeof payload !== "object") {
    return undefined;
  }
  const value = (payload as Record<string, unknown>)[key];
  return typeof value === "string" && value.trim() ? value : undefined;
}

function readBooleanMetadata(payload: unknown, key: string): boolean | undefined {
  if (!payload || typeof payload !== "object") {
    return undefined;
  }
  const value = (payload as Record<string, unknown>)[key];
  return typeof value === "boolean" ? value : undefined;
}

function parseHeartbeat(rawPayload: string): "ping" | "pong" | null {
  try {
    const parsed = JSON.parse(rawPayload) as { action?: unknown };
    if (parsed.action === "ping" || parsed.action === "pong") {
      return parsed.action;
    }
  } catch {
    return null;
  }
  return null;
}

function extractTextFromContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((item) => {
      const record = toRecord(item);
      if (record.type === "text" && typeof record.text === "string") {
        return record.text;
      }
      return "";
    })
    .filter(Boolean)
    .join("\n");
}

function extractImagesFromContent(content: unknown): string[] {
  if (!Array.isArray(content)) {
    return [];
  }
  return normalizeUrlList(
    undefined,
    content
      .map((item) => {
        const record = toRecord(item);
        if (record.type === "image_url") {
          return toRecord(record.image_url).url;
        }
        if (record.type === "image") {
          return record.url ?? toRecord(record.image).url;
        }
        return undefined;
      })
      .filter(Boolean)
  );
}

function extractFilesFromContent(content: unknown): string[] {
  if (!Array.isArray(content)) {
    return [];
  }
  return normalizeUrlList(
    undefined,
    content
      .map((item) => {
        const record = toRecord(item);
        if (record.type === "file") {
          return record.url ?? toRecord(record.file).url;
        }
        return undefined;
      })
      .filter(Boolean)
  );
}

function normalizeUrlList(primary: unknown, fallback: unknown): string[] {
  const source = Array.isArray(primary) ? primary : Array.isArray(fallback) ? fallback : [];
  return source
    .map((entry) => {
      if (typeof entry === "string") {
        return entry;
      }
      const record = toRecord(entry);
      return typeof record.url === "string" ? record.url : "";
    })
    .filter(Boolean);
}

function summarizeVisibleActivity(event: TimelineEvent): string | null {
  const name = String(event.payload?.name ?? event.payload?.toolName ?? event.payload?.skillName ?? "").trim();
  if (event.kind === "tool.call") {
    return name ? `Used tool ${name}` : "Used a tool";
  }
  if (event.kind === "tool.result") {
    return name ? `Tool ${name} returned a result` : "Tool returned a result";
  }
  const message = String(event.payload?.message ?? event.payload?.status ?? "").trim();
  return message || null;
}

function validateConfig(config: Hub53AIConfig) {
  if (!config.botId) {
    throw new Error("hub53ai.botId is required when hub53ai.enabled is true");
  }
  if (!config.secret) {
    throw new Error("hub53ai.secret is required when hub53ai.enabled is true");
  }
  if (!config.wsUrl) {
    throw new Error("hub53ai.wsUrl is required when hub53ai.enabled is true");
  }
  if (!config.wsUrl.startsWith("ws://") && !config.wsUrl.startsWith("wss://")) {
    throw new Error("hub53ai.wsUrl must start with ws:// or wss://");
  }
}

function inferErrorCode(errorText: string): string {
  const lower = errorText.toLowerCase();
  if (lower.includes("timeout")) {
    return "TIMEOUT";
  }
  if (lower.includes("rate limit")) {
    return "RATE_LIMITED";
  }
  if (lower.includes("quota")) {
    return "INSUFFICIENT_QUOTA";
  }
  if (lower.includes("unauthorized") || lower.includes("access denied")) {
    return "ACCESS_DENIED";
  }
  if (lower.includes("websocket")) {
    return "WEBSOCKET_ERROR";
  }
  return "INTERNAL_ERROR";
}

function sanitizeWsUrl(wsUrl: string): string | undefined {
  if (!wsUrl) {
    return undefined;
  }
  try {
    const url = new URL(wsUrl);
    url.username = "";
    url.password = "";
    url.search = "";
    return url.toString();
  } catch {
    return wsUrl;
  }
}

function maskBotId(botId: string): string | undefined {
  if (!botId) {
    return undefined;
  }
  if (botId.length <= 4) {
    return `${botId.slice(0, 1)}***`;
  }
  return `${botId.slice(0, 2)}***${botId.slice(-2)}`;
}

function stringOr(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}
