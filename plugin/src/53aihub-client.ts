import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import WebSocket from "ws";

import type { GatewayClient, GatewayEvent, GatewaySession } from "./gateway-client";
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

type StoredHubState = {
  mappings: Record<string, string>;
  outbox: Hub53AIOutgoingChunk[];
};

type HubBridgeCallbacks = {
  onSessionUpsert(session: GatewaySession): Promise<void>;
  onUserMessage(message: SessionMessage): Promise<void>;
  onSessionStatus(sessionId: string, status: SessionStatus): Promise<void>;
  onEnsureSessionStream(sessionId: string): Promise<void>;
  getLastEventSeq(sessionId: string): number;
  onStatusChange(): void;
};

type HubBridgeInput = {
  stateDir: string;
  config: Hub53AIConfig;
  gateway: GatewayClient;
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
    if (reconnectAttempts >= input.config.maxReconnectAttempts) {
      connectionStatus = "error";
      lastError = `Max reconnect attempts (${input.config.maxReconnectAttempts}) reached`;
      notifyStatus();
      return;
    }
    reconnectAttempts += 1;
    const backoff = Math.min(input.config.reconnectBaseMs * 2 ** (reconnectAttempts - 1), 30_000);
    reconnectTimer = setTimeout(connect, backoff);
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

    const session = await resolveSession(message);
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
      await sendReply({
        reqId: message.reqId,
        text: DEFAULT_THINKING_MESSAGE,
        status: "thinking"
      });
    }

    const close = input.gateway.subscribe(session.id, input.callbacks.getLastEventSeq(session.id), {
      onEvent: (event) => {
        void handleGatewayEvent(message, event);
      },
      onDisconnect: (error) => {
        const messageText = error instanceof Error ? error.message : "gateway stream disconnected";
        void sendReply({
          reqId: message.reqId,
          text: `⚠️ ${messageText}`,
          status: "error",
          error: {
            code: "WEBSOCKET_ERROR",
            message: messageText
          }
        });
      }
    });

    const terminalPromise = waitForTerminalEvent(message.reqId);
    try {
      await input.gateway.sendMessage(session.id, buildPrompt(message));
      await terminalPromise;
    } catch (error) {
      const messageText = error instanceof Error ? error.message : String(error);
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ ${messageText}`,
        status: "error",
        error: {
          code: inferErrorCode(messageText),
          message: messageText
        }
      });
    } finally {
      close();
      clearTerminalResolver(message.reqId);
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

  async function handleGatewayEvent(message: Hub53AIIncomingMessage, event: GatewayEvent) {
    if (event.kind === "assistant.delta" || event.kind === "assistant.message") {
      const content = String(event.payload?.content ?? "");
      const delta = extractReplyDelta(message.reqId, content);
      if (delta) {
        await sendReply({
          reqId: message.reqId,
          text: delta,
          status: "streaming"
        });
      }
      return;
    }

    if (event.kind === "tool.call" || event.kind === "tool.result" || event.kind === "status.update") {
      const summary = summarizeVisibleActivity(event);
      if (summary && input.config.sendThinkingMessage) {
        await sendReply({
          reqId: message.reqId,
          text: summary,
          status: "thinking"
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
          status: "streaming"
        });
      }
      await sendReply({
        reqId: message.reqId,
        text: "",
        status: "done"
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
        error: {
          code: inferErrorCode(errorText),
          message: errorText
        }
      });
      lastReplyByReq.delete(message.reqId);
      resolveTerminalEvent(message.reqId);
    }
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
    const mappedId = state.mappings[message.chatId];
    if (mappedId) {
      const session = await input.gateway.getSession(mappedId);
      await input.callbacks.onSessionUpsert(session);
      return session;
    }

    const session = await input.gateway.createSession(`53AIHub ${message.chatId}`);
    state.mappings[message.chatId] = session.id;
    await persistState();
    await input.callbacks.onSessionUpsert(session);
    return session;
  }

  async function sendReply(inputReply: {
    reqId: string;
    text: string;
    status: "streaming" | "thinking" | "done" | "error";
    error?: {
      code: string;
      message: string;
      details?: string;
    };
  }) {
    const frame = buildOutgoingChunk(inputReply.reqId, inputReply.text, inputReply.status, inputReply.error);
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
      const messages = Array.isArray(openAIReq.messages) ? openAIReq.messages : [];
      const lastUserMsg = [...messages].reverse().find((message) => toRecord(message).role === "user");
      if (!lastUserMsg) {
        return null;
      }
      const userMessage = toRecord(lastUserMsg);
      const content = userMessage.content;
      const userId = stringOr(openAIReq.user, userMessage.name, `user-${String(wsMsg.req_id ?? randomUUID())}`);
      const chatId = stringOr(openAIReq.conversation_id, userId);
      return {
        type: "message",
        msgId: String(wsMsg.req_id ?? randomUUID()),
        reqId: String(wsMsg.req_id ?? randomUUID()),
        chatId,
        userId,
        text: extractTextFromContent(content),
        imageUrls: extractImagesFromContent(content),
        fileUrls: extractFilesFromContent(content)
      };
    }

    const data = toRecord(wsMsg.data);
    const chatId = stringOr(data.chatId, data.userId, "default-chat");
    const userId = stringOr(data.userId, data.chatId, "default-user");
    return {
      type: stringOr(data.type, "message"),
      msgId: stringOr(data.msgId, data.id, `msg-${Date.now()}`),
      reqId: String(wsMsg.req_id ?? data.msgId ?? data.id ?? `msg-${Date.now()}`),
      chatId,
      userId,
      text: stringOr(data.text, data.content, ""),
      imageUrls: normalizeUrlList(data.imageUrls, data.images),
      fileUrls: normalizeUrlList(data.fileUrls, data.files),
      quoteContent: typeof data.quoteContent === "string" ? data.quoteContent : undefined
    };
  } catch {
    return null;
  }
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
  error?: Hub53AIOutgoingChunk["data"]["error"]
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
