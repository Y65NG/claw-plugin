import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import WebSocket from "ws";

import type {
  GatewayClient,
  GatewayEvent,
  GatewayRuntimeInfo,
  GatewaySession
} from "./gateway-client";
import {
  collectReferencedLocalOutputFiles,
  collectCreatedLocalOutputFiles,
  collectRecentReferencedLocalOutputFiles,
  extractReferencedLocalOutputPaths,
  snapshotLocalOutputFiles,
  type LocalOutputFileSnapshot
} from "./local-output-files";
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
  detectCreatedFiles?: boolean;
  fileWorkspaceDirs?: string[];
  createdFilesMaxFileBytes?: number;
  createdFilesMaxCount?: number;
  createdFilesExclude?: string[];
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
  conversationTitle?: string;
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

export type Hub53AIOutputFile = {
  id: string;
  file_name: string;
  url?: string;
  download_url?: string;
  signed_download_url?: string;
  mime_type?: string;
  size?: number;
  base64?: string;
  content?: string;
  message_id?: string;
  source_kind?: string;
};

type OpenClawTimelineV2SegmentType =
  | "answer"
  | "thinking"
  | "tool_call"
  | "tool_result"
  | "run"
  | "output_files";

type OpenClawTimelineV2Operation = "append" | "replace" | "close";
type OpenClawTimelineV2Visibility = "hidden" | "stream" | "final";

type OpenClawTimelineV2Meta = {
  protocol_version: "openclaw.timeline.v2";
  turn_id: string;
  segment_id: string;
  segment_type: OpenClawTimelineV2SegmentType;
  segment_index: number;
  delta_index: number;
  operation: OpenClawTimelineV2Operation;
  visibility: OpenClawTimelineV2Visibility;
  final: boolean;
};

export type Hub53AIMediaAttachment = Hub53AIOutputFile & {
  kind: "image" | "audio" | "video" | "text" | "file";
};

export type Hub53AIOutgoingProcessStep = {
  req_id: string;
  action: "chat";
  status: "streaming";
  data: {
    id: string;
    object: "process.step";
    created: number;
    model: "openclaw-agent";
    status: "streaming";
    session_id?: string;
    conversation_id?: string;
    process_step: {
      step_code: "output_files";
      name: string;
      status: "completed";
      message: string;
      data: {
        files: Hub53AIOutputFile[];
        contract_version: "v1";
        openclaw_timeline?: OpenClawTimelineV2Meta;
        media_attachments: Hub53AIMediaAttachment[];
        media_contract_version: "v1";
      };
      timestamp: number;
    };
  };
};

export type Hub53AIOutgoingRPCFrame = {
  req_id: string;
  action: string;
  status: "done" | "error";
  data: unknown;
};

export type Hub53AIQueuedFrame = Hub53AIOutgoingChunk | Hub53AIOutgoingProcessStep;
export type Hub53AIOutgoingFrame = Hub53AIQueuedFrame | Hub53AIOutgoingRPCFrame;

type StoredHubState = {
  mappings: Record<string, string>;
  outbox: Hub53AIQueuedFrame[];
};

type HubBridgeCallbacks = {
  onSessionUpsert(session: GatewaySession): Promise<void>;
  onUserMessage(message: SessionMessage): Promise<void>;
  onSessionStatus(sessionId: string, status: SessionStatus): Promise<void>;
  onBridgeThinkingEvent?(event: TimelineEvent): Promise<void>;
  listSessionEvents?(sessionId: string): TimelineEvent[] | Promise<TimelineEvent[]>;
  listKnownSessions?(): GatewaySession[] | Promise<GatewaySession[]>;
  onEnsureSessionStream(sessionId: string): Promise<void>;
  getLastEventSeq(sessionId: string): number;
  onStatusChange(): void;
};

type HubBridgeInput = {
  stateDir: string;
  configPath?: string;
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
const CONTROL_CENTER_SESSION_TITLE = "Claw Control Center";
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
  const activeReqIdsBySession = new Map<string, Set<string>>();
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
      const page = await input.gateway.listSessionPage({
        limit: pagination.limit,
        offset: pagination.offset
      });
      return mergeKnownHubSessionTitles(page);
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
      void input.gateway.controlSession(sessionId, "stop").catch((error) => {
        input.logger?.warn?.(
          `[53aihub] failed to stop session ${sessionId}: ${error instanceof Error ? error.message : String(error)}`
        );
      });
      resolveActiveSessionRequests(sessionId);
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
    const deduped = filterSyntheticToolPlaceholderThinkingEvents(
      filterSupersededHistoryThinkingEvents(
        dedupeTimelineEvents([...gatewayEvents, ...storedEvents])
          .map(normalizeTimelineEventSegmentType)
          .map(normalizeTimelineEventMessageSeq)
      )
    );
    traceOpenClawDuplicate(input.logger, "hub.events.list", {
      sessionId,
      gatewayCount: gatewayEvents.length,
      storedCount: storedEvents.length,
      dedupedCount: deduped.length,
      gatewayTail: gatewayEvents.slice(-6).map(summarizeTimelineEventForTrace),
      storedTail: storedEvents.slice(-6).map(summarizeTimelineEventForTrace),
      dedupedTail: deduped.slice(-6).map(summarizeTimelineEventForTrace)
    });
    return deduped;
  }

  function dedupeTimelineEvents(events: TimelineEvent[]): TimelineEvent[] {
    const byKey = new Map<string, TimelineEvent>();
    for (const event of events) {
      const key = timelineEventDedupeKey(event);
      const previous = byKey.get(key);
      byKey.set(key, previous ? mergeTimelineEvent(previous, event) : event);
    }
    return dedupeAssistantAnswerSnapshots(
      dedupeAssistantMessageEchoes([...byKey.values()].sort((left, right) => left.seq - right.seq))
    );
  }

  function timelineEventDedupeKey(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const toolCallId = stringOr(
      data.toolCallId,
      data.tool_call_id,
      data.callId,
      data.call_id,
      payload.toolCallId,
      payload.tool_call_id,
      payload.callId,
      payload.call_id
    );
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

  function dedupeAssistantMessageEchoes(events: TimelineEvent[]): TimelineEvent[] {
    const output: TimelineEvent[] = [];
    const assistantMessageByContent = new Map<string, number>();
    for (const event of events) {
      if (event.kind === "user.message" || event.kind === "run.started") {
        assistantMessageByContent.clear();
        output.push(event);
        continue;
      }
      if (event.kind !== "assistant.message") {
        output.push(event);
        continue;
      }

      const contentKey = normalizeAssistantDedupeContent(String(event.payload?.content ?? ""));
      if (!contentKey) {
        output.push(event);
        continue;
      }

      const previousIndex = assistantMessageByContent.get(contentKey);
      if (previousIndex === undefined) {
        assistantMessageByContent.set(contentKey, output.length);
        output.push(event);
        continue;
      }

      const previous = output[previousIndex];
      if (shouldPreferAssistantMessageEvent(event, previous)) {
        output[previousIndex] = {
          ...event,
          payload: mergeTimelinePayload(previous.payload, event.payload)
        };
      }
    }
    return output.sort((left, right) => left.seq - right.seq);
  }

  function dedupeAssistantAnswerSnapshots(events: TimelineEvent[]): TimelineEvent[] {
    const output: TimelineEvent[] = [];
    const candidateIndexes: number[] = [];

    for (const event of events) {
      if (event.kind === "user.message" || event.kind === "run.started") {
        candidateIndexes.length = 0;
        output.push(event);
        continue;
      }

      if (!isAssistantAnswerSnapshotEvent(event)) {
        output.push(event);
        continue;
      }

      const replacementIndex = findAssistantAnswerReplacementIndex(output, candidateIndexes, event);
      if (replacementIndex >= 0) {
        const previous = output[replacementIndex]!;
        if (shouldPreferAssistantAnswerSnapshot(event, previous)) {
          output[replacementIndex] = {
            ...event,
            payload: mergeTimelinePayload(previous.payload, event.payload)
          };
        }
        continue;
      }

      candidateIndexes.push(output.length);
      output.push(event);
    }

    return output.sort((left, right) => left.seq - right.seq);
  }

  function isAssistantAnswerSnapshotEvent(event: TimelineEvent): boolean {
    if (event.kind !== "assistant.message" && event.kind !== "assistant.delta") {
      return false;
    }
    return normalizeAssistantDedupeContent(String(event.payload?.content ?? "")) !== "";
  }

  function findAssistantAnswerReplacementIndex(
    output: TimelineEvent[],
    candidateIndexes: number[],
    incoming: TimelineEvent
  ): number {
    for (let position = candidateIndexes.length - 1; position >= 0; position -= 1) {
      const index = candidateIndexes[position]!;
      const previous = output[index];
      if (previous && canCollapseAssistantAnswerSnapshot(previous, incoming)) {
        return index;
      }
    }
    return -1;
  }

  function canCollapseAssistantAnswerSnapshot(previous: TimelineEvent, incoming: TimelineEvent): boolean {
    if (previous.sessionId !== incoming.sessionId) {
      return false;
    }

    const previousSegmentId = readAssistantAnswerSegmentId(previous);
    const incomingSegmentId = readAssistantAnswerSegmentId(incoming);
    if (previousSegmentId && incomingSegmentId) {
      return previousSegmentId === incomingSegmentId;
    }

    const previousRunId = readStringMetadata(previous.payload, "runId");
    const incomingRunId = readStringMetadata(incoming.payload, "runId");
    if (previousRunId && incomingRunId && previousRunId === incomingRunId) {
      return true;
    }

    if (isBareSessionAssistantMessageSnapshot(previous) && isAuthoritativeChatAnswerSnapshot(incoming)) {
      const distance = Math.abs(Number(incoming.seq || 0) - Number(previous.seq || 0));
      return distance > 0 && distance <= 20;
    }

    if (!shouldPreferAssistantAnswerSnapshot(incoming, previous)) {
      return false;
    }

    return areAssistantAnswerContentsRelated(
      String(previous.payload?.content ?? ""),
      String(incoming.payload?.content ?? "")
    );
  }

  function readAssistantAnswerSegmentId(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const timeline = toRecord(payload.openclaw_timeline);
    return stringOr(timeline.segment_id, payload.segment_id);
  }

  function shouldPreferAssistantAnswerSnapshot(incoming: TimelineEvent, previous: TimelineEvent): boolean {
    return assistantAnswerSnapshotSpecificity(incoming) >= assistantAnswerSnapshotSpecificity(previous);
  }

  function isBareSessionAssistantMessageSnapshot(event: TimelineEvent): boolean {
    if (event.kind !== "assistant.message") {
      return false;
    }
    const payload = toRecord(event.payload);
    return !readStringMetadata(payload, "runId") && payload.rawSeq === undefined && !readAssistantAnswerSegmentId(event);
  }

  function isAuthoritativeChatAnswerSnapshot(event: TimelineEvent): boolean {
    const payload = toRecord(event.payload);
    if (!readStringMetadata(payload, "runId")) {
      return false;
    }
    return event.kind === "assistant.message" || payload.replace === true || readStringMetadata(payload, "mode") === "replace";
  }

  function assistantAnswerSnapshotSpecificity(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    let score = assistantMessageSpecificity(event);
    if (event.kind === "assistant.message") score += 8;
    if (event.kind === "assistant.delta") score += 2;
    if (payload.replace === true) score += 2;
    if (payload.final === true) score += 2;
    if (readStringMetadata(payload, "visibility") === "final") score += 2;
    return score;
  }

  function areAssistantAnswerContentsRelated(leftContent: string, rightContent: string): boolean {
    const left = normalizeAssistantDedupeContent(leftContent);
    const right = normalizeAssistantDedupeContent(rightContent);
    if (!left || !right) {
      return false;
    }
    if (left === right || left.includes(right) || right.includes(left)) {
      return true;
    }
    const shorter = Math.min(left.length, right.length);
    if (shorter < 120) {
      return false;
    }
    const commonPrefix = commonPrefixLength(left, right);
    return commonPrefix / shorter >= 0.8;
  }

  function commonPrefixLength(left: string, right: string): number {
    const limit = Math.min(left.length, right.length);
    let index = 0;
    while (index < limit && left[index] === right[index]) {
      index += 1;
    }
    return index;
  }

  function shouldPreferAssistantMessageEvent(incoming: TimelineEvent, previous: TimelineEvent): boolean {
    return assistantMessageSpecificity(incoming) >= assistantMessageSpecificity(previous);
  }

  function assistantMessageSpecificity(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    let score = 0;
    if (typeof payload.runId === "string" && payload.runId.trim()) score += 4;
    if (payload.rawSeq !== undefined) score += 2;
    if (payload.state === "final") score += 1;
    if (payload.mode === "replace") score += 1;
    return score;
  }

  function normalizeAssistantDedupeContent(content: string): string {
    return content.replace(/\s+/g, " ").trim();
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
    const userName = stringOr(record.userName, record.user_name, userObject.name, userObject.userName, userObject.username);
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

    return restoreLatestHubSession(chatId, userName);
  }

  async function getMappedSession(chatId: string): Promise<GatewaySession | null> {
    const mappedId = state.mappings[chatId];
    if (!mappedId) {
      return null;
    }
    try {
      const session = await input.gateway.getSession(mappedId);
      if (!isHubManagedSessionTitle(session.title, chatId)) {
        delete state.mappings[chatId];
        await persistState();
        input.logger?.warn?.(
          `Ignored stale 53AIHub session mapping for ${chatId}: mapped session ${mappedId} is "${session.title}"`
        );
        return null;
      }
      return session;
    } catch {
      delete state.mappings[chatId];
      await persistState();
      return null;
    }
  }

  async function restoreLatestHubSession(chatId: string, userName?: string): Promise<GatewaySession | null> {
    const page = await input.gateway.listSessionPage({
      limit: 100,
      offset: 0
    });
    const knownSessions = await listKnownSessions();
    const sessions = mergeKnownHubSessions(page.sessions, knownSessions)
      .filter((session) => isRestorableHubSession(session, chatId, userName))
      .sort((left, right) => toTime(right.updatedAt || right.createdAt) - toTime(left.updatedAt || left.createdAt));
    const session = sessions[0];
    if (!session) {
      return null;
    }

    return session;
  }

  async function mergeKnownHubSessionTitles<T extends { sessions: GatewaySession[] }>(page: T): Promise<T> {
    const knownSessions = await listKnownSessions();
    if (!knownSessions.length) {
      return page;
    }
    return {
      ...page,
      sessions: mergeKnownHubSessions(page.sessions, knownSessions)
    };
  }

  async function listKnownSessions(): Promise<GatewaySession[]> {
    try {
      return (await input.callbacks.listKnownSessions?.()) ?? [];
    } catch (error) {
      input.logger?.warn?.(
        `Failed to read known 53AIHub sessions: ${error instanceof Error ? error.message : String(error)}`
      );
      return [];
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
      trackActiveSessionRequest(sessionId, message.reqId);
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
        turnId: buildOpenClawTimelineTurnId(session.id, message.reqId),
        nextSegmentIndex: 0,
        nextDeltaIndexBySegment: new Map<string, number>(),
        segmentIndexById: new Map<string, number>(),
        currentActivitySeen: false,
        emittedOutputFileKeys: new Set<string>(),
        referencedLocalOutputPaths: new Set<string>(),
        localOutputSnapshot: await snapshotLocalOutputFiles({
          config: input.config,
          configPath: input.configPath,
          stateDir: input.stateDir,
          logger: input.logger
        })
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
      if (sessionId) {
        untrackActiveSessionRequest(sessionId, message.reqId);
      }
      clearTerminalResolver(message.reqId);
      lastReplyByReq.delete(message.reqId);
    }
  }

  function trackActiveSessionRequest(sessionId: string, reqId: string) {
    const reqIds = activeReqIdsBySession.get(sessionId) ?? new Set<string>();
    reqIds.add(reqId);
    activeReqIdsBySession.set(sessionId, reqIds);
  }

  function untrackActiveSessionRequest(sessionId: string, reqId: string) {
    const reqIds = activeReqIdsBySession.get(sessionId);
    if (!reqIds) {
      return;
    }
    reqIds.delete(reqId);
    if (!reqIds.size) {
      activeReqIdsBySession.delete(sessionId);
    }
  }

  function resolveActiveSessionRequests(sessionId: string) {
    for (const reqId of activeReqIdsBySession.get(sessionId) ?? []) {
      lastReplyByReq.delete(reqId);
      resolveTerminalEvent(reqId);
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
    turnId: string;
    nextSegmentIndex: number;
    nextDeltaIndexBySegment: Map<string, number>;
    segmentIndexById: Map<string, number>;
    currentActivitySeen: boolean;
    emittedOutputFileKeys: Set<string>;
    localOutputSnapshot?: LocalOutputFileSnapshot;
    referencedLocalOutputPaths: Set<string>;
    localOutputFilesSent?: boolean;
  };

  async function handleGatewayEvent(
    message: Hub53AIIncomingMessage,
    event: GatewayEvent,
    sessionId: string,
    eventScope: GatewayEventScope
  ) {
    if (!(activeReqIdsBySession.get(sessionId)?.has(message.reqId) ?? true)) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_inactive_req", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      });
      return;
    }
    if (isReplayFromPreviousRun(event, eventScope)) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_previous_run", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      });
      return;
    }
    traceOpenClawDuplicate(input.logger, "hub.event.received", {
      reqId: message.reqId,
      sessionId,
      event: summarizeTimelineEventForTrace(event)
    });
    if (isCurrentRunActivityEvent(event)) {
      eventScope.currentActivitySeen = true;
    }
    for (const path of extractReferencedLocalOutputPaths(event.payload, eventScope.localOutputSnapshot, {
      config: input.config,
      configPath: input.configPath,
      stateDir: input.stateDir,
      logger: input.logger
    })) {
      eventScope.referencedLocalOutputPaths.add(path);
    }
    await sendOutputFilesForEvent(message.reqId, sessionId, event, eventScope);

    if (event.kind === "assistant.delta" || event.kind === "assistant.message") {
      const content = String(event.payload?.content ?? "");
      const replaceReply = isReplyReplaceEvent(event);
      const delta = extractReplyDelta(message.reqId, content, replaceReply);
      traceOpenClawDuplicate(input.logger, "hub.reply.delta_decision", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event),
        replaceReply,
        inputContentLength: content.length,
        outputDeltaLength: delta.length,
        outputDeltaHash: hashTraceText(delta)
      });
      if (delta) {
        await sendReply({
          reqId: message.reqId,
          text: delta,
          status: "streaming",
          sessionId,
          mode: readStringMetadata(event.payload, "mode"),
          replace: readBooleanMetadata(event.payload, "replace"),
          eventKind: event.kind,
          payload: augmentPayloadWithEventMeta(event, eventScope)
        });
      }
      if (event.kind === "assistant.message") {
        await sendCreatedLocalOutputFiles(message.reqId, sessionId, eventScope);
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
          payload: augmentPayloadWithEventMeta(event, eventScope)
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
        await sendReply({
          reqId: message.reqId,
          text: summary,
          status: "thinking",
          sessionId,
          mode: "append",
          replace: false,
          eventKind: event.kind,
          payload: augmentPayloadWithEventMeta(event, eventScope)
        });
      }
      return;
    }

    if (event.kind === "run.completed") {
      const finalDelta = extractReplyDelta(message.reqId, String(event.payload?.content ?? ""), isReplyReplaceEvent(event));
      if (finalDelta) {
        await sendReply({
          reqId: message.reqId,
          text: finalDelta,
          status: "streaming",
          sessionId,
          mode: readStringMetadata(event.payload, "mode"),
          replace: readBooleanMetadata(event.payload, "replace"),
          eventKind: event.kind,
          payload: augmentPayloadWithEventMeta(event, eventScope)
        });
      }
      await sendCreatedLocalOutputFiles(message.reqId, sessionId, eventScope);
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

  function isReplyReplaceEvent(event: TimelineEvent): boolean {
    return readBooleanMetadata(event.payload, "replace") === true || readStringMetadata(event.payload, "mode") === "replace";
  }

  function extractReplyDelta(reqId: string, content: string, replace = false): string {
    if (!content) {
      return "";
    }

    const previous = lastReplyByReq.get(reqId) ?? "";
    if (!previous) {
      lastReplyByReq.set(reqId, content);
      return content;
    }

    if (replace) {
      if (content === previous) {
        return "";
      }
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
    if (isOpenClawSessionId(message.chatId)) {
      const session = await getSessionWithKnownHubTitle(message.chatId, message.conversationTitle);
      await input.callbacks.onSessionUpsert(session);
      return session;
    }

    const restoredSession = await restoreLatestHubSession(message.chatId, message.userName || message.userId);
    if (restoredSession) {
      const nextSession = await renamePlaceholderSessionIfNeeded(restoredSession, message, desiredTitle);
      await input.callbacks.onSessionUpsert(nextSession);
      return nextSession;
    }

    const session = await createSessionWithUniqueTitle(desiredTitle);
    await input.callbacks.onSessionUpsert(session);
    return session;
  }

  async function getSessionWithKnownHubTitle(sessionId: string, titleHint?: string): Promise<GatewaySession> {
    const session = await input.gateway.getSession(sessionId);
    const knownSessions = await listKnownSessions();
    const titleHintSession = applyHubTitleHint(session, titleHint);
    return mergeKnownHubSessions([titleHintSession], knownSessions)[0] ?? titleHintSession;
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
    traceOpenClawDuplicate(input.logger, "hub.reply.send", summarizeOutgoingFrameForTrace(frame));
    if (!sendRaw(JSON.stringify(frame), true)) {
      state.outbox.push(frame);
      state.outbox = state.outbox.slice(-MAX_OUTBOX_FRAMES);
      await persistState();
    }
  }

  async function sendQueuedFrame(frame: Hub53AIQueuedFrame) {
    if (!sendRaw(JSON.stringify(frame), true)) {
      state.outbox.push(frame);
      state.outbox = state.outbox.slice(-MAX_OUTBOX_FRAMES);
      await persistState();
    }
  }

  function augmentPayloadWithEventMeta(event: GatewayEvent, eventScope: GatewayEventScope) {
    const timeline = buildOpenClawTimelineMeta(event, eventScope);
    return {
      ...(event.payload || {}),
      ...(typeof event.seq === "number" ? { seq: event.seq, message_seq: event.seq } : {}),
      turn_id: timeline.turn_id,
      segment_id: timeline.segment_id,
      segment_type: timeline.segment_type,
      segment_index: timeline.segment_index,
      delta_index: timeline.delta_index,
      operation: timeline.operation,
      visibility: timeline.visibility,
      final: timeline.final,
      openclaw_timeline: timeline
    };
  }

  function buildOpenClawTimelineTurnId(sessionId: string, reqId: string) {
    return `${sessionId || "openclaw"}:turn:${reqId || randomUUID()}`;
  }

  function buildOpenClawTimelineMeta(
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ): OpenClawTimelineV2Meta {
    const payloadTimeline = toRecord(toRecord(event.payload).openclaw_timeline);
    const payloadSegmentId = stringOr(payloadTimeline.segment_id, toRecord(event.payload).segment_id);
    const payloadSegmentType = stringOr(payloadTimeline.segment_type, toRecord(event.payload).segment_type);
    const segmentType = normalizeOpenClawSegmentType(payloadSegmentType) || getDefaultOpenClawSegmentType(event.kind);
    const segmentId = payloadSegmentId || getDefaultOpenClawSegmentId(event, eventScope.turnId, segmentType);
    const segmentIndex = getOpenClawSegmentIndex(segmentId, eventScope);
    const deltaIndex = getNextOpenClawDeltaIndex(segmentId, eventScope);
    const operation = getDefaultOpenClawOperation(event.kind);
    const visibility = getDefaultOpenClawVisibility(event.kind);
    return {
      protocol_version: "openclaw.timeline.v2",
      turn_id: stringOr(payloadTimeline.turn_id, toRecord(event.payload).turn_id, eventScope.turnId),
      segment_id: segmentId,
      segment_type: segmentType,
      segment_index: segmentIndex,
      delta_index: deltaIndex,
      operation,
      visibility,
      final: visibility === "final"
    };
  }

  function buildOpenClawOutputFilesTimelineMeta(
    eventScope: GatewayEventScope,
    files: Hub53AIOutputFile[]
  ): OpenClawTimelineV2Meta {
    const fileKey = files
      .flatMap((file) => getOutputFileKeys(file))
      .filter(Boolean)
      .sort()
      .join(",");
    const segmentId = `${eventScope.turnId}:output_files:${fileKey || "generated"}`;
    return {
      protocol_version: "openclaw.timeline.v2",
      turn_id: eventScope.turnId,
      segment_id: segmentId,
      segment_type: "output_files",
      segment_index: getOpenClawSegmentIndex(segmentId, eventScope),
      delta_index: getNextOpenClawDeltaIndex(segmentId, eventScope),
      operation: "replace",
      visibility: "final",
      final: true
    };
  }

  function normalizeOpenClawSegmentType(value: string): OpenClawTimelineV2SegmentType | undefined {
    if (
      value === "answer" ||
      value === "thinking" ||
      value === "tool_call" ||
      value === "tool_result" ||
      value === "run" ||
      value === "output_files"
    ) {
      return value;
    }
    return undefined;
  }

  function getExpectedOpenClawSegmentType(kind: string): OpenClawTimelineV2SegmentType | undefined {
    if (kind === "assistant.delta" || kind === "assistant.message") return "answer";
    if (kind === "assistant.thinking") return "thinking";
    if (kind === "tool.call") return "tool_call";
    if (kind === "tool.result") return "tool_result";
    if (kind === "run.completed" || kind === "run.failed" || kind === "run.interrupted") return "run";
    return undefined;
  }

  function normalizeTimelineEventSegmentType(event: TimelineEvent): TimelineEvent {
    const expectedSegmentType = getExpectedOpenClawSegmentType(event.kind);
    if (!expectedSegmentType) {
      return event;
    }

    const payload = toRecord(event.payload);
    const timeline = toRecord(payload.openclaw_timeline);
    const currentSegmentType = normalizeOpenClawSegmentType(stringOr(timeline.segment_type, payload.segment_type));
    if (currentSegmentType === expectedSegmentType) {
      return event;
    }

    return {
      ...event,
      payload: {
        ...payload,
        segment_type: expectedSegmentType,
        ...(Object.keys(timeline).length > 0
          ? {
              openclaw_timeline: {
                ...timeline,
                segment_type: expectedSegmentType
              }
            }
          : {})
      }
    };
  }

  function readHistoryMessageSeq(event: TimelineEvent): number {
    const match = String(event.id || "").match(/:history:(\d+)(?::|$)/);
    if (!match) return 0;
    const parsed = Number(match[1]);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function normalizeTimelineEventMessageSeq(event: TimelineEvent): TimelineEvent {
    const historyMessageSeq = readHistoryMessageSeq(event);
    if (!historyMessageSeq) {
      return event;
    }

    const payload = toRecord(event.payload);
    return {
      ...event,
      payload: {
        ...payload,
        messageSeq: payload.messageSeq ?? historyMessageSeq,
        message_seq: payload.message_seq ?? historyMessageSeq
      }
    };
  }

  function readTimelinePayloadSeq(event: TimelineEvent, key: string): number {
    const payload = toRecord(event.payload);
    const value = payload[key];
    const parsed = typeof value === "number" ? value : Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function normalizeThinkingContentForDedupe(event: TimelineEvent): string {
    if (event.kind !== "assistant.thinking") {
      return "";
    }
    return String(event.payload?.content ?? "").replace(/\s+/g, " ").trim();
  }

  function filterSupersededHistoryThinkingEvents(events: TimelineEvent[]): TimelineEvent[] {
    const canonicalHistoryThinking = new Set<string>();

    for (const event of events) {
      if (event.kind !== "assistant.thinking") continue;
      const historyMessageSeq = readHistoryMessageSeq(event);
      const content = normalizeThinkingContentForDedupe(event);
      if (!historyMessageSeq || !content) continue;
      canonicalHistoryThinking.add(`${event.sessionId}:${historyMessageSeq}:${content}`);
    }

    if (canonicalHistoryThinking.size === 0) {
      return events;
    }

    return events.filter((event) => {
      if (event.kind !== "assistant.thinking") return true;
      if (readHistoryMessageSeq(event)) return true;

      const content = normalizeThinkingContentForDedupe(event);
      if (!content) return true;

      const messageSeq =
        readTimelinePayloadSeq(event, "messageSeq") ||
        readTimelinePayloadSeq(event, "message_seq") ||
        readTimelinePayloadSeq(event, "rawSeq");
      if (!messageSeq) return true;

      return !canonicalHistoryThinking.has(`${event.sessionId}:${messageSeq}:${content}`);
    });
  }

  function getDefaultOpenClawSegmentType(kind: string): OpenClawTimelineV2SegmentType {
    if (kind === "assistant.delta" || kind === "assistant.message") return "answer";
    if (kind === "assistant.thinking") return "thinking";
    if (kind === "tool.call") return "tool_call";
    if (kind === "tool.result") return "tool_result";
    return "run";
  }

  function getDefaultOpenClawSegmentId(
    event: GatewayEvent,
    turnId: string,
    segmentType: OpenClawTimelineV2SegmentType
  ) {
    if (segmentType === "answer") {
      return `${turnId}:answer:0`;
    }
    if (segmentType === "tool_call" || segmentType === "tool_result") {
      const payload = toRecord(event.payload);
      const data = toRecord(payload.data);
      const toolIdentity = stringOr(
        data.toolCallId,
        data.tool_call_id,
        data.callId,
        data.call_id,
        data.id,
        payload.toolCallId,
        payload.tool_call_id,
        payload.callId,
        payload.call_id,
        payload.id
      );
      if (toolIdentity) {
        return `${turnId}:${segmentType}:${toolIdentity}`;
      }
      const toolName = stringOr(data.name, payload.name, data.toolName, payload.toolName, "tool");
      const eventIdentity = identityStringOr(
        data.rawSeq,
        data.raw_seq,
        payload.rawSeq,
        payload.raw_seq,
        data.seq,
        payload.seq,
        event.seq,
        event.id
      );
      return `${turnId}:${segmentType}:${toolName}:${eventIdentity || randomUUID()}`;
    }
    return `${turnId}:${segmentType}:${event.seq ?? event.id ?? randomUUID()}`;
  }

  function getDefaultOpenClawOperation(kind: string): OpenClawTimelineV2Operation {
    if (kind === "assistant.delta") return "append";
    if (kind === "run.completed" || kind === "run.failed" || kind === "run.interrupted") return "close";
    return "replace";
  }

  function getDefaultOpenClawVisibility(kind: string): OpenClawTimelineV2Visibility {
    if (kind === "assistant.delta") return "hidden";
    return "final";
  }

  function getOpenClawSegmentIndex(segmentId: string, eventScope: GatewayEventScope) {
    const existing = eventScope.segmentIndexById.get(segmentId);
    if (typeof existing === "number") {
      return existing;
    }
    const next = eventScope.nextSegmentIndex;
    eventScope.segmentIndexById.set(segmentId, next);
    eventScope.nextSegmentIndex += 1;
    return next;
  }

  function getNextOpenClawDeltaIndex(segmentId: string, eventScope: GatewayEventScope) {
    const next = eventScope.nextDeltaIndexBySegment.get(segmentId) ?? 0;
    eventScope.nextDeltaIndexBySegment.set(segmentId, next + 1);
    return next;
  }

  async function sendOutputFilesForEvent(
    reqId: string,
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ) {
    const files = extractOutputFilesFromPayload(event.payload);
    await sendOutputFiles(reqId, sessionId, files, eventScope);
  }

  async function sendCreatedLocalOutputFiles(
    reqId: string,
    sessionId: string,
    eventScope: GatewayEventScope
  ) {
    if (eventScope.localOutputFilesSent) {
      return;
    }
    const files = await collectCreatedLocalOutputFiles(eventScope.localOutputSnapshot, {
      config: input.config,
      configPath: input.configPath,
      stateDir: input.stateDir,
      logger: input.logger
    });
    const referencedFiles = await collectReferencedLocalOutputFiles(
      eventScope.referencedLocalOutputPaths,
      eventScope.localOutputSnapshot,
      {
        config: input.config,
        configPath: input.configPath,
        stateDir: input.stateDir,
        logger: input.logger
      }
    );
    const recentReferencedPaths = [...eventScope.referencedLocalOutputPaths].filter(
      (path) => !eventScope.localOutputSnapshot?.files.has(path)
    );
    const recentReferencedFiles = await collectRecentReferencedLocalOutputFiles(
      recentReferencedPaths,
      {
        config: input.config,
        configPath: input.configPath,
        stateDir: input.stateDir,
        logger: input.logger
      },
      eventScope.eventBoundaryMs
    );
    const outputFiles = [...files, ...referencedFiles, ...recentReferencedFiles];
    input.logger?.info?.(
      `[53aihub] local output scan: changed=${files.length}, referenced=${referencedFiles.length}, recentReferenced=${recentReferencedFiles.length}, refs=${eventScope.referencedLocalOutputPaths.size}, files=${
        outputFiles.map((file) => file.file_name).join(",") || "none"
      }`
    );
    const sent = await sendOutputFiles(reqId, sessionId, outputFiles, eventScope);
    if (sent) {
      eventScope.localOutputFilesSent = true;
    }
  }

  async function sendOutputFiles(
    reqId: string,
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope
  ): Promise<boolean> {
    if (files.length === 0) {
      return false;
    }

    const freshFiles = files.filter((file) => {
      const keys = getOutputFileKeys(file);
      if (keys.some((key) => eventScope.emittedOutputFileKeys.has(key))) {
        return false;
      }
      for (const key of keys) {
        eventScope.emittedOutputFileKeys.add(key);
      }
      return true;
    });
    if (freshFiles.length === 0) {
      return false;
    }

    await sendQueuedFrame(
      buildOutputFilesProcessStep(
        reqId,
        freshFiles,
        sessionId,
        buildOpenClawOutputFilesTimelineMeta(eventScope, freshFiles)
      )
    );
    return true;
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
    await mkdir(dirname(statePath), { recursive: true });
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

function isOpenClawDuplicateTraceEnabled(): boolean {
  const value = String(process.env.OPENCLAW_TRACE_DUPLICATES ?? process.env.OPENCLAW_DIAG_LOGS ?? "").toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

function traceOpenClawDuplicate(
  logger: HubBridgeInput["logger"] | undefined,
  label: string,
  payload: Record<string, unknown>
): void {
  if (!isOpenClawDuplicateTraceEnabled()) {
    return;
  }
  const line = `[openclaw-dup-trace] ${label} ${safeTraceJson(payload)}`;
  if (logger?.info) {
    logger.info(line);
    return;
  }
  console.info(line);
}

function summarizeTimelineEventForTrace(event: TimelineEvent): Record<string, unknown> {
  const payload = toRecord(event.payload);
  const timeline = toRecord(payload.openclaw_timeline);
  const content = typeof payload.content === "string" ? payload.content : "";
  return {
    id: event.id,
    kind: event.kind,
    seq: event.seq,
    rawSeq: payload.rawSeq,
    messageSeq: payload.message_seq ?? payload.messageSeq,
    runId: payload.runId,
    state: payload.state,
    mode: payload.mode,
    replace: payload.replace,
    segmentId: stringOr(timeline.segment_id, payload.segment_id),
    segmentType: stringOr(timeline.segment_type, payload.segment_type),
    deltaIndex: timeline.delta_index ?? payload.delta_index,
    visibility: timeline.visibility ?? payload.visibility,
    final: timeline.final ?? payload.final,
    contentLength: content.length,
    contentHash: hashTraceText(content)
  };
}

function summarizeOutgoingFrameForTrace(frame: Hub53AIOutgoingFrame): Record<string, unknown> {
  if ("data" in frame && toRecord(frame.data).object === "chat.completion.chunk") {
    const data = toRecord(frame.data);
    const choice = toRecord((Array.isArray(data.choices) ? data.choices : [])[0]);
    const delta = toRecord(choice.delta);
    const payload = toRecord(data.payload);
    const timeline = toRecord(payload.openclaw_timeline);
    const content = typeof delta.content === "string" ? delta.content : "";
    return {
      reqId: frame.req_id,
      status: frame.status,
      eventKind: data.event_kind,
      sessionId: data.session_id,
      finishReason: choice.finish_reason,
      mode: data.mode,
      replace: data.replace,
      payloadSeq: payload.seq,
      rawSeq: payload.rawSeq,
      runId: payload.runId,
      segmentId: stringOr(timeline.segment_id, payload.segment_id),
      segmentType: stringOr(timeline.segment_type, payload.segment_type),
      deltaIndex: timeline.delta_index ?? payload.delta_index,
      contentLength: content.length,
      contentHash: hashTraceText(content)
    };
  }
  return {
    reqId: frame.req_id,
    action: frame.action,
    status: frame.status
  };
}

function hashTraceText(text: string): string {
  return text ? createHash("sha1").update(text).digest("hex").slice(0, 12) : "";
}

function safeTraceJson(payload: Record<string, unknown>): string {
  try {
    return JSON.stringify(payload);
  } catch {
    return "{}";
  }
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
        conversationTitle: extractConversationTitle(openAIReq, metadata),
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
      conversationTitle: extractConversationTitle(data),
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

function extractConversationTitle(...sources: unknown[]): string | undefined {
  const titleKeys = [
    "openclaw_conversation_title",
    "openclawConversationTitle",
    "conversation_title",
    "conversationTitle",
    "title"
  ];
  for (const source of sources) {
    const record = toRecord(source);
    const title = stringFromKeys(record, titleKeys);
    if (title) {
      return title;
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

function isHubManagedSessionTitle(title: string, chatId: string): boolean {
  const normalized = title.trim();
  return normalized.startsWith(HUB_SESSION_TITLE_PREFIX) || isOldHubPlaceholderTitle(normalized, chatId);
}

function isRestorableHubSession(session: GatewaySession, chatId: string, userName?: string): boolean {
  const normalized = session.title.trim();
  if (isOldHubPlaceholderTitle(normalized, chatId)) {
    return true;
  }
  if (!normalized.startsWith(HUB_SESSION_TITLE_PREFIX) || !userName) {
    return false;
  }
  return normalized.startsWith(`${HUB_SESSION_TITLE_PREFIX}${sanitizeTitlePart(userName)}：`);
}

function mergeKnownHubSessions(gatewaySessions: GatewaySession[], knownSessions: GatewaySession[]): GatewaySession[] {
  const knownHubById = new Map(
    knownSessions.filter((session) => isHubTitle(session.title)).map((session) => [session.id, session])
  );
  if (!knownHubById.size) {
    return gatewaySessions;
  }

  const merged = gatewaySessions.map((session) => {
    const knownSession = knownHubById.get(session.id);
    if (!knownSession || !shouldPreferKnownHubTitle(session.title, knownSession.title)) {
      return session;
    }
    return {
      ...session,
      title: knownSession.title
    };
  });
  return merged;
}

function applyHubTitleHint(session: GatewaySession, titleHint?: string): GatewaySession {
  const normalizedTitle = titleHint?.trim();
  if (!normalizedTitle || !isHubTitle(normalizedTitle)) {
    return session;
  }
  return {
    ...session,
    title: normalizedTitle
  };
}

function shouldPreferKnownHubTitle(gatewayTitle: string, knownTitle: string): boolean {
  return isHubTitle(knownTitle) && (isControlCenterTitle(gatewayTitle) || !isHubTitle(gatewayTitle));
}

function isHubTitle(title: string): boolean {
  return title.trim().startsWith(HUB_SESSION_TITLE_PREFIX);
}

function isControlCenterTitle(title: string): boolean {
  return title.trim() === CONTROL_CENTER_SESSION_TITLE;
}

function toTime(value?: string): number {
  if (!value) return 0;
  const time = Date.parse(value);
  return Number.isFinite(time) ? time : 0;
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

function buildOutputFilesProcessStep(
  reqId: string,
  files: Hub53AIOutputFile[],
  sessionId?: string,
  timeline?: OpenClawTimelineV2Meta
): Hub53AIOutgoingProcessStep {
  const mediaAttachments = files.map((file) => ({
    ...file,
    kind: resolveMediaKind(file.mime_type, file.file_name)
  }));
  return {
    req_id: reqId,
    action: "chat",
    status: "streaming",
    data: {
      id: `${reqId}-output-files-${Date.now()}`,
      object: "process.step",
      created: Math.floor(Date.now() / 1000),
      model: "openclaw-agent",
      status: "streaming",
      ...(sessionId ? { session_id: sessionId, conversation_id: sessionId } : {}),
      process_step: {
        step_code: "output_files",
        name: "生成文件",
        status: "completed",
        message: `生成了 ${files.length} 个文件`,
        data: {
          files,
          contract_version: "v1",
          ...(timeline ? { openclaw_timeline: timeline } : {}),
          media_attachments: mediaAttachments,
          media_contract_version: "v1"
        },
        timestamp: Math.floor(Date.now() / 1000)
      }
    }
  };
}

function extractOutputFilesFromPayload(payload: unknown): Hub53AIOutputFile[] {
  const candidates = collectOutputFileCandidates(payload);
  const seen = new Set<string>();
  const files: Hub53AIOutputFile[] = [];
  for (const candidate of candidates) {
    const file = normalizeOutputFile(candidate);
    if (!file) {
      continue;
    }
    const key = getOutputFileKeys(file)[0] ?? "";
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    files.push(file);
  }
  return files;
}

function collectOutputFileCandidates(value: unknown, depth = 0): unknown[] {
  if (value == null || depth > 4) {
    return [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((entry) => collectOutputFileCandidates(entry, depth + 1));
  }
  if (typeof value !== "object") {
    return [];
  }

  const record = value as Record<string, unknown>;
  if (looksLikeOutputFile(record)) {
    return [record];
  }

  const candidates: unknown[] = [];
  for (const key of [
    "output_files",
    "outputFiles",
    "files",
    "file",
    "attachments",
    "attachment",
    "artifacts",
    "artifact",
    "media",
    "media_attachments",
    "generated_files",
    "generatedFiles"
  ]) {
    const nested = record[key];
    if (nested == null) {
      continue;
    }
    if (Array.isArray(nested)) {
      candidates.push(...nested);
    } else {
      candidates.push(nested);
    }
  }

  for (const key of ["data", "result", "payload", "message", "content", "output"]) {
    candidates.push(...collectOutputFileCandidates(record[key], depth + 1));
  }

  return candidates;
}

function looksLikeOutputFile(record: Record<string, unknown>): boolean {
  const url = readFirstString(record, ["url", "href", "download_url", "downloadUrl", "file_url", "fileUrl", "signed_url"]);
  const base64 = readFirstString(record, ["base64"]);
  const content = readFirstString(record, ["content", "data"]);
  const explicitFileName = readFirstString(record, ["file_name", "fileName", "filename", "path", "title"]);
  const displayName = explicitFileName || readFirstString(record, ["name"]);
  const type = readFirstString(record, ["type", "kind", "mime_type", "mimeType", "mime"]);
  const fileLikeType = ["file", "image", "audio", "video", "attachment", "artifact", "media"].includes(type) || type.includes("/");
  if (url || base64) {
    return Boolean(displayName || fileLikeType);
  }
  return Boolean(content && (explicitFileName || fileLikeType));
}

function normalizeOutputFile(value: unknown): Hub53AIOutputFile | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const record = value as Record<string, unknown>;
  const url = readFirstString(record, ["url", "href", "download_url", "downloadUrl", "file_url", "fileUrl", "signed_url"]);
  const base64 = readFirstString(record, ["base64"]);
  const content = readFirstString(record, ["content", "data"]);
  if (!url && !base64 && !content) {
    return null;
  }

  const fileName =
    readFirstString(record, ["file_name", "fileName", "filename", "name", "path", "title"]) ||
    inferFileNameFromUrl(url) ||
    "file";
  const id = readFirstString(record, ["id", "file_id", "fileId", "upload_file_id", "uploadFileId"]) || url || fileName;
  const mimeType = readFirstString(record, ["mime_type", "mimeType", "mime", "content_type", "contentType"]);
  const size = readFirstNumber(record, ["size", "file_size", "fileSize", "bytes"]);
  return {
    id,
    file_name: fileName,
    ...(url ? { url } : {}),
    ...(readFirstString(record, ["download_url", "downloadUrl"]) ? { download_url: readFirstString(record, ["download_url", "downloadUrl"]) } : {}),
    ...(readFirstString(record, ["signed_download_url", "signedDownloadUrl"]) ? { signed_download_url: readFirstString(record, ["signed_download_url", "signedDownloadUrl"]) } : {}),
    ...(mimeType ? { mime_type: mimeType } : {}),
    ...(typeof size === "number" ? { size } : {}),
    ...(base64 ? { base64 } : {}),
    ...(content && !base64 ? { content } : {}),
    ...(readFirstString(record, ["message_id", "messageId"]) ? { message_id: readFirstString(record, ["message_id", "messageId"]) } : {}),
    ...(readFirstString(record, ["source_kind", "sourceKind"]) ? { source_kind: readFirstString(record, ["source_kind", "sourceKind"]) } : {})
  };
}

function getOutputFileKeys(file: Hub53AIOutputFile): string[] {
  return [
    file.id ? `id:${file.id}` : "",
    file.url || file.file_name ? `url-name:${file.url ?? ""}|${file.file_name}` : "",
    file.file_name ? `name:${file.file_name}` : ""
  ].filter(Boolean);
}

function readFirstString(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function readFirstNumber(record: Record<string, unknown>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim()) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return undefined;
}

function inferFileNameFromUrl(url?: string): string {
  if (!url) {
    return "";
  }
  try {
    const parsed = new URL(url);
    const last = parsed.pathname.split("/").filter(Boolean).at(-1);
    return last ? decodeURIComponent(last) : "";
  } catch {
    const last = url.split("?")[0]?.split("/").filter(Boolean).at(-1);
    return last ? decodeURIComponent(last) : "";
  }
}

function resolveMediaKind(mimeType = "", fileName = ""): Hub53AIMediaAttachment["kind"] {
  const lower = mimeType.toLowerCase();
  if (lower.startsWith("image/")) return "image";
  if (lower.startsWith("audio/")) return "audio";
  if (lower.startsWith("video/")) return "video";
  if (lower.startsWith("text/")) return "text";
  const ext = fileName.split(".").pop()?.toLowerCase() ?? "";
  if (["png", "jpg", "jpeg", "gif", "webp", "svg"].includes(ext)) return "image";
  if (["mp3", "wav", "m4a", "ogg"].includes(ext)) return "audio";
  if (["mp4", "mov", "webm"].includes(ext)) return "video";
  if (["txt", "md", "csv", "json", "log"].includes(ext)) return "text";
  return "file";
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
  const data = toRecord(event.payload?.data);
  const name = String(
    event.payload?.name ??
      event.payload?.toolName ??
      event.payload?.skillName ??
      data.name ??
      data.toolName ??
      data.skillName ??
      ""
  ).trim();
  if (event.kind === "tool.call") {
    return name ? `Used tool ${name}` : "Used a tool";
  }
  if (event.kind === "tool.result") {
    return name ? `Tool ${name} returned a result` : "Tool returned a result";
  }
  const message = String(event.payload?.message ?? event.payload?.status ?? "").trim();
  return message || null;
}

function isSyntheticToolPlaceholderThinkingEvent(event: TimelineEvent): boolean {
  if (event.kind !== "assistant.thinking") {
    return false;
  }
  const content = String(event.payload?.content ?? "").replace(/\s+/g, " ").trim();
  if (!content) {
    return false;
  }
  const normalized = content.toLowerCase();
  if (normalized === "used a tool" || normalized === "tool returned a result") {
    return true;
  }
  return /^used tool\b/i.test(content) || /^tool .+ returned a result$/i.test(content);
}

function filterSyntheticToolPlaceholderThinkingEvents(events: TimelineEvent[]): TimelineEvent[] {
  return events.filter((event) => !isSyntheticToolPlaceholderThinkingEvent(event));
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

function identityStringOr(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return String(value);
    }
  }
  return "";
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}
