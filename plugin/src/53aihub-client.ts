import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

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
  diagnosticLogs?: boolean;
  ledgerDebug?: boolean;
  duplicateTrace?: boolean;
  debug?: {
    all?: boolean;
    ledger?: boolean;
    duplicates?: boolean;
  };
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
  clientMessageId?: string;
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
        content?: string;
        reasoning_content?: string;
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

type OpenClawLedgerPartType = "answer" | "thinking" | "tool" | "output_file" | "status";
type OpenClawLedgerEventType =
  | "turn.started"
  | "part.delta"
  | "part.replace"
  | "part.done"
  | "turn.completed"
  | "turn.interrupted"
  | "turn.failed";
type OpenClawLedgerOperation = "append" | "replace" | "close" | "noop";
type OpenClawLedgerTerminalStatus = "running" | "completed" | "interrupted" | "failed" | "cancelled";

type OpenClawLedgerEvent = {
  protocol_version: "openclaw.ledger.v1";
  seq: number;
  session_id: string;
  conversation_id: string;
  turn_id: string;
  run_id?: string;
  active_request_id: string;
  part_id: string;
  part_type: OpenClawLedgerPartType;
  event_type: OpenClawLedgerEventType;
  operation: OpenClawLedgerOperation;
  visibility: OpenClawTimelineV2Visibility;
  text?: string;
  payload?: Record<string, unknown>;
  terminal_status?: OpenClawLedgerTerminalStatus;
  created_at: string;
  raw_event_ref?: string;
};

type OpenClawLedgerTurnSnapshot = {
  turn_id: string;
  run_id?: string;
  active_request_id: string;
  status: OpenClawLedgerTerminalStatus;
  terminal_seq?: number;
  last_seq: number;
  part_ids: string[];
};

type OpenClawLedgerTurnStats = {
  last_created_at_ms: number;
  has_visible_part: boolean;
};

type TrackedActiveOpenClawTurns = {
  turnIds: Set<string>;
  runIds: Set<string>;
  requestIdByTurnId: Map<string, string>;
  requestIdByRunId: Map<string, string>;
};

type OpenClawSessionSnapshot = {
  session_id: string;
  conversation_id: string;
  last_seq: number;
  active_turns: OpenClawLedgerTurnSnapshot[];
  recent_events: OpenClawLedgerEvent[];
  ledger_events?: OpenClawLedgerEvent[];
  ledgerEvents?: OpenClawLedgerEvent[];
};

type OpenClawRunFailureClassification = {
  code: string;
  reason: string;
  userMessage: string;
  rawMessage?: string;
  provider?: string;
  model?: string;
  runtimeMs?: number;
  authRelated: boolean;
  confidence: "high" | "medium" | "low";
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
  canonicalEventsBySession?: Record<string, TimelineEvent[]>;
  syntheticEventsBySession?: Record<string, TimelineEvent[]>;
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
const MAX_OUTBOX_FRAMES = 200;
const MAX_SYNTHETIC_EVENTS_PER_SESSION = 200;
const MAX_CANONICAL_EVENTS_PER_SESSION = 500;
const OPENCLAW_ORPHAN_RUNNING_TURN_TERMINAL_MS = 30_000;
const RUN_WAIT_TIMEOUT_MS = 30 * 60 * 1000;
const HUB_SESSION_TITLE_PREFIX = "53AI Hub-";
const CONTROL_CENTER_SESSION_TITLE = "Claw Control Center";
const HUB_TITLE_SUMMARY_LENGTH = 40;
const HUB_RPC_ACTIONS = new Set([
  "sessions.list",
  "sessions.current",
  "sessions.messages",
  "sessions.events",
  "sessions.snapshot",
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
  const activeRequestDetailsBySession = new Map<string, Map<string, ActiveSessionRequest>>();
  const syntheticEventsBySession = new Map<string, TimelineEvent[]>();
  const canonicalEventsBySession = new Map<string, TimelineEvent[]>();
  const ledgerSeqBySession = new Map<string, number>();
  let persistStateQueue: Promise<void> = Promise.resolve();

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
    await persistState({ force: true });
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
      const ledgerEvents = listCanonicalLedgerEvents(sessionId);
      return {
        messages: pageMessages,
        events,
        ledger_events: ledgerEvents,
        ledgerEvents,
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
      const ledgerEvents = listCanonicalLedgerEvents(sessionId, afterSeq);
      return {
        events: pageEvents,
        ledger_events: ledgerEvents,
        ledgerEvents,
        pagination: buildPagination(pagination.limit, pagination.offset, events.length, pageEvents.length)
      };
    }

    if (request.action === "sessions.snapshot") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      const afterSeq = readRPCAfterSeq(payload);
      await ensureCanonicalLedgerBackfill(sessionId);
      return buildOpenClawSessionSnapshot(sessionId, listCanonicalLedgerEvents(sessionId), afterSeq);
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
      await resolveActiveSessionRequests(sessionId, "control.stop");
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
    const rawEvents = filterSyntheticToolPlaceholderThinkingEvents(
      filterSupersededHistoryThinkingEvents(
        dedupeTimelineEvents([...gatewayEvents, ...storedEvents])
          .map(normalizeTimelineEventSegmentType)
          .map(normalizeTimelineEventMessageSeq)
      )
    );
    await ensureCanonicalLedgerBackfillFromEvents(sessionId, rawEvents);
    const canonicalEvents = listCanonicalSessionEvents(sessionId);
    const deduped = dedupeTimelineEvents([...rawEvents, ...canonicalEvents]);
    traceOpenClawDuplicate(input.logger, "hub.events.list", {
      sessionId,
      gatewayCount: gatewayEvents.length,
      canonicalCount: canonicalEvents.length,
      storedCount: storedEvents.length,
      syntheticCount: syntheticEventsBySession.get(sessionId)?.length ?? 0,
      dedupedCount: deduped.length,
      gatewayTail: gatewayEvents.slice(-6).map(summarizeTimelineEventForTrace),
      canonicalTail: canonicalEvents.slice(-6).map(summarizeTimelineEventForTrace),
      storedTail: storedEvents.slice(-6).map(summarizeTimelineEventForTrace),
      syntheticTail: (syntheticEventsBySession.get(sessionId) ?? []).slice(-6).map(summarizeTimelineEventForTrace),
      dedupedTail: deduped.slice(-6).map(summarizeTimelineEventForTrace)
    }, input.config);
    return deduped;
  }

  async function ensureCanonicalLedgerBackfill(sessionId: string) {
    if (!sessionId) {
      return;
    }
    const [gatewayEvents, storedEvents] = await Promise.all([
      input.gateway.listEvents(sessionId, 0),
      input.callbacks.listSessionEvents?.(sessionId) ?? []
    ]);
    const rawEvents = filterSyntheticToolPlaceholderThinkingEvents(
      filterSupersededHistoryThinkingEvents(
        dedupeTimelineEvents([...gatewayEvents, ...storedEvents])
          .map(normalizeTimelineEventSegmentType)
          .map(normalizeTimelineEventMessageSeq)
      )
    );
    await ensureCanonicalLedgerBackfillFromEvents(sessionId, rawEvents);
  }

  async function ensureCanonicalLedgerBackfillFromEvents(sessionId: string, events: TimelineEvent[]) {
    if (!sessionId || !events.length) {
      return;
    }

    const initialCanonicalEvents = listCanonicalSessionEvents(sessionId);
    const liveCompletedRunIds = collectOpenClawLiveCompletedRunIds(initialCanonicalEvents);
    const turnGroups = collectCompletedHistoryLedgerBackfillGroups(sessionId, events).filter(
      (group) => !group.runId || !liveCompletedRunIds.has(group.runId)
    );
    const expectedBackfillRefs = buildExpectedHistoryBackfillRefs(sessionId, turnGroups);
    const rewrittenExisting = pruneCanonicalHistoryBackfillEvents(sessionId, expectedBackfillRefs, liveCompletedRunIds);
    const existingCanonicalEvents = listCanonicalSessionEvents(sessionId);
    const existingEventIds = new Set(existingCanonicalEvents.map((event) => event.id).filter(Boolean));
    const existingRawRefs = new Set(
      extractOpenClawLedgerEvents(existingCanonicalEvents)
        .map((event) => event.raw_event_ref)
        .filter((ref): ref is string => Boolean(ref))
    );

    let appended = 0;
    let skippedExisting = 0;
    let skippedIncomplete = 0;

    for (const group of turnGroups) {
      if (!group.terminalSeen) {
        skippedIncomplete += group.events.length;
        continue;
      }
      const identity = group.runId || `history:${group.firstSeq || group.events[0]?.id || "turn"}`;
      const eventScope = createHistoryLedgerBackfillScope(sessionId, identity);
      if (group.runId) {
        eventScope.currentRunId = group.runId;
      }

      for (const event of group.events) {
        if (!shouldAttachOpenClawTimeline(event)) {
          continue;
        }
        const rawEventRef = buildOpenClawRawEventRef(event);
        if ((event.id && existingEventIds.has(event.id)) || existingRawRefs.has(rawEventRef)) {
          skippedExisting += 1;
          continue;
        }
        applyOpenClawEventScopeActivity(event, eventScope);
        augmentPayloadWithEventMeta(event, eventScope);
        appendWriteToolOutputFilesForHistoryEvent(sessionId, event, eventScope);
        appended += 1;
        if (event.id) {
          existingEventIds.add(event.id);
        }
        existingRawRefs.add(rawEventRef);
      }
    }

    if (appended || skippedIncomplete || rewrittenExisting) {
      traceOpenClawLedger(input.logger, "history-backfill", {
        sessionId,
        groupCount: turnGroups.length,
        appended,
        rewrittenExisting,
        skippedExisting,
        skippedIncomplete,
        skippedCompletedLiveRuns: liveCompletedRunIds.size
      }, input.config);
    }
  }

  type HistoryBackfillExpectedRef = {
    turnId: string;
    activeRequestId: string;
    runId?: string;
  };

  function buildExpectedHistoryBackfillRefs(
    sessionId: string,
    groups: HistoryLedgerBackfillGroup[]
  ): Map<string, HistoryBackfillExpectedRef> {
    const refs = new Map<string, HistoryBackfillExpectedRef>();
    for (const group of groups) {
      if (!group.terminalSeen) {
        continue;
      }
      const identity = group.runId || `history:${group.firstSeq || group.events[0]?.id || "turn"}`;
      const activeRequestId = `history:${identity}`;
      const expected: HistoryBackfillExpectedRef = {
        turnId: buildOpenClawTimelineTurnId(sessionId, activeRequestId),
        activeRequestId,
        ...(group.runId ? { runId: group.runId } : {})
      };
      for (const event of group.events) {
        if (shouldAttachOpenClawTimeline(event)) {
          refs.set(buildOpenClawRawEventRef(event), expected);
        }
      }
    }
    return refs;
  }

  function pruneCanonicalHistoryBackfillEvents(
    sessionId: string,
    expectedRefs: Map<string, HistoryBackfillExpectedRef>,
    skipRunIds: Set<string> = new Set()
  ): number {
    if (!sessionId || (expectedRefs.size === 0 && skipRunIds.size === 0)) {
      return 0;
    }

    const events = canonicalEventsBySession.get(sessionId) ?? [];
    if (events.length === 0) {
      return 0;
    }

    const nextEvents = events.filter((event) => {
      const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
      if (!ledger || !ledger.active_request_id.startsWith("history:")) {
        return true;
      }
      if (ledger.run_id && skipRunIds.has(ledger.run_id)) {
        return false;
      }
      const expected = ledger.raw_event_ref ? expectedRefs.get(ledger.raw_event_ref) : undefined;
      if (!expected) {
        return false;
      }
      return (
        ledger.turn_id === expected.turnId &&
        ledger.active_request_id === expected.activeRequestId &&
        (!expected.runId || ledger.run_id === expected.runId)
      );
    });
    const removed = events.length - nextEvents.length;
    if (removed <= 0) {
      return 0;
    }

    canonicalEventsBySession.set(sessionId, nextEvents);
    setPersistedSessionEvents("canonicalEventsBySession", sessionId, nextEvents, MAX_CANONICAL_EVENTS_PER_SESSION);
    return removed;
  }

  type HistoryLedgerBackfillGroup = {
    runId: string;
    firstSeq: number;
    lastSeq: number;
    terminalSeen: boolean;
    messageSeqs: Set<number>;
    events: TimelineEvent[];
  };

  function collectCompletedHistoryLedgerBackfillGroups(
    sessionId: string,
    events: TimelineEvent[]
  ): HistoryLedgerBackfillGroup[] {
    const groups: HistoryLedgerBackfillGroup[] = [];
    const groupByMessageSeq = new Map<string, HistoryLedgerBackfillGroup>();
    const deferredHistoryEvents: TimelineEvent[] = [];
    let current: HistoryLedgerBackfillGroup | null = null;

    const flush = () => {
      if (current && current.events.length) {
        groups.push(current);
      }
      current = null;
    };

    const addMessageSeqMapping = (group: HistoryLedgerBackfillGroup, event: TimelineEvent) => {
      for (const messageSeq of readBackfillAssistantMessageSeqs(event)) {
        group.messageSeqs.add(messageSeq);
        groupByMessageSeq.set(buildHistoryBackfillMessageSeqKey(group.runId, messageSeq), group);
        if (!group.runId) {
          groupByMessageSeq.set(buildHistoryBackfillMessageSeqKey("", messageSeq), group);
        }
      }
    };

    const sortedEvents = events
      .filter((event) => event.sessionId === sessionId)
      .sort((left, right) => (left.seq || 0) - (right.seq || 0));

    for (const event of sortedEvents) {
      const runId = getGatewayEventRunIdentity(event);
      if (event.kind === "run.started") {
        flush();
        current = createHistoryLedgerBackfillGroup(event, runId);
      } else if (!current && (runId || shouldAttachOpenClawTimeline(event))) {
        current = createHistoryLedgerBackfillGroup(event, runId);
      } else if (runId && current?.runId && current.runId !== runId) {
        flush();
        current = createHistoryLedgerBackfillGroup(event, runId);
      } else if (runId && current && !current.runId) {
        current.runId = runId;
      }

      if (!current) {
        continue;
      }

      if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
        current.firstSeq = current.firstSeq > 0 ? Math.min(current.firstSeq, event.seq) : event.seq;
        current.lastSeq = Math.max(current.lastSeq, event.seq);
      }
      addMessageSeqMapping(current, event);

      if (shouldAttachOpenClawTimeline(event)) {
        if (readHistoryMessageSeq(event)) {
          deferredHistoryEvents.push(event);
        } else {
          current.events.push(event);
        }
      }

      if (isTerminalRunEvent(event)) {
        current.terminalSeen = true;
        flush();
      }
    }

    flush();
    for (const event of deferredHistoryEvents) {
      const targetGroup = findHistoryBackfillGroupForEvent(groups, groupByMessageSeq, event);
      if (!targetGroup) {
        continue;
      }
      targetGroup.events.push(event);
      if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
        targetGroup.firstSeq = targetGroup.firstSeq > 0 ? Math.min(targetGroup.firstSeq, event.seq) : event.seq;
        targetGroup.lastSeq = Math.max(targetGroup.lastSeq, event.seq);
      }
    }

    for (const group of groups) {
      group.events.sort((left, right) => (left.seq || 0) - (right.seq || 0));
    }

    return groups;
  }

  function createHistoryLedgerBackfillGroup(event: TimelineEvent, runId: string): HistoryLedgerBackfillGroup {
    return {
      runId,
      firstSeq: typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : 0,
      lastSeq: typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : 0,
      terminalSeen: false,
      messageSeqs: new Set<number>(),
      events: []
    };
  }

  function findHistoryBackfillGroupForEvent(
    groups: HistoryLedgerBackfillGroup[],
    groupByMessageSeq: Map<string, HistoryLedgerBackfillGroup>,
    event: TimelineEvent
  ): HistoryLedgerBackfillGroup | undefined {
    const historyMessageSeq = readHistoryMessageSeq(event);
    if (historyMessageSeq) {
      const runId = getGatewayEventRunIdentity(event);
      const mappedGroup = runId
        ? groupByMessageSeq.get(buildHistoryBackfillMessageSeqKey(runId, historyMessageSeq))
        : groupByMessageSeq.get(buildHistoryBackfillMessageSeqKey("", historyMessageSeq));
      if (mappedGroup) {
        return mappedGroup;
      }
      if (runId) {
        return undefined;
      }
      const matchingGroups = groups.filter((group) => group.messageSeqs.has(historyMessageSeq));
      if (matchingGroups.length === 1) {
        return matchingGroups[0];
      }
    }

    const eventSeq = typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : 0;
    if (!eventSeq) {
      return undefined;
    }

    return groups.find((group) => {
      if (!group.terminalSeen || !group.firstSeq || !group.lastSeq) {
        return false;
      }
      const runId = getGatewayEventRunIdentity(event);
      if (runId && group.runId && group.runId !== runId) {
        return false;
      }
      return eventSeq >= group.firstSeq && eventSeq <= group.lastSeq;
    });
  }

  function buildHistoryBackfillMessageSeqKey(runId: string, messageSeq: number): string {
    return `${runId || "*"}:${messageSeq}`;
  }

  function readBackfillAssistantMessageSeqs(event: TimelineEvent): number[] {
    if (event.kind !== "status.update" || readHistoryMessageSeq(event)) {
      return [];
    }

    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const session = toRecord(payload.session);
    const phase = String(payload.phase ?? payload.status ?? data.phase ?? data.status ?? session.phase ?? session.status ?? "").toLowerCase();
    if (phase && phase !== "message" && phase !== "assistant_message" && phase !== "final_message") {
      return [];
    }

    return uniquePositiveNumbers(
      payload.messageSeq,
      payload.message_seq,
      data.messageSeq,
      data.message_seq,
      session.messageSeq,
      session.message_seq
    );
  }

  function uniquePositiveNumbers(...values: unknown[]): number[] {
    const output: number[] = [];
    const seen = new Set<number>();
    for (const value of values) {
      const parsed = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(parsed) || parsed <= 0 || seen.has(parsed)) {
        continue;
      }
      seen.add(parsed);
      output.push(parsed);
    }
    return output;
  }

  function firstPositiveNumber(...values: unknown[]): number {
    for (const value of values) {
      const parsed = typeof value === "number" ? value : Number(value);
      if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
      }
    }
    return 0;
  }

  function createHistoryLedgerBackfillScope(sessionId: string, identity: string): GatewayEventScope {
    const activeRequestId = `history:${identity}`;
    const turnId = buildOpenClawTimelineTurnId(sessionId, activeRequestId);
    return {
      eventBoundaryMs: 0,
      turnId,
      activeRequestId,
      currentTurnId: turnId,
      nextSegmentIndex: 0,
      nextAnswerSegmentIndex: 0,
      answerBoundaryAfterVisibleResponse: false,
      activityAppliedEventKeys: new Set<string>(),
      answerContentAppliedEventKeys: new Set<string>(),
      answerSegmentTextById: new Map<string, string>(),
      nextDeltaIndexBySegment: new Map<string, number>(),
      segmentIndexById: new Map<string, number>(),
      timelineMetaByEventKey: new Map<string, OpenClawTimelineV2Meta>(),
      currentActivitySeen: false,
      visibleResponseSeen: false,
      lastSeqSeen: 0,
      emittedOutputFileKeys: new Set<string>(),
      referencedLocalOutputPaths: new Set<string>(),
      writeOutputFilesByToolCallId: new Map<string, Hub53AIOutputFile[]>()
    };
  }

  function listCanonicalSessionEvents(sessionId: string): TimelineEvent[] {
    return dedupeTimelineEvents([
      ...(canonicalEventsBySession.get(sessionId) ?? []),
      ...(syntheticEventsBySession.get(sessionId) ?? [])
    ])
      .map(normalizeTimelineEventSegmentType)
      .map(normalizeTimelineEventMessageSeq);
  }

  function listCanonicalLedgerEvents(sessionId: string, afterSeq = 0): OpenClawLedgerEvent[] {
    const ledgerEvents = extractOpenClawLedgerEvents(listCanonicalSessionEvents(sessionId));
    const dedupedEvents = dedupeCanonicalLedgerEventsForExposure(ledgerEvents);
    if (dedupedEvents.length !== ledgerEvents.length) {
      traceOpenClawLedger(input.logger, "canonical-dedupe", {
        sessionId,
        before: ledgerEvents.length,
        after: dedupedEvents.length,
        removed: ledgerEvents.length - dedupedEvents.length
      }, input.config);
    }
    return dedupedEvents
      .filter((event) => event.seq > afterSeq)
      .sort((left, right) => left.seq - right.seq);
  }

  function dedupeCanonicalLedgerEventsForExposure(events: OpenClawLedgerEvent[]): OpenClawLedgerEvent[] {
    if (events.length <= 1) {
      return events;
    }

    const liveCompletedRunIds = collectOpenClawLiveCompletedRunIdsFromLedger(events);
    const scopedEvents = events.filter((event) => {
      if (!isOpenClawLedgerRunIdentityConsistent(event)) {
        return false;
      }
      if (event.run_id && liveCompletedRunIds.has(event.run_id) && isOpenClawHistoryLedgerEvent(event)) {
        return false;
      }
      return true;
    });
    const foldedEvents = foldSplitOpenClawAnswerEventsForExposure(scopedEvents);
    const selectedAnswers = new Map<string, OpenClawLedgerEvent>();
    const selectedOutputFiles = new Map<string, OpenClawLedgerEvent>();
    const duplicateAnswerRefs = new Set<string>();
    for (const event of [...foldedEvents].sort((left, right) => left.seq - right.seq)) {
      const answerIdentity = getOpenClawLedgerAnswerSemanticIdentity(event);
      if (answerIdentity) {
        const previous = selectedAnswers.get(answerIdentity);
        if (!previous) {
          selectedAnswers.set(answerIdentity, event);
        } else {
          const preferred = shouldPreferOpenClawLedgerAnswerEvent(event, previous) ? event : previous;
          const dropped = preferred === event ? previous : event;
          duplicateAnswerRefs.add(getOpenClawLedgerEventObjectRef(dropped));
          selectedAnswers.set(answerIdentity, preferred);
        }
      }

      const outputFileIdentity = getOpenClawLedgerOutputFileSemanticIdentity(event);
      if (outputFileIdentity) {
        const previous = selectedOutputFiles.get(outputFileIdentity);
        if (!previous) {
          selectedOutputFiles.set(outputFileIdentity, event);
        } else {
          const preferred = event.seq >= previous.seq ? event : previous;
          const dropped = preferred === event ? previous : event;
          duplicateAnswerRefs.add(getOpenClawLedgerEventObjectRef(dropped));
          selectedOutputFiles.set(outputFileIdentity, preferred);
        }
      }
    }

    if (duplicateAnswerRefs.size === 0 && foldedEvents.length === events.length && foldedEvents.every((event, index) => event === events[index])) {
      return events;
    }
    return foldedEvents.filter((event) => !duplicateAnswerRefs.has(getOpenClawLedgerEventObjectRef(event)));
  }

  function getOpenClawLedgerEventObjectRef(event: OpenClawLedgerEvent): string {
    return event.raw_event_ref || `${event.session_id}:${event.seq}:${event.turn_id}:${event.part_id}:${event.event_type}`;
  }

  function foldSplitOpenClawAnswerEventsForExposure(events: OpenClawLedgerEvent[]): OpenClawLedgerEvent[] {
    const splitAnswers = events.some((event) => event.part_type === "answer" && getOpenClawLedgerAnswerPartIndex(event.part_id) > 0);
    if (!splitAnswers) {
      return events;
    }

    const answerStateByRun = new Map<string, Map<number, string>>();
    const replacements = new Map<string, OpenClawLedgerEvent>();
    for (const event of [...events].sort((left, right) => left.seq - right.seq)) {
      if (event.part_type !== "answer") {
        continue;
      }
      const runIdentity = getOpenClawLedgerRunIdentity(event);
      if (!runIdentity) {
        continue;
      }
      const answerIndex = getOpenClawLedgerAnswerPartIndex(event.part_id);
      const text = readOpenClawLedgerAnswerText(event);
      const stateKey = `${event.session_id}:run:${runIdentity}`;
      const segmentTextByIndex = answerStateByRun.get(stateKey) ?? new Map<number, string>();
      answerStateByRun.set(stateKey, segmentTextByIndex);

      const priorText = joinOpenClawAnswerSegments(segmentTextByIndex, answerIndex - 1);
      const nextSegmentText =
        answerIndex > 0 && priorText && text.startsWith(priorText)
          ? text.slice(priorText.length).replace(/^\s+/, "")
          : text;
      const existingText = segmentTextByIndex.get(answerIndex) || "";
      segmentTextByIndex.set(answerIndex, event.operation === "append" ? `${existingText}${nextSegmentText}` : nextSegmentText);

      if (answerIndex <= 0) {
        continue;
      }

      const foldedText = joinOpenClawAnswerSegments(segmentTextByIndex, answerIndex);
      replacements.set(
        getOpenClawLedgerEventObjectRef(event),
        rewriteOpenClawLedgerAnswerEventForExposure(event, `${event.turn_id}:answer:0`, foldedText)
      );
    }

    if (replacements.size === 0) {
      return events;
    }
    return events.map((event) => replacements.get(getOpenClawLedgerEventObjectRef(event)) ?? event);
  }

  function rewriteOpenClawLedgerAnswerEventForExposure(
    event: OpenClawLedgerEvent,
    partId: string,
    text: string
  ): OpenClawLedgerEvent {
    const payload = toRecord(event.payload);
    const payloadWithText =
      typeof payload.content === "string"
        ? {
            ...payload,
            content: text,
            openclaw_folded_answer_segments: true,
            openclaw_original_part_id: event.part_id
          }
        : payload;
    return {
      ...event,
      part_id: partId,
      event_type: "part.replace",
      operation: "replace",
      text,
      ...(Object.keys(payloadWithText).length > 0 ? { payload: payloadWithText } : {})
    };
  }

  function readOpenClawLedgerAnswerText(event: OpenClawLedgerEvent): string {
    if (typeof event.text === "string") {
      return event.text;
    }
    const payload = toRecord(event.payload);
    return typeof payload.content === "string" ? payload.content : "";
  }

  function joinOpenClawAnswerSegments(segmentTextByIndex: Map<number, string>, maxIndex: number): string {
    if (maxIndex < 0) {
      return "";
    }
    const texts: string[] = [];
    for (let index = 0; index <= maxIndex; index += 1) {
      const text = segmentTextByIndex.get(index);
      if (text) {
        texts.push(text);
      }
    }
    return texts.join("");
  }

  function getOpenClawLedgerAnswerSemanticIdentity(event: OpenClawLedgerEvent): string {
    if (event.part_type !== "answer") {
      return "";
    }
    if (event.operation === "append") {
      return "";
    }
    const runIdentity = getOpenClawLedgerRunIdentity(event);
    if (!runIdentity) {
      return "";
    }
    return `${event.session_id}:run:${runIdentity}:answer`;
  }

  function getOpenClawLedgerOutputFileSemanticIdentity(event: OpenClawLedgerEvent): string {
    if (event.part_type !== "output_file") {
      return "";
    }
    const runIdentity = event.run_id || extractOpenClawHistoryIdentity(event.turn_id) || extractOpenClawHistoryIdentity(event.part_id);
    if (!runIdentity) {
      return "";
    }
    const files = extractOutputFilesFromPayload(event.payload);
    const fileKey = files
      .map((file) => getOutputFilePartIdentityKey(file))
      .filter(Boolean)
      .sort()
      .join(",");
    return `${event.session_id}:run:${runIdentity}:output_file:${fileKey || getOpenClawLedgerPartSemanticIndex(event.part_id, "output_file")}`;
  }

  function isOpenClawLedgerRunIdentityConsistent(event: OpenClawLedgerEvent): boolean {
    if (!event.run_id) {
      return true;
    }
    const historyIdentity = extractOpenClawHistoryIdentity(event.active_request_id) || extractOpenClawHistoryIdentity(event.turn_id);
    return !historyIdentity || historyIdentity === event.run_id;
  }

  function collectOpenClawLiveCompletedRunIds(events: TimelineEvent[]): Set<string> {
    return collectOpenClawLiveCompletedRunIdsFromLedger(extractOpenClawLedgerEvents(events));
  }

  function collectOpenClawLiveCompletedRunIdsFromLedger(events: OpenClawLedgerEvent[]): Set<string> {
    const runIds = new Set<string>();
    for (const event of events) {
      if (!event.run_id || isOpenClawHistoryLedgerEvent(event)) {
        continue;
      }
      if (event.terminal_status && event.terminal_status !== "running") {
        runIds.add(event.run_id);
      }
    }
    return runIds;
  }

  function extractOpenClawHistoryIdentity(value?: string): string {
    if (!value) {
      return "";
    }
    const match = value.match(/(?:^|:)history:([^:]+)/);
    return match?.[1] || "";
  }

  function getOpenClawLedgerRunIdentity(event: OpenClawLedgerEvent): string {
    return event.run_id || extractOpenClawHistoryIdentity(event.turn_id) || extractOpenClawHistoryIdentity(event.part_id);
  }

  function getOpenClawLedgerAnswerPartIndex(partId: string): number {
    const parsed = Number(getOpenClawLedgerPartSemanticIndex(partId, "answer"));
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function getOpenClawLedgerPartSemanticIndex(partId: string, partType: OpenClawLedgerPartType): string {
    const marker = `:${partType}:`;
    const markerIndex = partId.lastIndexOf(marker);
    if (markerIndex < 0) {
      return "0";
    }
    const suffix = partId.slice(markerIndex + marker.length);
    const index = suffix.split(":")[0]?.trim();
    return index || "0";
  }

  function shouldPreferOpenClawLedgerAnswerEvent(
    incoming: OpenClawLedgerEvent,
    previous: OpenClawLedgerEvent
  ): boolean {
    const incomingScore = scoreOpenClawLedgerAnswerEvent(incoming);
    const previousScore = scoreOpenClawLedgerAnswerEvent(previous);
    if (incomingScore !== previousScore) {
      return incomingScore > previousScore;
    }
    return incoming.seq >= previous.seq;
  }

  function scoreOpenClawLedgerAnswerEvent(event: OpenClawLedgerEvent): number {
    let score = 0;
    if (event.visibility === "final") score += 200;
    if (event.operation === "replace") score += 80;
    if (event.event_type === "part.replace") score += 40;
    if (readStringMetadata(event.payload, "source_kind") === "assistant.message") score += 30;
    if (!isOpenClawHistoryLedgerEvent(event)) score += 20;
    if (String(event.text ?? "").trim()) score += 5;
    return score;
  }

  function isOpenClawHistoryLedgerEvent(event: OpenClawLedgerEvent): boolean {
    return event.active_request_id.startsWith("history:") || event.turn_id.includes(":history:");
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
    if (previousSegmentId || incomingSegmentId) {
      return previousSegmentId === incomingSegmentId;
    }

    const previousRunId = getAssistantAnswerRunIdentity(previous);
    const incomingRunId = getAssistantAnswerRunIdentity(incoming);
    if (previousRunId && incomingRunId && previousRunId === incomingRunId) {
      const previousRawSeq = readAssistantAnswerRawSeqIdentity(previous);
      const incomingRawSeq = readAssistantAnswerRawSeqIdentity(incoming);
      return previousRawSeq > 0 && previousRawSeq === incomingRawSeq;
    }

    if (isBareSessionAssistantMessageSnapshot(previous) && isAuthoritativeChatAnswerSnapshot(incoming)) {
      const distance = Math.abs(Number(incoming.seq || 0) - Number(previous.seq || 0));
      return distance > 0 && distance <= 20;
    }

    return false;
  }

  function readAssistantAnswerSegmentId(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const timeline = toRecord(payload.openclaw_timeline);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    return stringOr(timeline.segment_id, payload.segment_id, ledger?.part_type === "answer" ? ledger.part_id : undefined);
  }

  function getAssistantAnswerRunIdentity(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    return stringOr(payload.runId, payload.run_id, ledger?.run_id);
  }

  function readAssistantAnswerRawSeqIdentity(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    const ledgerPayload = toRecord(ledger?.payload);
    return firstPositiveNumber(payload.rawSeq, payload.raw_seq, ledgerPayload.rawSeq, ledgerPayload.raw_seq);
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

  function shouldPreferAssistantMessageEvent(incoming: TimelineEvent, previous: TimelineEvent): boolean {
    return assistantMessageSpecificity(incoming) >= assistantMessageSpecificity(previous);
  }

  function assistantMessageSpecificity(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    let score = 0;
    if (typeof payload.runId === "string" && payload.runId.trim()) score += 4;
    if (ledger?.run_id) score += 4;
    if (ledger && !isOpenClawHistoryLedgerEvent(ledger)) score += 3;
    if (payload.rawSeq !== undefined) score += 2;
    if (payload.state === "final") score += 1;
    if (payload.mode === "replace") score += 1;
    if (ledger?.visibility === "final") score += 1;
    if (ledger?.operation === "replace") score += 1;
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
      await input.callbacks.onEnsureSessionStream(session.id);
      await input.callbacks.onUserMessage({
        id: `hub53ai-user-${message.msgId}`,
        sessionId: session.id,
        role: "user",
        content: buildPrompt(message),
        createdAt: new Date().toISOString()
      });
      await input.callbacks.onSessionStatus(session.id, "running");

      const requestIdentity = message.clientMessageId || message.reqId;
      const eventScope: GatewayEventScope = {
        eventBoundaryMs: Date.now(),
        turnId: buildOpenClawTimelineTurnId(session.id, requestIdentity),
        activeRequestId: requestIdentity,
        nextSegmentIndex: 0,
        nextAnswerSegmentIndex: 0,
        answerBoundaryAfterVisibleResponse: false,
        activityAppliedEventKeys: new Set<string>(),
        answerContentAppliedEventKeys: new Set<string>(),
        answerSegmentTextById: new Map<string, string>(),
        nextDeltaIndexBySegment: new Map<string, number>(),
        segmentIndexById: new Map<string, number>(),
        timelineMetaByEventKey: new Map<string, OpenClawTimelineV2Meta>(),
        currentActivitySeen: false,
        visibleResponseSeen: false,
        lastSeqSeen: 0,
        emittedOutputFileKeys: new Set<string>(),
        referencedLocalOutputPaths: new Set<string>(),
        writeOutputFilesByToolCallId: new Map<string, Hub53AIOutputFile[]>(),
        localOutputSnapshot: await snapshotLocalOutputFiles({
          config: input.config,
          configPath: input.configPath,
          stateDir: input.stateDir,
          logger: input.logger
        })
      };
      trackActiveSessionRequest(sessionId, message.reqId, { message, eventScope });
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

  function trackActiveSessionRequest(sessionId: string, reqId: string, details?: ActiveSessionRequest) {
    const reqIds = activeReqIdsBySession.get(sessionId) ?? new Set<string>();
    reqIds.add(reqId);
    activeReqIdsBySession.set(sessionId, reqIds);
    if (details) {
      const requests = activeRequestDetailsBySession.get(sessionId) ?? new Map<string, ActiveSessionRequest>();
      requests.set(reqId, details);
      activeRequestDetailsBySession.set(sessionId, requests);
    }
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
    const requests = activeRequestDetailsBySession.get(sessionId);
    if (!requests) {
      return;
    }
    requests.delete(reqId);
    if (!requests.size) {
      activeRequestDetailsBySession.delete(sessionId);
    }
  }

  async function resolveActiveSessionRequests(sessionId: string, reason: string) {
    const handledReqIds = new Set<string>();
    for (const [reqId, details] of activeRequestDetailsBySession.get(sessionId) ?? []) {
      handledReqIds.add(reqId);
      await emitSyntheticTerminalEvent(details.message, sessionId, details.eventScope, {
        kind: "run.interrupted",
        status: "interrupted",
        reason
      });
      lastReplyByReq.delete(reqId);
      resolveTerminalEvent(reqId);
    }
    for (const reqId of activeReqIdsBySession.get(sessionId) ?? []) {
      if (handledReqIds.has(reqId)) continue;
      lastReplyByReq.delete(reqId);
      resolveTerminalEvent(reqId);
    }
  }

  async function emitSyntheticTerminalEvent(
    message: Hub53AIIncomingMessage,
    sessionId: string,
    eventScope: GatewayEventScope,
    inputTerminal: {
      kind: "run.completed" | "run.failed" | "run.interrupted";
      status: Exclude<OpenClawLedgerTerminalStatus, "running">;
      reason: string;
    }
  ) {
    if (eventScope.terminalSeen) {
      return;
    }

    const seq = await getNextSyntheticEventSeq(sessionId, eventScope);
    const createdAt = new Date().toISOString();
    const event: TimelineEvent = {
      id: `synthetic:${inputTerminal.reason}:${message.reqId}:${seq}`,
      sessionId,
      seq,
      kind: inputTerminal.kind,
      payload: {
        synthetic: true,
        synthetic_terminal: true,
        synthetic_reason: inputTerminal.reason,
        active_request_id: eventScope.activeRequestId,
        req_id: message.reqId,
        ...(message.clientMessageId ? { client_message_id: message.clientMessageId } : {}),
        turn_id: eventScope.currentTurnId || eventScope.turnId,
        ...(eventScope.currentRunId ? { runId: eventScope.currentRunId, run_id: eventScope.currentRunId } : {}),
        terminal_status: inputTerminal.status
      },
      createdAt
    };
    const payload = augmentPayloadWithEventMeta(event, eventScope);
    const syntheticEvent = {
      ...event,
      payload
    };
    appendSyntheticSessionEvent(sessionId, syntheticEvent);
    eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, seq);
    eventScope.terminalSeen = true;
    const ledgerEvent = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(syntheticEvent));
    traceOpenClawLedger(input.logger, "synthetic-terminal", {
      reqId: message.reqId,
      sessionId,
      reason: inputTerminal.reason,
      event: summarizeTimelineEventForTrace(syntheticEvent),
      ...(ledgerEvent ? { ledger: summarizeOpenClawLedgerEvent(ledgerEvent) } : {})
    }, input.config);
    await sendReply({
      reqId: message.reqId,
      text: "",
      status: "done",
      sessionId,
      eventKind: inputTerminal.kind,
      payload
    });
  }

  async function getNextSyntheticEventSeq(sessionId: string, eventScope: GatewayEventScope): Promise<number> {
    const syntheticSeq = (syntheticEventsBySession.get(sessionId) ?? []).reduce(
      (maxSeq, event) => Math.max(maxSeq, event.seq || 0),
      0
    );
    let gatewaySeq = 0;
    try {
      const events = await input.gateway.listEvents(sessionId, 0);
      gatewaySeq = events.reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
    } catch {
      gatewaySeq = 0;
    }
    return Math.max(eventScope.lastSeqSeen, syntheticSeq, gatewaySeq) + 1;
  }

  function appendSyntheticSessionEvent(sessionId: string, event: TimelineEvent) {
    if (!sessionId) {
      return;
    }
    const events = syntheticEventsBySession.get(sessionId) ?? [];
    if (events.some((candidate) => candidate.id === event.id)) {
      return;
    }
    const nextEvents = [...events, event].slice(-MAX_SYNTHETIC_EVENTS_PER_SESSION);
    syntheticEventsBySession.set(sessionId, nextEvents);
    setPersistedSessionEvents("syntheticEventsBySession", sessionId, nextEvents, MAX_SYNTHETIC_EVENTS_PER_SESSION);
    rememberOpenClawLedgerSeq(sessionId, event);
    persistStateSoon("synthetic ledger event");
  }

  function appendCanonicalSessionEvent(sessionId: string, event: TimelineEvent) {
    if (!sessionId) {
      return;
    }
    const events = canonicalEventsBySession.get(sessionId) ?? [];
    const key = event.id || `${event.sessionId}:${event.seq}:${event.kind}`;
    const next = events.filter((candidate) => (candidate.id || `${candidate.sessionId}:${candidate.seq}:${candidate.kind}`) !== key);
    const nextEvents = [...next, event].slice(-MAX_CANONICAL_EVENTS_PER_SESSION);
    canonicalEventsBySession.set(sessionId, nextEvents);
    setPersistedSessionEvents("canonicalEventsBySession", sessionId, nextEvents, MAX_CANONICAL_EVENTS_PER_SESSION);
    rememberOpenClawLedgerSeq(sessionId, event);
    persistStateSoon("canonical ledger event");
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
    activeRequestId: string;
    currentTurnId?: string;
    currentRunId?: string;
    nextSegmentIndex: number;
    nextAnswerSegmentIndex: number;
    currentAnswerSegmentId?: string;
    answerBoundaryAfterVisibleResponse: boolean;
    currentAnswerRawOrder?: number;
    activityAppliedEventKeys: Set<string>;
    answerContentAppliedEventKeys: Set<string>;
    answerSegmentTextById: Map<string, string>;
    nextDeltaIndexBySegment: Map<string, number>;
    segmentIndexById: Map<string, number>;
    timelineMetaByEventKey: Map<string, OpenClawTimelineV2Meta>;
    currentActivitySeen: boolean;
    visibleResponseSeen: boolean;
    lastSeqSeen: number;
    terminalSeen?: boolean;
    emittedOutputFileKeys: Set<string>;
    localOutputSnapshot?: LocalOutputFileSnapshot;
    referencedLocalOutputPaths: Set<string>;
    writeOutputFilesByToolCallId: Map<string, Hub53AIOutputFile[]>;
    localOutputFilesSent?: boolean;
  };

  type ActiveSessionRequest = {
    message: Hub53AIIncomingMessage;
    eventScope: GatewayEventScope;
  };

  function applyOpenClawEventScopeActivity(event: GatewayEvent, eventScope: GatewayEventScope) {
    const cacheKey = getTimelineEventCacheKey(event);
    if (eventScope.activityAppliedEventKeys.has(cacheKey)) {
      return;
    }
    eventScope.activityAppliedEventKeys.add(cacheKey);

    const isAnswerEvent = event.kind === "assistant.delta" || event.kind === "assistant.message";
    const isVisibleEvent = isVisibleOpenClawResponseEvent(event);
    if (isCurrentRunActivityEvent(event)) {
      if (!isAnswerEvent && eventScope.visibleResponseSeen && isVisibleEvent) {
        const eventRawOrder = getOpenClawRawOrder(event);
        if (!eventRawOrder || !eventScope.currentAnswerRawOrder || eventRawOrder >= eventScope.currentAnswerRawOrder) {
          eventScope.answerBoundaryAfterVisibleResponse = true;
        } else {
          traceOpenClawLedger(input.logger, "stale", {
            reason: "late_non_answer_activity_before_current_answer",
            event: summarizeTimelineEventForTrace(event),
            eventRawOrder,
            currentAnswerRawOrder: eventScope.currentAnswerRawOrder
          }, input.config);
        }
      }
      eventScope.currentActivitySeen = true;
    }
    if (isVisibleEvent) {
      eventScope.visibleResponseSeen = true;
    }
    rememberWriteToolOutputFileCandidate(event, eventScope);
  }

  async function handleGatewayEvent(
    message: Hub53AIIncomingMessage,
    event: GatewayEvent,
    sessionId: string,
    eventScope: GatewayEventScope
  ) {
    if (!(activeReqIdsBySession.get(sessionId)?.has(message.reqId) ?? true)) {
      traceOpenClawLedger(input.logger, "stale", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      traceOpenClawDuplicate(input.logger, "hub.event.skip_inactive_req", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return;
    }
    traceOpenClawLedger(input.logger, "raw", {
      reqId: message.reqId,
      sessionId,
      event: summarizeTimelineEventForTrace(event)
    }, input.config);
    if (isReplayFromPreviousRun(event, eventScope)) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_previous_run", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return;
    }
    if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
      eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, event.seq);
    }
    if (isTerminalRunEvent(event) && event.kind !== "run.failed") {
      eventScope.terminalSeen = true;
    }
    traceOpenClawDuplicate(input.logger, "hub.event.received", {
      reqId: message.reqId,
      sessionId,
      event: summarizeTimelineEventForTrace(event)
    }, input.config);
    applyOpenClawEventScopeActivity(event, eventScope);
    if (shouldAttachOpenClawTimeline(event)) {
      augmentPayloadWithEventMeta(event, eventScope);
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
    await sendWriteToolOutputFilesForEvent(message.reqId, sessionId, event, eventScope);

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
      }, input.config);
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
      const derivedFailedEvent = buildFailedRunEventFromStatusUpdate(event, sessionId, message.reqId, eventScope);
      if (derivedFailedEvent) {
        await handleRunFailureEvent(message, sessionId, derivedFailedEvent, eventScope, {
          appendToSessionEvents: true,
          traceReason: "status.update.failed"
        });
      }
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
        sessionId,
        eventKind: event.kind,
        payload: augmentPayloadWithEventMeta(event, eventScope)
      });
      lastReplyByReq.delete(message.reqId);
      resolveTerminalEvent(message.reqId);
      return;
    }

    if (event.kind === "run.failed" || event.kind === "run.interrupted") {
      if (event.kind === "run.failed") {
        await handleRunFailureEvent(message, sessionId, event, eventScope);
        return;
      }

      const errorText = String(event.payload?.error ?? event.payload?.message ?? event.kind);
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ ${errorText}`,
        status: "error",
        sessionId,
        error: {
          code: inferErrorCode(errorText),
          message: errorText
        },
        eventKind: event.kind,
        payload: augmentPayloadWithEventMeta(event, eventScope)
      });
      lastReplyByReq.delete(message.reqId);
      resolveTerminalEvent(message.reqId);
    }
  }

  async function handleRunFailureEvent(
    message: Hub53AIIncomingMessage,
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    options?: {
      appendToSessionEvents?: boolean;
      traceReason?: string;
    }
  ) {
    if (eventScope.terminalSeen) {
      return;
    }

    const classification = classifyOpenClawRunFailure(event, eventScope);
    const payload = augmentPayloadWithEventMeta(enrichFailedRunEvent(event, message.reqId, eventScope, classification), eventScope);
    const failedEvent: TimelineEvent = {
      ...event,
      payload
    };
    if (options?.appendToSessionEvents) {
      appendSyntheticSessionEvent(sessionId, failedEvent);
    }
    eventScope.terminalSeen = true;
    if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
      eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, event.seq);
    }
    const ledgerEvent = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(failedEvent));
    traceOpenClawLedger(input.logger, "synthetic-terminal", {
      reqId: message.reqId,
      sessionId,
      reason: options?.traceReason ?? "run.failed",
      classification: {
        code: classification.code,
        reason: classification.reason,
        provider: classification.provider,
        model: classification.model,
        runtimeMs: classification.runtimeMs,
        authRelated: classification.authRelated,
        confidence: classification.confidence
      },
      event: summarizeTimelineEventForTrace(failedEvent),
      ...(ledgerEvent ? { ledger: summarizeOpenClawLedgerEvent(ledgerEvent) } : {})
    }, input.config);
    await input.callbacks.onSessionStatus(sessionId, "failed");
    await sendReply({
      reqId: message.reqId,
      text: `⚠️ ${classification.userMessage}`,
      status: "error",
      sessionId,
      error: {
        code: classification.code,
        message: classification.userMessage,
        ...(classification.rawMessage ? { details: classification.rawMessage } : {})
      },
      eventKind: event.kind,
      payload
    });
    lastReplyByReq.delete(message.reqId);
    resolveTerminalEvent(message.reqId);
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

  function isFailedStatusUpdate(event: GatewayEvent): boolean {
    if (event.kind !== "status.update") {
      return false;
    }
    const payload = toRecord(event.payload);
    const session = toRecord(payload.session);
    const phase = String(payload.phase ?? session.phase ?? "").toLowerCase();
    const status = String(payload.status ?? session.status ?? "").toLowerCase();
    return phase === "error" || phase === "failed" || status === "error" || status === "failed";
  }

  function buildFailedRunEventFromStatusUpdate(
    event: GatewayEvent,
    sessionId: string,
    reqId: string,
    eventScope: GatewayEventScope
  ): TimelineEvent | null {
    if (!isFailedStatusUpdate(event)) {
      return null;
    }
    const runIdentity = getGatewayEventRunIdentity(event) || eventScope.currentRunId;
    const eventMs = Date.parse(event.createdAt);
    const eventBeforeBoundary = Number.isFinite(eventMs) && eventMs < eventScope.eventBoundaryMs;
    const classification = classifyOpenClawRunFailure(event, eventScope);
    const requestScopedAuthFailure = classification.authRelated && !eventBeforeBoundary;
    if (eventScope.currentRunId && runIdentity && eventScope.currentRunId !== runIdentity) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_status_failed_mismatched_run", {
        reqId,
        sessionId,
        currentRunId: eventScope.currentRunId,
        eventRunId: runIdentity,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return null;
    }
    if (!runIdentity && !eventScope.currentActivitySeen && !requestScopedAuthFailure) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_status_failed_without_current_run", {
        reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return null;
    }

    if (!eventScope.currentActivitySeen && eventBeforeBoundary) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_status_failed_before_boundary", {
        reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return null;
    }

    const payload = enrichFailedRunEvent(
      {
        ...event,
        kind: "run.failed",
        payload: {
          ...toRecord(event.payload),
          derived_terminal: true,
          derived_from_kind: event.kind,
          derived_from_event_id: event.id,
          derived_from_event_seq: event.seq
        }
      },
      reqId,
      eventScope,
      classification
    ).payload;
    return {
      id: `derived:run.failed:${event.id || `${event.sessionId || sessionId}:${event.seq || Date.now()}`}`,
      sessionId: event.sessionId || sessionId,
      seq: typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : eventScope.lastSeqSeen + 1,
      kind: "run.failed",
      payload,
      createdAt: event.createdAt || new Date().toISOString()
    };
  }

  function getGatewayEventRunIdentity(event: GatewayEvent): string {
    const payload = toRecord(event.payload);
    const session = toRecord(payload.session);
    return identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id,
      session.runId,
      session.run_id,
      session.responseId,
      session.response_id
    );
  }

  function isVisibleOpenClawResponseEvent(event: GatewayEvent): boolean {
    if (event.kind === "assistant.delta" || event.kind === "assistant.message" || event.kind === "assistant.thinking") {
      return String(event.payload?.content ?? "").trim().length > 0;
    }
    if (event.kind === "tool.call" || event.kind === "tool.result") {
      return true;
    }
    if (event.kind === "process.step" && isOutputFilesProcessStepEvent(event)) {
      return true;
    }
    return false;
  }

  function enrichFailedRunEvent(
    event: GatewayEvent,
    reqId: string,
    eventScope: GatewayEventScope,
    classification: OpenClawRunFailureClassification
  ): GatewayEvent {
    const payload = toRecord(event.payload);
    const runIdentity = getGatewayEventRunIdentity(event) || eventScope.currentRunId;
    const turnId = eventScope.currentTurnId || eventScope.turnId || stringOr(payload.turn_id);
    const rawMessage = classification.rawMessage || extractOpenClawFailureMessage(payload);
    return {
      ...event,
      payload: {
        ...payload,
        ...(runIdentity ? { runId: payload.runId ?? runIdentity, run_id: payload.run_id ?? runIdentity } : {}),
        active_request_id: stringOr(payload.active_request_id) || eventScope.activeRequestId || reqId,
        req_id: stringOr(payload.req_id) || reqId,
        request_id: stringOr(payload.request_id) || reqId,
        turn_id: turnId,
        terminal_status: "failed",
        failure_code: classification.code,
        failure_reason: classification.reason,
        failure_auth_related: classification.authRelated,
        failure_confidence: classification.confidence,
        user_message: classification.userMessage,
        error_message: classification.userMessage,
        error: classification.userMessage,
        message: classification.userMessage,
        ...(rawMessage ? { raw_error_message: rawMessage } : {}),
        failure_classification: {
          code: classification.code,
          reason: classification.reason,
          user_message: classification.userMessage,
          ...(rawMessage ? { raw_message: rawMessage } : {}),
          ...(classification.provider ? { provider: classification.provider } : {}),
          ...(classification.model ? { model: classification.model } : {}),
          ...(typeof classification.runtimeMs === "number" ? { runtime_ms: classification.runtimeMs } : {}),
          auth_related: classification.authRelated,
          confidence: classification.confidence,
          visible_response_seen: eventScope.visibleResponseSeen
        }
      }
    };
  }

  function classifyOpenClawRunFailure(
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ): OpenClawRunFailureClassification {
    const payload = toRecord(event.payload);
    const existingClassification = toRecord(payload.failure_classification);
    const existingCode = stringOr(payload.failure_code, existingClassification.code);
    const existingUserMessage = stringOr(payload.user_message, payload.error_message, existingClassification.user_message);
    const rawMessage = stringOr(payload.raw_error_message, existingClassification.raw_message) || extractOpenClawFailureMessage(payload);
    const provider = readOpenClawFailureProvider(payload);
    const model = readOpenClawFailureModel(payload);
    const runtimeMs = readOpenClawFailureRuntimeMs(payload);
    if (existingCode && existingUserMessage) {
      return {
        code: existingCode,
        reason: stringOr(payload.failure_reason, existingClassification.reason) || "openclaw_run_failed",
        userMessage: existingUserMessage,
        ...(rawMessage ? { rawMessage } : {}),
        ...(provider ? { provider } : {}),
        ...(model ? { model } : {}),
        ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
        authRelated: Boolean(payload.failure_auth_related ?? existingClassification.auth_related),
        confidence: normalizeFailureConfidence(stringOr(payload.failure_confidence, existingClassification.confidence)) ?? "medium"
      };
    }

    const lowerRaw = rawMessage.toLowerCase();
    const authRelated = isOpenClawAuthenticationFailureText(lowerRaw);
    const qclawSignal = isQClawFailureSignal(`${provider} ${model} ${rawMessage}`.toLowerCase());
    const failedFast = typeof runtimeMs !== "number" || runtimeMs <= 5_000;
    const lowInformationFailure = !rawMessage || isLowInformationFailureMessage(rawMessage);
    if (qclawSignal && (authRelated || (failedFast && lowInformationFailure && !eventScope.visibleResponseSeen))) {
      return {
        code: "QCLAW_LOGIN_REQUIRED",
        reason: authRelated ? "qclaw_auth_failed" : "qclaw_login_or_provider_auth_failed",
        userMessage: buildOpenClawAuthFailureUserMessage({
          scope: "qclaw",
          model
        }),
        ...(rawMessage ? { rawMessage } : {}),
        ...(provider ? { provider } : {}),
        ...(model ? { model } : {}),
        ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
        authRelated: true,
        confidence: authRelated ? "high" : "medium"
      };
    }

    if (authRelated) {
      return {
        code: "OPENCLAW_AUTH_REQUIRED",
        reason: "openclaw_auth_failed",
        userMessage: buildOpenClawAuthFailureUserMessage({
          scope: "openclaw",
          model
        }),
        rawMessage,
        ...(provider ? { provider } : {}),
        ...(model ? { model } : {}),
        ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
        authRelated: true,
        confidence: "high"
      };
    }

    const readableFailure = rawMessage && !isLowInformationFailureMessage(rawMessage)
      ? rawMessage
      : "OpenClaw 智能体运行失败，但 Gateway 没有提供可读错误信息。请查看 OpenClaw/QClaw 登录状态、模型供应商凭据以及 raw event 调试日志。";
    return {
      code: inferErrorCode(rawMessage || "OpenClaw run failed"),
      reason: "openclaw_run_failed",
      userMessage: readableFailure,
      ...(rawMessage ? { rawMessage } : {}),
      ...(provider ? { provider } : {}),
      ...(model ? { model } : {}),
      ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
      authRelated: false,
      confidence: rawMessage ? "medium" : "low"
    };
  }

  function readOpenClawFailureProvider(payload: Record<string, unknown>): string {
    const session = toRecord(payload.session);
    const modelInfo = toRecord(payload.modelInfo);
    return stringOr(
      payload.modelProvider,
      payload.model_provider,
      payload.provider,
      session.modelProvider,
      session.model_provider,
      session.provider,
      modelInfo.provider
    );
  }

  function readOpenClawFailureModel(payload: Record<string, unknown>): string {
    const session = toRecord(payload.session);
    const modelInfo = toRecord(payload.modelInfo);
    return stringOr(
      payload.model,
      payload.modelName,
      payload.model_name,
      session.model,
      session.modelName,
      session.model_name,
      modelInfo.model,
      modelInfo.name
    );
  }

  function readOpenClawFailureRuntimeMs(payload: Record<string, unknown>): number | undefined {
    const session = toRecord(payload.session);
    return numberOr(
      payload.runtimeMs,
      payload.runtime_ms,
      payload.elapsedMs,
      payload.elapsed_ms,
      session.runtimeMs,
      session.runtime_ms,
      session.elapsedMs,
      session.elapsed_ms
    );
  }

  function extractOpenClawFailureMessage(payload: Record<string, unknown>): string {
    const candidates = collectOpenClawFailureMessageCandidates(payload, 0);
    return candidates.find((candidate) => !isLowInformationFailureMessage(candidate)) || candidates[0] || "";
  }

  function collectOpenClawFailureMessageCandidates(value: unknown, depth: number): string[] {
    if (depth > 3) {
      return [];
    }
    if (typeof value === "string") {
      const normalized = value.trim();
      return normalized ? [normalized] : [];
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return [];
    }
    const record = value as Record<string, unknown>;
    const directKeys = [
      "error",
      "message",
      "error_message",
      "errorMessage",
      "detail",
      "details",
      "reason",
      "failure_reason",
      "cause",
      "description"
    ];
    const direct = directKeys
      .map((key) => record[key])
      .flatMap((candidate) => collectOpenClawFailureMessageCandidates(candidate, depth + 1));
    const nestedKeys = ["error", "cause", "data", "response", "session", "result"];
    const nested = nestedKeys
      .filter((key) => typeof record[key] === "object" && record[key] !== null)
      .flatMap((key) => collectOpenClawFailureMessageCandidates(record[key], depth + 1));
    return [...direct, ...nested];
  }

  function isLowInformationFailureMessage(message: string): boolean {
    const normalized = message.replace(/\s+/g, " ").trim().toLowerCase();
    return (
      !normalized ||
      normalized === "error" ||
      normalized === "failed" ||
      normalized === "failure" ||
      normalized === "run.failed" ||
      normalized === "unknown error" ||
      normalized === "internal_error" ||
      normalized === "status failed"
    );
  }

  function isOpenClawAuthenticationFailureText(lowerText: string): boolean {
    return /login|log in|sign in|auth|unauthorized|forbidden|credential|api key|apikey|token|permission|401|403|not logged in|认证|鉴权|授权|登录|未登录|凭据|密钥|权限|账号|账户/.test(lowerText);
  }

  function isQClawFailureSignal(lowerText: string): boolean {
    return /(?:^|[^a-z0-9])q[\s_-]*claw(?:[^a-z0-9]|$)/.test(lowerText);
  }

  function buildOpenClawAuthFailureUserMessage(inputMessage: {
    scope: "qclaw" | "openclaw";
    model?: string;
  }): string {
    const modelHint = inputMessage.model ? `当前模型：${inputMessage.model}。` : "";
    if (inputMessage.scope === "qclaw") {
      return `QClaw/OpenClaw 智能体登录失败或模型供应商认证失效。请重新登录 QClaw/OpenClaw，或检查模型供应商的 API Key、Base URL 与账号权限。${modelHint}`;
    }
    return `OpenClaw 智能体登录失败或模型供应商认证失效。请重新登录 OpenClaw/QClaw，或检查模型供应商凭据、Base URL 与账号权限。${modelHint}`;
  }

  function normalizeFailureConfidence(value: string): OpenClawRunFailureClassification["confidence"] | undefined {
    if (value === "high" || value === "medium" || value === "low") {
      return value;
    }
    return undefined;
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
    traceOpenClawDuplicate(input.logger, "hub.reply.send", summarizeOutgoingFrameForTrace(frame), input.config);
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
    const payload = normalizeOpenClawAnswerPayloadForSegment(event, eventScope, timeline);
    const ledger = buildOpenClawLedgerEvent(event, eventScope, timeline);
    const runIdentity = identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id,
      eventScope.currentRunId
    );
    const payloadWithMeta = {
      ...payload,
      event_id: event.id,
      event_kind: event.kind,
      event_created_at: event.createdAt,
      ...(typeof event.seq === "number" ? { seq: event.seq, message_seq: payload.message_seq ?? event.seq } : {}),
      ...(runIdentity ? { runId: payload.runId ?? runIdentity, run_id: payload.run_id ?? runIdentity } : {}),
      turn_id: timeline.turn_id,
      segment_id: timeline.segment_id,
      segment_type: timeline.segment_type,
      segment_index: timeline.segment_index,
      delta_index: timeline.delta_index,
      operation: timeline.operation,
      visibility: timeline.visibility,
      final: timeline.final,
      openclaw_timeline: timeline,
      openclaw_ledger: ledger
    };
    const canonicalPayload = attachOpenClawLedgerToPayload(
      event,
      attachOpenClawTimelineToPayload(event, payloadWithMeta, timeline),
      ledger
    );
    if (eventScope.activeRequestId && eventScope.activeRequestId !== "events") {
      appendCanonicalSessionEvent(event.sessionId || ledger.session_id || "", {
        ...event,
        payload: canonicalPayload
      });
    }
    traceOpenClawLedger(input.logger, "ledger", summarizeOpenClawLedgerEvent(ledger), input.config);
    return canonicalPayload;
  }

  function normalizeOpenClawAnswerPayloadForSegment(
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): Record<string, unknown> {
    const payload = toRecord(event.payload);
    if (timeline.segment_type !== "answer" || typeof payload.content !== "string") {
      return payload;
    }

    const cacheKey = getTimelineEventCacheKey(event);
    if (eventScope.answerContentAppliedEventKeys.has(cacheKey)) {
      return payload;
    }
    eventScope.answerContentAppliedEventKeys.add(cacheKey);

    const originalContent = payload.content;
    const normalizedContent = normalizeCumulativeAnswerSnapshotContent(originalContent, event, eventScope, timeline);
    const nextPayload =
      normalizedContent === originalContent
        ? payload
        : {
            ...payload,
            content: normalizedContent,
            openclaw_normalized_cumulative_answer: true,
            openclaw_original_content_length: originalContent.length,
            openclaw_normalized_content_length: normalizedContent.length
          };
    if (nextPayload !== payload) {
      event.payload = nextPayload;
      traceOpenClawLedger(input.logger, "ledger", {
        event: summarizeTimelineEventForTrace(event),
        reason: "cumulative_answer_snapshot_segment_prefix",
        segmentId: timeline.segment_id,
        originalLength: originalContent.length,
        normalizedLength: normalizedContent.length,
        originalHash: hashTraceText(originalContent),
        normalizedHash: hashTraceText(normalizedContent)
      }, input.config);
    }

    rememberOpenClawAnswerSegmentContent(event, eventScope, timeline, normalizedContent);
    return nextPayload;
  }

  function normalizeCumulativeAnswerSnapshotContent(
    content: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): string {
    if (timeline.operation !== "replace" && !isReplyReplaceEvent(event)) {
      return content;
    }
    const answerIndex = getOpenClawAnswerSegmentIndex(timeline.segment_id);
    if (answerIndex <= 0) {
      return content;
    }
    const priorText = getPriorOpenClawAnswerSegmentsText(timeline.turn_id, answerIndex, eventScope);
    if (!priorText || !content.startsWith(priorText)) {
      return content;
    }
    return content.slice(priorText.length).replace(/^\s+/, "");
  }

  function rememberOpenClawAnswerSegmentContent(
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta,
    content: string
  ) {
    const existing = eventScope.answerSegmentTextById.get(timeline.segment_id) || "";
    const next =
      timeline.operation === "append"
        ? `${existing}${content}`
        : timeline.operation === "replace"
          ? content
          : existing || content;
    eventScope.answerSegmentTextById.set(timeline.segment_id, next);

    const rawOrder = getOpenClawRawOrder(event);
    if (rawOrder) {
      eventScope.currentAnswerRawOrder = eventScope.currentAnswerRawOrder
        ? Math.max(eventScope.currentAnswerRawOrder, rawOrder)
        : rawOrder;
    }
  }

  function getPriorOpenClawAnswerSegmentsText(turnId: string, currentAnswerIndex: number, eventScope: GatewayEventScope): string {
    const texts: string[] = [];
    for (let index = 0; index < currentAnswerIndex; index += 1) {
      const text = eventScope.answerSegmentTextById.get(`${turnId}:answer:${index}`);
      if (text) {
        texts.push(text);
      }
    }
    return texts.join("");
  }

  function getOpenClawAnswerSegmentIndex(segmentId: string): number {
    const match = segmentId.match(/:answer:(\d+)(?::|$)/);
    if (!match) {
      return 0;
    }
    const parsed = Number(match[1]);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function getOpenClawRawOrder(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    return firstPositiveNumber(
      payload.rawSeq,
      payload.raw_seq,
      payload.messageSeq,
      payload.message_seq,
      event.seq
    );
  }

  function buildOpenClawTimelineTurnId(sessionId: string, reqId: string) {
    return `${sessionId || "openclaw"}:turn:${reqId || randomUUID()}`;
  }

  function normalizeTimelineEventsWithProtocol(sessionId: string, events: TimelineEvent[]): TimelineEvent[] {
    const eventScope: GatewayEventScope = {
      eventBoundaryMs: 0,
      turnId: buildOpenClawTimelineTurnId(sessionId, "events"),
      activeRequestId: "events",
      nextSegmentIndex: 0,
      nextAnswerSegmentIndex: 0,
      answerBoundaryAfterVisibleResponse: false,
      activityAppliedEventKeys: new Set<string>(),
      answerContentAppliedEventKeys: new Set<string>(),
      answerSegmentTextById: new Map<string, string>(),
      nextDeltaIndexBySegment: new Map<string, number>(),
      segmentIndexById: new Map<string, number>(),
      timelineMetaByEventKey: new Map<string, OpenClawTimelineV2Meta>(),
      currentActivitySeen: false,
      visibleResponseSeen: false,
      lastSeqSeen: 0,
      emittedOutputFileKeys: new Set<string>(),
      referencedLocalOutputPaths: new Set<string>(),
      writeOutputFilesByToolCallId: new Map<string, Hub53AIOutputFile[]>()
    };
    return events.map((event) => {
      if (!shouldAttachOpenClawTimeline(event)) {
        return event;
      }
      applyOpenClawEventScopeActivity(event, eventScope);
      return {
        ...event,
        payload: augmentPayloadWithEventMeta(event, eventScope)
      };
    });
  }

  function shouldAttachOpenClawTimeline(event: TimelineEvent): boolean {
    return Boolean(getExpectedOpenClawSegmentType(event));
  }

  function readOpenClawTimelineFromEvent(event: TimelineEvent): Record<string, unknown> {
    const payload = toRecord(event.payload);
    const direct = toRecord(payload.openclaw_timeline);
    if (Object.keys(direct).length > 0) {
      return direct;
    }
    const processStep = toRecord(payload.process_step);
    const processData = toRecord(processStep.data);
    return toRecord(processData.openclaw_timeline);
  }

  function attachOpenClawTimelineToPayload(
    event: TimelineEvent,
    payload: Record<string, unknown>,
    timeline: OpenClawTimelineV2Meta
  ): Record<string, unknown> {
    if (event.kind !== "process.step" || timeline.segment_type !== "output_files") {
      return payload;
    }

    const processStep = toRecord(payload.process_step);
    if (Object.keys(processStep).length === 0) {
      return payload;
    }
    const processData = toRecord(processStep.data);
    return {
      ...payload,
      process_step: {
        ...processStep,
        data: {
          ...processData,
          openclaw_timeline: timeline
        }
      }
    };
  }

  function attachOpenClawLedgerToPayload(
    event: TimelineEvent,
    payload: Record<string, unknown>,
    ledger: OpenClawLedgerEvent
  ): Record<string, unknown> {
    if (event.kind !== "process.step" || ledger.part_type !== "output_file") {
      return payload;
    }

    const processStep = toRecord(payload.process_step);
    if (Object.keys(processStep).length === 0) {
      return payload;
    }
    const processData = toRecord(processStep.data);
    return {
      ...payload,
      process_step: {
        ...processStep,
        data: {
          ...processData,
          openclaw_ledger: ledger
        }
      }
    };
  }

  function readOpenClawLedgerFromEvent(event: TimelineEvent): Record<string, unknown> {
    const payload = toRecord(event.payload);
    const direct = toRecord(payload.openclaw_ledger);
    if (Object.keys(direct).length > 0) {
      return direct;
    }
    const processStep = toRecord(payload.process_step);
    const processData = toRecord(processStep.data);
    return toRecord(processData.openclaw_ledger);
  }

  function buildOpenClawLedgerEvent(
    event: TimelineEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): OpenClawLedgerEvent {
    const payload = toRecord(event.payload);
    const runIdentity = identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id,
      eventScope.currentRunId
    );
    const text = typeof payload.content === "string" ? payload.content : undefined;
    const terminalStatus = getOpenClawLedgerTerminalStatus(event);
    const eventType = getOpenClawLedgerEventType(event, timeline);
    return {
      protocol_version: "openclaw.ledger.v1",
      seq: getOpenClawLedgerSeq(event, timeline, eventType),
      session_id: event.sessionId || "",
      conversation_id: event.sessionId || "",
      turn_id: timeline.turn_id,
      ...(runIdentity ? { run_id: runIdentity } : {}),
      active_request_id:
        stringOr(payload.active_request_id) ||
        eventScope.activeRequestId ||
        stringOr(payload.req_id, payload.request_id) ||
        timeline.turn_id,
      part_id: getOpenClawLedgerPartId(timeline),
      part_type: getOpenClawLedgerPartType(timeline.segment_type),
      event_type: eventType,
      operation: eventType === "turn.started" ? "noop" : timeline.operation,
      visibility: timeline.visibility,
      ...(text != null ? { text } : {}),
      payload: buildOpenClawLedgerPayload(event),
      ...(terminalStatus ? { terminal_status: terminalStatus } : {}),
      created_at: event.createdAt || new Date().toISOString(),
      raw_event_ref: buildOpenClawRawEventRef(event)
    };
  }

  function buildOpenClawLedgerPayload(event: TimelineEvent): Record<string, unknown> {
    const payload = toRecord(event.payload);
    const rest = { ...payload };
    delete rest.openclaw_ledger;
    return {
      ...rest,
      source_kind: event.kind,
      event_id: event.id,
      event_seq: event.seq
    };
  }

  function getOpenClawLedgerSeq(
    event: TimelineEvent,
    timeline: OpenClawTimelineV2Meta,
    eventType: OpenClawLedgerEventType
  ): number {
    const sessionId = event.sessionId || "";
    const rawEventRef = buildOpenClawRawEventRef(event);
    const existingSeq = findExistingOpenClawLedgerSeq(sessionId, rawEventRef, getOpenClawLedgerPartId(timeline), eventType);
    if (existingSeq) {
      return existingSeq;
    }
    return allocateNextOpenClawLedgerSeq(sessionId);
  }

  function findExistingOpenClawLedgerSeq(
    sessionId: string,
    rawEventRef: string,
    partId: string,
    eventType: OpenClawLedgerEventType
  ): number {
    for (const source of [canonicalEventsBySession, syntheticEventsBySession]) {
      for (const event of source.get(sessionId) ?? []) {
        const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
        if (!ledger) {
          continue;
        }
        if (ledger.raw_event_ref === rawEventRef && ledger.part_id === partId && ledger.event_type === eventType) {
          return ledger.seq;
        }
      }
    }
    return 0;
  }

  function allocateNextOpenClawLedgerSeq(sessionId: string): number {
    const key = sessionId || "openclaw";
    const current = ledgerSeqBySession.get(key) ?? computeMaxOpenClawLedgerSeqForSession(sessionId);
    const next = current + 1;
    ledgerSeqBySession.set(key, next);
    return next;
  }

  function rememberOpenClawLedgerSeq(sessionId: string, event: TimelineEvent) {
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    if (!ledger) {
      return;
    }
    const key = sessionId || ledger.session_id || "openclaw";
    ledgerSeqBySession.set(key, Math.max(ledgerSeqBySession.get(key) ?? 0, ledger.seq));
  }

  function computeMaxOpenClawLedgerSeqForSession(sessionId: string): number {
    let maxSeq = 0;
    for (const source of [canonicalEventsBySession, syntheticEventsBySession]) {
      for (const event of source.get(sessionId) ?? []) {
        const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
        if (ledger) {
          maxSeq = Math.max(maxSeq, ledger.seq);
        }
      }
    }
    return maxSeq;
  }

  function getOpenClawLedgerPartId(timeline: OpenClawTimelineV2Meta): string {
    if (timeline.segment_type === "run") {
      return `${timeline.turn_id}:status`;
    }
    return timeline.segment_id;
  }

  function getOpenClawLedgerPartType(segmentType: OpenClawTimelineV2SegmentType): OpenClawLedgerPartType {
    if (segmentType === "answer") return "answer";
    if (segmentType === "thinking") return "thinking";
    if (segmentType === "tool_call" || segmentType === "tool_result") return "tool";
    if (segmentType === "output_files") return "output_file";
    return "status";
  }

  function getOpenClawLedgerEventType(
    event: TimelineEvent,
    timeline: OpenClawTimelineV2Meta
  ): OpenClawLedgerEventType {
    if (event.kind === "run.started") return "turn.started";
    if (event.kind === "run.completed") return "turn.completed";
    if (event.kind === "run.interrupted") return "turn.interrupted";
    if (event.kind === "run.failed") return "turn.failed";
    if (timeline.operation === "append") return "part.delta";
    if (timeline.operation === "close") return "part.done";
    return "part.replace";
  }

  function getOpenClawLedgerTerminalStatus(event: TimelineEvent): OpenClawLedgerTerminalStatus | undefined {
    if (event.kind === "run.started") return "running";
    if (event.kind === "run.completed") return "completed";
    if (event.kind === "run.interrupted") return "interrupted";
    if (event.kind === "run.failed") return "failed";
    return undefined;
  }

  function buildOpenClawRawEventRef(event: TimelineEvent): string {
    return [event.sessionId || "openclaw", event.seq, event.id || event.kind].filter((part) => part != null && part !== "").join(":");
  }

  function extractOpenClawLedgerEvents(events: TimelineEvent[]): OpenClawLedgerEvent[] {
    return events
      .map((event) => normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event)))
      .filter((event): event is OpenClawLedgerEvent => Boolean(event));
  }

  function normalizeOpenClawLedgerEvent(value: Record<string, unknown>): OpenClawLedgerEvent | null {
    if (value.protocol_version !== "openclaw.ledger.v1") {
      return null;
    }
    const seq = typeof value.seq === "number" ? value.seq : Number(value.seq);
    const sessionId = stringOr(value.session_id);
    const turnId = stringOr(value.turn_id);
    const activeRequestId = stringOr(value.active_request_id);
    const partId = stringOr(value.part_id);
    const partType = normalizeOpenClawLedgerPartType(stringOr(value.part_type));
    const eventType = normalizeOpenClawLedgerEventType(stringOr(value.event_type));
    const operation = normalizeOpenClawLedgerOperation(stringOr(value.operation));
    const visibility = normalizeOpenClawLedgerVisibility(stringOr(value.visibility));
    if (!Number.isFinite(seq) || !sessionId || !turnId || !activeRequestId || !partId || !partType || !eventType || !operation || !visibility) {
      return null;
    }
    const terminalStatus = normalizeOpenClawLedgerTerminalStatus(stringOr(value.terminal_status));
    return {
      protocol_version: "openclaw.ledger.v1",
      seq,
      session_id: sessionId,
      conversation_id: stringOr(value.conversation_id) || sessionId,
      turn_id: turnId,
      ...(stringOr(value.run_id) ? { run_id: stringOr(value.run_id) } : {}),
      active_request_id: activeRequestId,
      part_id: partId,
      part_type: partType,
      event_type: eventType,
      operation,
      visibility,
      ...(typeof value.text === "string" ? { text: value.text } : {}),
      ...(toRecord(value.payload) ? { payload: toRecord(value.payload) } : {}),
      ...(terminalStatus ? { terminal_status: terminalStatus } : {}),
      created_at: stringOr(value.created_at) || new Date().toISOString(),
      ...(stringOr(value.raw_event_ref) ? { raw_event_ref: stringOr(value.raw_event_ref) } : {})
    };
  }

  function normalizeOpenClawLedgerPartType(value: string): OpenClawLedgerPartType | undefined {
    if (value === "answer" || value === "thinking" || value === "tool" || value === "output_file" || value === "status") {
      return value;
    }
    return undefined;
  }

  function normalizeOpenClawLedgerEventType(value: string): OpenClawLedgerEventType | undefined {
    if (
      value === "turn.started" ||
      value === "part.delta" ||
      value === "part.replace" ||
      value === "part.done" ||
      value === "turn.completed" ||
      value === "turn.interrupted" ||
      value === "turn.failed"
    ) {
      return value;
    }
    return undefined;
  }

  function normalizeOpenClawLedgerOperation(value: string): OpenClawLedgerOperation | undefined {
    if (value === "append" || value === "replace" || value === "close" || value === "noop") return value;
    return undefined;
  }

  function normalizeOpenClawLedgerVisibility(value: string): OpenClawTimelineV2Visibility | undefined {
    if (value === "hidden" || value === "stream" || value === "final") return value;
    return undefined;
  }

  function normalizeOpenClawLedgerTerminalStatus(value: string): OpenClawLedgerTerminalStatus | undefined {
    if (value === "running" || value === "completed" || value === "interrupted" || value === "failed" || value === "cancelled") {
      return value;
    }
    return undefined;
  }

  function buildOpenClawSessionSnapshot(sessionId: string, inputLedgerEvents: OpenClawLedgerEvent[], afterSeq = 0): OpenClawSessionSnapshot {
    let ledgerEvents = inputLedgerEvents;
    let { turns, stats, lastSeq } = collectOpenClawLedgerTurnSnapshotState(ledgerEvents);
    const syntheticTerminals = synthesizeTerminalEventsForOrphanedRunningTurns(sessionId, [...turns.values()], stats, lastSeq);
    if (syntheticTerminals.length) {
      ledgerEvents = [...ledgerEvents, ...syntheticTerminals].sort((left, right) => left.seq - right.seq);
      ({ turns, lastSeq } = collectOpenClawLedgerTurnSnapshotState(ledgerEvents));
    }
    const activeTurns = selectOpenClawActiveTurnsForSnapshot(sessionId, [...turns.values()], lastSeq);
    const ledgerEventsAfterSeq = ledgerEvents.filter((event) => event.seq > afterSeq);
    const snapshot = {
      session_id: sessionId,
      conversation_id: sessionId,
      last_seq: lastSeq,
      active_turns: activeTurns,
      recent_events: ledgerEvents,
      ledger_events: ledgerEventsAfterSeq,
      ledgerEvents: ledgerEventsAfterSeq
    };
    traceOpenClawLedger(input.logger, "snapshot.summary", summarizeOpenClawSessionSnapshotForTrace({
      sessionId,
      afterSeq,
      snapshot,
      turns: [...turns.values()],
      syntheticTerminals,
      tracked: getTrackedActiveOpenClawTurns(sessionId)
    }), input.config);
    return snapshot;
  }

  function collectOpenClawLedgerTurnSnapshotState(ledgerEvents: OpenClawLedgerEvent[]): {
    turns: Map<string, OpenClawLedgerTurnSnapshot>;
    stats: Map<string, OpenClawLedgerTurnStats>;
    lastSeq: number;
  } {
    const turns = new Map<string, OpenClawLedgerTurnSnapshot>();
    const stats = new Map<string, OpenClawLedgerTurnStats>();
    for (const event of ledgerEvents) {
      const existing = turns.get(event.turn_id) || {
        turn_id: event.turn_id,
        run_id: event.run_id,
        active_request_id: event.active_request_id,
        status: "running" as OpenClawLedgerTerminalStatus,
        last_seq: 0,
        part_ids: []
      };
      if (event.run_id && !existing.run_id) {
        existing.run_id = event.run_id;
      }
      existing.active_request_id = event.active_request_id || existing.active_request_id;
      existing.last_seq = Math.max(existing.last_seq, event.seq);
      if (!existing.part_ids.includes(event.part_id)) {
        existing.part_ids.push(event.part_id);
      }
      if (event.terminal_status && event.terminal_status !== "running" && existing.status === "running") {
        existing.status = event.terminal_status;
        existing.terminal_seq = event.seq;
      } else if (event.terminal_status === "running" && existing.status === "running") {
        existing.status = event.terminal_status;
      }
      turns.set(event.turn_id, existing);

      const currentStats = stats.get(event.turn_id) || {
        last_created_at_ms: 0,
        has_visible_part: false
      };
      currentStats.last_created_at_ms = Math.max(
        currentStats.last_created_at_ms,
        getOpenClawLedgerCreatedAtMs(event)
      );
      if (isOpenClawLedgerVisiblePart(event)) {
        currentStats.has_visible_part = true;
      }
      stats.set(event.turn_id, currentStats);
    }
    const lastSeq = ledgerEvents.reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
    return { turns, stats, lastSeq };
  }

  function synthesizeTerminalEventsForOrphanedRunningTurns(
    sessionId: string,
    turns: OpenClawLedgerTurnSnapshot[],
    stats: Map<string, OpenClawLedgerTurnStats>,
    lastSeq: number
  ): OpenClawLedgerEvent[] {
    const tracked = getTrackedActiveOpenClawTurns(sessionId);
    let nextSeq = Math.max(lastSeq, getMaxOpenClawStoredSessionEventSeq(sessionId));
    const syntheticEvents: OpenClawLedgerEvent[] = [];
    for (const turn of turns) {
      if (!shouldSynthesizeTerminalForOrphanedRunningTurn(turn, stats.get(turn.turn_id), tracked)) {
        continue;
      }
      nextSeq += 1;
      syntheticEvents.push(appendSyntheticTerminalLedgerEventForOrphanedTurn(sessionId, turn, nextSeq));
    }
    return syntheticEvents;
  }

  function shouldSynthesizeTerminalForOrphanedRunningTurn(
    turn: OpenClawLedgerTurnSnapshot,
    stats: OpenClawLedgerTurnStats | undefined,
    tracked: TrackedActiveOpenClawTurns
  ): boolean {
    if (turn.status !== "running") {
      return false;
    }
    if (String(turn.active_request_id || "").startsWith("history:")) {
      return false;
    }
    if (tracked.turnIds.has(turn.turn_id) || (turn.run_id ? tracked.runIds.has(turn.run_id) : false)) {
      return false;
    }
    if (!stats?.has_visible_part) {
      return false;
    }
    const lastCreatedAtMs = stats.last_created_at_ms;
    if (!lastCreatedAtMs || Date.now() - lastCreatedAtMs < OPENCLAW_ORPHAN_RUNNING_TURN_TERMINAL_MS) {
      return false;
    }
    return true;
  }

  function appendSyntheticTerminalLedgerEventForOrphanedTurn(
    sessionId: string,
    turn: OpenClawLedgerTurnSnapshot,
    seq: number
  ): OpenClawLedgerEvent {
    const createdAt = new Date().toISOString();
    const reason = "orphaned_running_turn_after_host_restart";
    const rawEventRef = `synthetic:${reason}:${turn.turn_id}:${seq}`;
    const ledger: OpenClawLedgerEvent = {
      protocol_version: "openclaw.ledger.v1",
      seq,
      session_id: sessionId,
      conversation_id: sessionId,
      turn_id: turn.turn_id,
      ...(turn.run_id ? { run_id: turn.run_id } : {}),
      active_request_id: turn.active_request_id,
      part_id: `${turn.turn_id}:status`,
      part_type: "status",
      event_type: "turn.interrupted",
      operation: "close",
      visibility: "hidden",
      terminal_status: "interrupted",
      created_at: createdAt,
      raw_event_ref: rawEventRef,
      payload: {
        synthetic: true,
        synthetic_terminal: true,
        synthetic_reason: reason,
        terminal_status: "interrupted"
      }
    };
    const event: TimelineEvent = {
      id: `${sessionId}:synthetic:${reason}:${seq}`,
      sessionId,
      seq,
      kind: "run.interrupted",
      payload: {
        synthetic: true,
        synthetic_terminal: true,
        synthetic_reason: reason,
        terminal_status: "interrupted",
        active_request_id: turn.active_request_id,
        turn_id: turn.turn_id,
        ...(turn.run_id ? { runId: turn.run_id, run_id: turn.run_id } : {}),
        openclaw_ledger: ledger
      },
      createdAt
    };
    appendSyntheticSessionEvent(sessionId, event);
    traceOpenClawLedger(input.logger, "synthetic-terminal", {
      sessionId,
      reason,
      turnId: turn.turn_id,
      runId: turn.run_id,
      activeRequestId: turn.active_request_id,
      ledger: summarizeOpenClawLedgerEvent(ledger)
    }, input.config);
    return ledger;
  }

  function getOpenClawLedgerCreatedAtMs(event: OpenClawLedgerEvent): number {
    const time = Date.parse(event.created_at);
    return Number.isFinite(time) ? time : 0;
  }

  function isOpenClawLedgerVisiblePart(event: OpenClawLedgerEvent): boolean {
    return event.part_type === "answer" || event.part_type === "thinking" || event.part_type === "tool" || event.part_type === "output_file";
  }

  function getMaxOpenClawStoredSessionEventSeq(sessionId: string): number {
    return listCanonicalSessionEvents(sessionId).reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
  }

  function selectOpenClawActiveTurnsForSnapshot(
    sessionId: string,
    turns: OpenClawLedgerTurnSnapshot[],
    lastSeq: number
  ): OpenClawLedgerTurnSnapshot[] {
    const terminalRunIds = new Set(
      turns
        .filter((turn) => turn.status !== "running" && turn.run_id)
        .map((turn) => String(turn.run_id))
    );
    const runningTurns = turns.filter((turn) => {
      if (turn.status !== "running") {
        return false;
      }
      if (
        turn.run_id &&
        terminalRunIds.has(turn.run_id) &&
        String(turn.active_request_id || "").startsWith("history:")
      ) {
        traceOpenClawLedger(input.logger, "stale-history-active-turn", {
          sessionId,
          turnId: turn.turn_id,
          runId: turn.run_id,
          activeRequestId: turn.active_request_id,
          lastSeq: turn.last_seq
        }, input.config);
        return false;
      }
      return true;
    });
    if (!runningTurns.length) {
      return [];
    }

    const tracked = getTrackedActiveOpenClawTurns(sessionId);
    if (tracked.turnIds.size || tracked.runIds.size) {
      return runningTurns
        .filter((turn) => tracked.turnIds.has(turn.turn_id) || (turn.run_id ? tracked.runIds.has(turn.run_id) : false))
        .map((turn) => ({
          ...turn,
          active_request_id:
            tracked.requestIdByTurnId.get(turn.turn_id) ||
            (turn.run_id ? tracked.requestIdByRunId.get(turn.run_id) : "") ||
            turn.active_request_id
        }));
    }

    const latestRunningSeq = runningTurns.reduce((maxSeq, turn) => Math.max(maxSeq, turn.last_seq || 0), 0);
    if (latestRunningSeq <= 0 || latestRunningSeq !== lastSeq) {
      return [];
    }
    return runningTurns.filter((turn) => turn.last_seq === latestRunningSeq);
  }

  function getTrackedActiveOpenClawTurns(sessionId: string): TrackedActiveOpenClawTurns {
    const tracked: TrackedActiveOpenClawTurns = {
      turnIds: new Set<string>(),
      runIds: new Set<string>(),
      requestIdByTurnId: new Map<string, string>(),
      requestIdByRunId: new Map<string, string>()
    };

    for (const [reqId, details] of activeRequestDetailsBySession.get(sessionId) ?? []) {
      const scope = details.eventScope;
      const turnIds = [
        scope.currentTurnId,
        scope.turnId,
        scope.currentRunId ? buildOpenClawTimelineTurnId(sessionId, scope.currentRunId) : "",
        buildOpenClawTimelineTurnId(sessionId, reqId)
      ].filter((turnId): turnId is string => Boolean(turnId));

      for (const turnId of turnIds) {
        tracked.turnIds.add(turnId);
        tracked.requestIdByTurnId.set(turnId, reqId);
      }

      if (scope.currentRunId) {
        tracked.runIds.add(scope.currentRunId);
        tracked.requestIdByRunId.set(scope.currentRunId, reqId);
      }
    }

    return tracked;
  }

  function getTimelineEventCacheKey(event: TimelineEvent): string {
    return event.id || `${event.sessionId}:${event.seq}:${event.kind}`;
  }

  function resolveOpenClawTimelineTurnId(
    event: TimelineEvent,
    eventScope: GatewayEventScope,
    payloadTimeline: Record<string, unknown>
  ): string {
    const payload = toRecord(event.payload);
    const runIdentity = identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id
    );
    const explicitTurnId = stringOr(payloadTimeline.turn_id, payload.turn_id);
    if (eventScope.activeRequestId && eventScope.activeRequestId !== "events") {
      if (runIdentity) {
        eventScope.currentRunId = runIdentity;
      }
      const turnId = eventScope.currentTurnId || eventScope.turnId || explicitTurnId;
      eventScope.currentTurnId = turnId;
      return turnId;
    }

    if (explicitTurnId) {
      if (runIdentity) {
        eventScope.currentRunId = runIdentity;
      }
      eventScope.currentTurnId = explicitTurnId;
      return explicitTurnId;
    }

    if (runIdentity) {
      eventScope.currentRunId = runIdentity;
      const turnId = buildOpenClawTimelineTurnId(event.sessionId || "", runIdentity);
      eventScope.currentTurnId = turnId;
      return turnId;
    }

    if (eventScope.currentTurnId) {
      return eventScope.currentTurnId;
    }

    const historyIdentity = identityStringOr(
      payload.messageSeq,
      payload.message_seq,
      payload.rawSeq,
      payload.raw_seq,
      readHistoryMessageSeq(event)
    );
    const turnId = historyIdentity
      ? buildOpenClawTimelineTurnId(event.sessionId || "", historyIdentity)
      : eventScope.currentTurnId || eventScope.turnId;
    eventScope.currentTurnId = turnId;
    return turnId;
  }

  function buildOpenClawTimelineMeta(
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ): OpenClawTimelineV2Meta {
    const cacheKey = getTimelineEventCacheKey(event);
    const cached = eventScope.timelineMetaByEventKey.get(cacheKey);
    if (cached) {
      return cached;
    }

    const payload = toRecord(event.payload);
    const payloadTimeline = readOpenClawTimelineFromEvent(event);
    const turnId = resolveOpenClawTimelineTurnId(event, eventScope, payloadTimeline);
    const payloadSegmentId = stringOr(payloadTimeline.segment_id, payload.segment_id);
    const payloadSegmentType = stringOr(payloadTimeline.segment_type, payload.segment_type);
    const segmentType = normalizeOpenClawSegmentType(payloadSegmentType) || getDefaultOpenClawSegmentType(event);
    const segmentId =
      segmentType === "answer"
        ? getDefaultOpenClawSegmentId(event, turnId, segmentType, eventScope)
        : payloadSegmentId || getDefaultOpenClawSegmentId(event, turnId, segmentType, eventScope);
    const segmentIndex = getOpenClawSegmentIndex(segmentId, eventScope);
    const deltaIndex = getNextOpenClawDeltaIndex(segmentId, eventScope);
    const operation = getDefaultOpenClawOperation(event);
    const visibility = getDefaultOpenClawVisibility(event);
    const timeline: OpenClawTimelineV2Meta = {
      protocol_version: "openclaw.timeline.v2",
      turn_id: turnId,
      segment_id: segmentId,
      segment_type: segmentType,
      segment_index: segmentIndex,
      delta_index: deltaIndex,
      operation,
      visibility,
      final: visibility === "final"
    };
    eventScope.timelineMetaByEventKey.set(cacheKey, timeline);
    return timeline;
  }

  function buildOpenClawOutputFilesTimelineMeta(
    eventScope: GatewayEventScope,
    files: Hub53AIOutputFile[]
  ): OpenClawTimelineV2Meta {
    const fileKey = files
      .map((file) => getOutputFilePartIdentityKey(file))
      .filter(Boolean)
      .sort()
      .join(",");
    const turnId = eventScope.currentTurnId || eventScope.turnId;
    const segmentId = `${turnId}:output_files:${fileKey || "generated"}`;
    return {
      protocol_version: "openclaw.timeline.v2",
      turn_id: turnId,
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

  function getExpectedOpenClawSegmentType(event: TimelineEvent): OpenClawTimelineV2SegmentType | undefined {
    const kind = event.kind;
    if (kind === "assistant.delta" || kind === "assistant.message") return "answer";
    if (kind === "assistant.thinking") return "thinking";
    if (kind === "tool.call") return "tool_call";
    if (kind === "tool.result") return "tool_result";
    if (kind === "run.started" || kind === "run.completed" || kind === "run.failed" || kind === "run.interrupted") return "run";
    if (kind === "process.step" && isOutputFilesProcessStepEvent(event)) return "output_files";
    return undefined;
  }

  function isOutputFilesProcessStepEvent(event: TimelineEvent): boolean {
    if (event.kind !== "process.step") {
      return false;
    }
    const payload = toRecord(event.payload);
    const processStep = toRecord(payload.process_step);
    return processStep.step_code === "output_files" && processStep.status === "completed";
  }

  function normalizeTimelineEventSegmentType(event: TimelineEvent): TimelineEvent {
    const expectedSegmentType = getExpectedOpenClawSegmentType(event);
    if (!expectedSegmentType) {
      return event;
    }

    const payload = toRecord(event.payload);
    const timeline = readOpenClawTimelineFromEvent(event);
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

  function getDefaultOpenClawSegmentType(event: TimelineEvent): OpenClawTimelineV2SegmentType {
    const kind = event.kind;
    if (kind === "assistant.delta" || kind === "assistant.message") return "answer";
    if (kind === "assistant.thinking") return "thinking";
    if (kind === "tool.call") return "tool_call";
    if (kind === "tool.result") return "tool_result";
    if (kind === "process.step" && isOutputFilesProcessStepEvent(event)) return "output_files";
    return "run";
  }

  function getDefaultOpenClawSegmentId(
    event: GatewayEvent,
    turnId: string,
    segmentType: OpenClawTimelineV2SegmentType,
    eventScope: GatewayEventScope
  ) {
    if (segmentType === "answer") {
      return resolveOpenClawAnswerSegmentId(turnId, eventScope);
    }
    if (segmentType === "output_files") {
      const fileKey = extractOutputFilesFromPayload(event.payload)
        .map((file) => getOutputFilePartIdentityKey(file))
        .filter(Boolean)
        .sort()
        .join(",");
      return `${turnId}:output_files:${fileKey || event.seq || event.id || "generated"}`;
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

  function resolveOpenClawAnswerSegmentId(turnId: string, eventScope: GatewayEventScope): string {
    eventScope.currentAnswerSegmentId = `${turnId}:answer:0`;
    eventScope.nextAnswerSegmentIndex = Math.max(eventScope.nextAnswerSegmentIndex, 1);
    eventScope.answerBoundaryAfterVisibleResponse = false;
    return eventScope.currentAnswerSegmentId;
  }

  function getDefaultOpenClawOperation(event: GatewayEvent): OpenClawTimelineV2Operation {
    if (event.kind === "assistant.delta") {
      return isReplyReplaceEvent(event) ? "replace" : "append";
    }
    if (event.kind === "run.completed" || event.kind === "run.failed" || event.kind === "run.interrupted") return "close";
    return "replace";
  }

  function getDefaultOpenClawVisibility(event: GatewayEvent): OpenClawTimelineV2Visibility {
    if (event.kind === "assistant.delta") return "stream";
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
    const timeline =
      getExpectedOpenClawSegmentType(event) === "output_files"
        ? buildOpenClawTimelineMeta(event, eventScope)
        : undefined;
    await sendOutputFiles(reqId, sessionId, files, eventScope, timeline, event);
  }

  function rememberWriteToolOutputFileCandidate(event: GatewayEvent, eventScope: GatewayEventScope) {
    if (event.kind !== "tool.call") {
      return;
    }
    const toolCallId = getOpenClawToolCallId(event);
    if (!toolCallId) {
      return;
    }
    const files = extractWriteToolOutputFiles(event);
    if (files.length === 0) {
      return;
    }
    eventScope.writeOutputFilesByToolCallId.set(toolCallId, files);
  }

  async function sendWriteToolOutputFilesForEvent(
    reqId: string,
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ) {
    if (!isSuccessfulWriteToolResultEvent(event)) {
      return;
    }
    const files = consumeWriteToolOutputFiles(event, eventScope);
    if (files.length === 0) {
      return;
    }
    await sendOutputFiles(reqId, sessionId, files, eventScope);
  }

  function appendWriteToolOutputFilesForHistoryEvent(
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ): boolean {
    if (!isSuccessfulWriteToolResultEvent(event)) {
      return false;
    }
    const files = consumeWriteToolOutputFiles(event, eventScope);
    if (files.length === 0) {
      return false;
    }
    return appendCanonicalOutputFilesEvent(sessionId, files, eventScope, event);
  }

  function consumeWriteToolOutputFiles(event: GatewayEvent, eventScope: GatewayEventScope): Hub53AIOutputFile[] {
    const toolCallId = getOpenClawToolCallId(event);
    if (!toolCallId) {
      return [];
    }
    const files = eventScope.writeOutputFilesByToolCallId.get(toolCallId) ?? [];
    eventScope.writeOutputFilesByToolCallId.delete(toolCallId);
    return files;
  }

  function extractWriteToolOutputFiles(event: GatewayEvent): Hub53AIOutputFile[] {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const args = toRecord(data.args);
    const toolName = stringOr(data.name, data.toolName, payload.name, payload.toolName).toLowerCase();
    if (toolName !== "write") {
      return [];
    }
    const path = stringOr(args.path, args.file_path, args.filePath, args.filename, args.fileName);
    const content = typeof args.content === "string" ? args.content : "";
    if (!path || !content) {
      return [];
    }
    return [
      {
        id: `local:${path}`,
        file_name: basename(path),
        mime_type: "text/plain",
        size: Buffer.byteLength(content, "utf8"),
        content,
        source_kind: "tool.write"
      }
    ];
  }

  function isSuccessfulWriteToolResultEvent(event: GatewayEvent): boolean {
    if (event.kind !== "tool.result") {
      return false;
    }
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const toolName = stringOr(data.name, data.toolName, payload.name, payload.toolName).toLowerCase();
    if (toolName !== "write") {
      return false;
    }
    if (data.isError === true || data.error || payload.error) {
      return false;
    }
    const phase = stringOr(data.phase, payload.phase).toLowerCase();
    return !phase || phase === "result" || phase === "completed" || phase === "done";
  }

  function getOpenClawToolCallId(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    return stringOr(
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
    eventScope: GatewayEventScope,
    timeline?: OpenClawTimelineV2Meta,
    sourceEvent?: GatewayEvent
  ): Promise<boolean> {
    if (files.length === 0) {
      return false;
    }

    const freshFiles = files.filter((file) => {
      const key = getOutputFileEmissionKey(file);
      if (key && eventScope.emittedOutputFileKeys.has(key)) {
        return false;
      }
      if (key) {
        eventScope.emittedOutputFileKeys.add(key);
      }
      return true;
    });
    if (freshFiles.length === 0) {
      return false;
    }

    const outputTimeline = timeline ?? buildOpenClawOutputFilesTimelineMeta(eventScope, freshFiles);
    const ledgerSourceEvent = sourceEvent ?? {
      id: `synthetic:output_files:${outputTimeline.segment_id}`,
      sessionId,
      seq: eventScope.lastSeqSeen,
      kind: "process.step",
      payload: {
        process_step: {
          step_code: "output_files",
          status: "completed",
          data: { files: freshFiles }
        }
      },
      createdAt: new Date().toISOString()
    };
    const outputLedger = buildOpenClawLedgerEvent(ledgerSourceEvent, eventScope, outputTimeline);
    if (eventScope.activeRequestId && eventScope.activeRequestId !== "events") {
      appendCanonicalSessionEvent(sessionId, buildCanonicalOutputFilesTimelineEvent(ledgerSourceEvent, outputTimeline, outputLedger));
    }
    await sendQueuedFrame(
      buildOutputFilesProcessStep(
        reqId,
        freshFiles,
        sessionId,
        outputTimeline,
        outputLedger
      )
    );
    return true;
  }

  function appendCanonicalOutputFilesEvent(
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope,
    sourceEvent?: GatewayEvent
  ): boolean {
    if (files.length === 0 || eventScope.activeRequestId === "events") {
      return false;
    }
    const freshFiles = files.filter((file) => {
      const key = getOutputFileEmissionKey(file);
      if (key && eventScope.emittedOutputFileKeys.has(key)) {
        return false;
      }
      if (key) {
        eventScope.emittedOutputFileKeys.add(key);
      }
      return true;
    });
    if (freshFiles.length === 0) {
      return false;
    }
    const outputTimeline = buildOpenClawOutputFilesTimelineMeta(eventScope, freshFiles);
    const ledgerSourceEvent = buildSyntheticOutputFilesTimelineEvent(sessionId, freshFiles, eventScope, outputTimeline, sourceEvent);
    const outputLedger = buildOpenClawLedgerEvent(ledgerSourceEvent, eventScope, outputTimeline);
    appendCanonicalSessionEvent(sessionId, buildCanonicalOutputFilesTimelineEvent(ledgerSourceEvent, outputTimeline, outputLedger));
    return true;
  }

  function buildSyntheticOutputFilesTimelineEvent(
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope,
    outputTimeline: OpenClawTimelineV2Meta,
    sourceEvent?: GatewayEvent
  ): TimelineEvent {
    return {
      id: `synthetic:output_files:${outputTimeline.segment_id}`,
      sessionId,
      seq: typeof sourceEvent?.seq === "number" && Number.isFinite(sourceEvent.seq) ? sourceEvent.seq : eventScope.lastSeqSeen,
      kind: "process.step",
      payload: {
        process_step: {
          step_code: "output_files",
          status: "completed",
          data: {
            files,
            ...(sourceEvent ? { source_event_ref: buildOpenClawRawEventRef(sourceEvent) } : {})
          }
        }
      },
      createdAt: sourceEvent?.createdAt || new Date().toISOString()
    };
  }

  function buildCanonicalOutputFilesTimelineEvent(
    event: TimelineEvent,
    outputTimeline: OpenClawTimelineV2Meta,
    outputLedger: OpenClawLedgerEvent
  ): TimelineEvent {
    return {
      ...event,
      payload: attachOpenClawLedgerToPayload(
        event,
        attachOpenClawTimelineToPayload(event, toRecord(event.payload), outputTimeline),
        outputLedger
      )
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
        outbox: Array.isArray(parsed?.outbox) ? parsed.outbox : [],
        canonicalEventsBySession: normalizeStoredSessionEvents(parsed?.canonicalEventsBySession, MAX_CANONICAL_EVENTS_PER_SESSION),
        syntheticEventsBySession: normalizeStoredSessionEvents(parsed?.syntheticEventsBySession, MAX_SYNTHETIC_EVENTS_PER_SESSION)
      };
      hydrateSessionEventMap(canonicalEventsBySession, state.canonicalEventsBySession);
      hydrateSessionEventMap(syntheticEventsBySession, state.syntheticEventsBySession);
      if (normalizeLoadedOpenClawLedgerSequences()) {
        await persistState({ force: true });
      }
    } catch {
      await persistState();
    }
  }

  function normalizeLoadedOpenClawLedgerSequences(): boolean {
    ledgerSeqBySession.clear();
    let changed = false;
    const sessionIds = new Set<string>([
      ...canonicalEventsBySession.keys(),
      ...syntheticEventsBySession.keys()
    ]);

    for (const sessionId of sessionIds) {
      let maxSeq = 0;
      const normalizeList = (events: TimelineEvent[]) =>
        events.map((event) => {
          const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
          if (!ledger) {
            return event;
          }
          const nextSeq = ledger.seq > maxSeq ? ledger.seq : maxSeq + 1;
          maxSeq = nextSeq;
          if (nextSeq === ledger.seq) {
            return event;
          }
          changed = true;
          return rewriteTimelineEventOpenClawLedgerSeq(event, nextSeq);
        });

      const canonical = normalizeList(canonicalEventsBySession.get(sessionId) ?? []);
      const synthetic = normalizeList(syntheticEventsBySession.get(sessionId) ?? []);
      canonicalEventsBySession.set(sessionId, canonical);
      syntheticEventsBySession.set(sessionId, synthetic);
      setPersistedSessionEvents("canonicalEventsBySession", sessionId, canonical, MAX_CANONICAL_EVENTS_PER_SESSION);
      setPersistedSessionEvents("syntheticEventsBySession", sessionId, synthetic, MAX_SYNTHETIC_EVENTS_PER_SESSION);
      ledgerSeqBySession.set(sessionId || "openclaw", maxSeq);
    }

    return changed;
  }

  function rewriteTimelineEventOpenClawLedgerSeq(event: TimelineEvent, seq: number): TimelineEvent {
    const payload = toRecord(event.payload);
    const direct = toRecord(payload.openclaw_ledger);
    if (direct.protocol_version === "openclaw.ledger.v1") {
      return {
        ...event,
        payload: {
          ...payload,
          openclaw_ledger: {
            ...direct,
            seq
          }
        }
      };
    }

    const processStep = toRecord(payload.process_step);
    const processData = toRecord(processStep.data);
    const nested = toRecord(processData.openclaw_ledger);
    if (nested.protocol_version !== "openclaw.ledger.v1") {
      return event;
    }
    return {
      ...event,
      payload: {
        ...payload,
        process_step: {
          ...processStep,
          data: {
            ...processData,
            openclaw_ledger: {
              ...nested,
              seq
            }
          }
        }
      }
    };
  }

  async function persistState(options: { force?: boolean } = {}) {
    if (stopped && !options.force) {
      return;
    }
    const serialized = `${JSON.stringify(state, null, 2)}\n`;
    const nextPersist = persistStateQueue
      .catch(() => undefined)
      .then(() => writePersistedState(serialized));
    persistStateQueue = nextPersist.catch(() => undefined);
    await nextPersist;
  }

  async function writePersistedState(serialized: string) {
    await mkdir(dirname(statePath), { recursive: true });
    const tempPath = `${statePath}.${randomUUID()}.tmp`;
    try {
      await writeFile(tempPath, serialized);
      await rename(tempPath, statePath);
    } catch (error) {
      if (stopped && isMissingPersistStatePathError(error)) {
        input.logger?.warn?.(
          `[53aihub] skipped late persist after bridge stop: ${error instanceof Error ? error.message : String(error)}`
        );
        return;
      }
      throw error;
    }
  }

  function persistStateSoon(reason: string) {
    if (stopped) {
      return;
    }
    void persistState().catch((error) => {
      input.logger?.warn?.(
        `[53aihub] failed to persist ${reason}: ${error instanceof Error ? error.message : String(error)}`
      );
    });
  }

  function isMissingPersistStatePathError(error: unknown): boolean {
    return Boolean(error && typeof error === "object" && "code" in error && error.code === "ENOENT");
  }

  function normalizeStoredSessionEvents(value: unknown, limit: number): Record<string, TimelineEvent[]> {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return {};
    }
    const output: Record<string, TimelineEvent[]> = {};
    for (const [sessionId, events] of Object.entries(value as Record<string, unknown>)) {
      if (!sessionId || !Array.isArray(events)) continue;
      output[sessionId] = events
        .filter((event): event is TimelineEvent => Boolean(event && typeof event === "object"))
        .slice(-limit);
    }
    return output;
  }

  function hydrateSessionEventMap(
    target: Map<string, TimelineEvent[]>,
    source: Record<string, TimelineEvent[]> | undefined
  ) {
    target.clear();
    for (const [sessionId, events] of Object.entries(source || {})) {
      target.set(sessionId, events);
    }
  }

  function setPersistedSessionEvents(
    key: "canonicalEventsBySession" | "syntheticEventsBySession",
    sessionId: string,
    events: TimelineEvent[],
    limit: number
  ) {
    const current = state[key] && typeof state[key] === "object" ? { ...state[key] } : {};
    current[sessionId] = events.slice(-limit);
    state[key] = current;
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

function isConfigDebugFlagEnabled(value: unknown): boolean {
  if (value === true) {
    return true;
  }
  if (typeof value !== "string") {
    return false;
  }
  const normalized = value.toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

function isOpenClawDuplicateTraceEnabled(config?: Hub53AIConfig): boolean {
  if (
    isConfigDebugFlagEnabled(config?.diagnosticLogs) ||
    isConfigDebugFlagEnabled(config?.duplicateTrace) ||
    isConfigDebugFlagEnabled(config?.debug?.all) ||
    isConfigDebugFlagEnabled(config?.debug?.duplicates)
  ) {
    return true;
  }
  const value = String(process.env.OPENCLAW_TRACE_DUPLICATES ?? process.env.OPENCLAW_DIAG_LOGS ?? "").toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

function traceOpenClawDuplicate(
  logger: HubBridgeInput["logger"] | undefined,
  label: string,
  payload: Record<string, unknown>,
  config?: Hub53AIConfig
): void {
  if (!isOpenClawDuplicateTraceEnabled(config)) {
    return;
  }
  const line = `[openclaw-dup-trace] ${label} ${safeTraceJson(payload)}`;
  if (logger?.info) {
    logger.info(line);
    return;
  }
  console.info(line);
}

function isOpenClawLedgerDebugEnabled(config?: Hub53AIConfig): boolean {
  if (
    isConfigDebugFlagEnabled(config?.diagnosticLogs) ||
    isConfigDebugFlagEnabled(config?.ledgerDebug) ||
    isConfigDebugFlagEnabled(config?.debug?.all) ||
    isConfigDebugFlagEnabled(config?.debug?.ledger)
  ) {
    return true;
  }
  const value = String(process.env.OPENCLAW_LEDGER_DEBUG ?? process.env.OPENCLAW_DIAG_LOGS ?? "").toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

function traceOpenClawLedger(
  logger: HubBridgeInput["logger"] | undefined,
  label: string,
  payload: Record<string, unknown>,
  config?: Hub53AIConfig
): void {
  if (!isOpenClawLedgerDebugEnabled(config)) {
    return;
  }
  const line = `[openclaw-ledger] ${label} ${safeTraceJson(payload)}`;
  if (logger?.info) {
    logger.info(line);
    return;
  }
  console.info(line);
}

function summarizeOpenClawSessionSnapshotForTrace(input: {
  sessionId: string;
  afterSeq: number;
  snapshot: OpenClawSessionSnapshot;
  turns: OpenClawLedgerTurnSnapshot[];
  syntheticTerminals: OpenClawLedgerEvent[];
  tracked: TrackedActiveOpenClawTurns;
}): Record<string, unknown> {
  const runningTurns = input.turns.filter((turn) => turn.status === "running");
  const ledgerEvents = input.snapshot.ledger_events ?? input.snapshot.ledgerEvents ?? [];
  return {
    sessionIdHash: hashTraceText(input.sessionId),
    afterSeq: input.afterSeq,
    lastSeq: input.snapshot.last_seq,
    turnCount: input.turns.length,
    runningTurnCount: runningTurns.length,
    turnStatusCounts: countOpenClawSnapshotValues(input.turns, (turn) => turn.status),
    activeTurnCount: input.snapshot.active_turns.length,
    runningActiveTurnCount: input.snapshot.active_turns.filter((turn) => turn.status === "running").length,
    activeTurns: summarizeOpenClawSnapshotTurnsForTrace(input.snapshot.active_turns, 5),
    runningTurns: summarizeOpenClawSnapshotTurnsForTrace(runningTurns, 5),
    trackedTurnCount: input.tracked.turnIds.size,
    trackedRunCount: input.tracked.runIds.size,
    recentEventCount: input.snapshot.recent_events.length,
    recentEventMaxSeq: maxOpenClawLedgerEventSeq(input.snapshot.recent_events),
    recentEventTypeCounts: countOpenClawSnapshotValues(input.snapshot.recent_events, (event) => event.event_type),
    recentTerminalStatusCounts: countOpenClawSnapshotValues(input.snapshot.recent_events, (event) => event.terminal_status, true),
    recentTailEvents: summarizeOpenClawSnapshotEventsForTrace(input.snapshot.recent_events, 3),
    ledgerEventCount: ledgerEvents.length,
    ledgerEventMaxSeq: maxOpenClawLedgerEventSeq(ledgerEvents),
    ledgerEventTypeCounts: countOpenClawSnapshotValues(ledgerEvents, (event) => event.event_type),
    ledgerTerminalStatusCounts: countOpenClawSnapshotValues(ledgerEvents, (event) => event.terminal_status, true),
    ledgerTailEvents: summarizeOpenClawSnapshotEventsForTrace(ledgerEvents, 3),
    syntheticTerminalCount: input.syntheticTerminals.length,
    syntheticTerminalStatuses: countOpenClawSnapshotValues(input.syntheticTerminals, (event) => event.terminal_status, true)
  };
}

function summarizeOpenClawSnapshotTurnsForTrace(turns: OpenClawLedgerTurnSnapshot[], limit: number): Record<string, unknown>[] {
  if (limit <= 0 || !turns.length) {
    return [];
  }
  return turns.slice(Math.max(0, turns.length - limit)).map((turn) => ({
    turnIdHash: hashTraceText(turn.turn_id),
    runIdHash: hashTraceText(turn.run_id || ""),
    activeRequestIdHash: hashTraceText(turn.active_request_id || ""),
    status: turn.status,
    terminalSeq: turn.terminal_seq,
    lastSeq: turn.last_seq,
    partCount: turn.part_ids.length,
    partIdHashes: turn.part_ids.slice(0, 5).map((partId) => hashTraceText(partId))
  }));
}

function summarizeOpenClawSnapshotEventsForTrace(events: OpenClawLedgerEvent[], limit: number): Record<string, unknown>[] {
  if (limit <= 0 || !events.length) {
    return [];
  }
  return events.slice(Math.max(0, events.length - limit)).map((event) => ({
    seq: event.seq,
    turnIdHash: hashTraceText(event.turn_id),
    runIdHash: hashTraceText(event.run_id || ""),
    activeRequestIdHash: hashTraceText(event.active_request_id),
    partIdHash: hashTraceText(event.part_id),
    partType: event.part_type,
    eventType: event.event_type,
    operation: event.operation,
    visibility: event.visibility,
    terminalStatus: event.terminal_status,
    textLength: event.text?.length ?? 0,
    textHash: hashTraceText(event.text || ""),
    rawEventRefHash: hashTraceText(event.raw_event_ref || "")
  }));
}

function countOpenClawSnapshotValues<T>(
  items: T[],
  readValue: (item: T) => string | undefined,
  skipEmpty = false
): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const item of items) {
    const value = String(readValue(item) || "").trim();
    if (!value && skipEmpty) {
      continue;
    }
    const key = value || "unknown";
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function maxOpenClawLedgerEventSeq(events: OpenClawLedgerEvent[]): number {
  return events.reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
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

function summarizeOpenClawLedgerEvent(event: OpenClawLedgerEvent): Record<string, unknown> {
  return {
    seq: event.seq,
    sessionId: event.session_id,
    turnId: event.turn_id,
    runId: event.run_id,
    activeRequestId: event.active_request_id,
    partId: event.part_id,
    partType: event.part_type,
    eventType: event.event_type,
    operation: event.operation,
    visibility: event.visibility,
    terminalStatus: event.terminal_status,
    rawEventRef: event.raw_event_ref,
    textLength: event.text?.length ?? 0,
    textHash: hashTraceText(event.text || "")
  };
}

function summarizeOutgoingFrameForTrace(frame: Hub53AIOutgoingFrame): Record<string, unknown> {
  if ("data" in frame && toRecord(frame.data).object === "chat.completion.chunk") {
    const data = toRecord(frame.data);
    const choice = toRecord((Array.isArray(data.choices) ? data.choices : [])[0]);
    const delta = toRecord(choice.delta);
    const payload = toRecord(data.payload);
    const timeline = toRecord(payload.openclaw_timeline);
    const content =
      typeof delta.content === "string"
        ? delta.content
        : typeof delta.reasoning_content === "string"
          ? delta.reasoning_content
          : "";
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
        clientMessageId: extractOpenClawClientMessageId(openAIReq, metadata, userMessage),
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
      clientMessageId: extractOpenClawClientMessageId(data, userObject),
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

function extractOpenClawClientMessageId(...sources: unknown[]): string | undefined {
  const idKeys = [
    "openclaw_client_message_id",
    "openclawClientMessageId",
    "client_message_id",
    "clientMessageId"
  ];
  for (const source of sources) {
    const record = toRecord(source);
    const id = stringFromKeys(record, idKeys);
    if (id) {
      return id;
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
  const delta =
    status === "thinking"
      ? {
          reasoning_content: text,
          role: "assistant" as const
        }
      : {
          content: text,
          role: "assistant" as const
        };
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
          delta,
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
  timeline?: OpenClawTimelineV2Meta,
  ledger?: OpenClawLedgerEvent
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
          ...(ledger ? { openclaw_ledger: ledger } : {}),
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
    "generatedFiles",
    "process_step",
    "processStep"
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

  for (const key of ["data", "result", "payload", "message", "content", "output", "process_step", "processStep"]) {
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
  const address = file.signed_download_url || file.download_url || file.url || "";
  const contentFingerprint = getOutputFileContentFingerprint(file);
  return [
    file.id ? `id:${file.id}` : "",
    contentFingerprint ? `content:${file.file_name}:${contentFingerprint}` : "",
    address || file.file_name ? `url-name:${address}|${file.file_name}` : "",
    file.file_name ? `name:${file.file_name}` : ""
  ].filter(Boolean);
}

function getOutputFilePartIdentityKey(file: Hub53AIOutputFile): string {
  const address = file.signed_download_url || file.download_url || file.url || "";
  const id = typeof file.id === "string" ? file.id.trim() : "";
  if (file.file_name) {
    return `name:${file.file_name}`;
  }
  if (id.startsWith("local:")) {
    return `local-path:${id.slice("local:".length)}`;
  }
  if (address) {
    return `address:${address}|${file.file_name || ""}`;
  }
  if (id && !id.startsWith("local-") && !id.startsWith("local-history-")) {
    return `id:${id}`;
  }
  return id ? `id:${id}` : "";
}

function getOutputFileEmissionKey(file: Hub53AIOutputFile): string {
  const fields = [
    ["id", file.id || ""],
    ["name", file.file_name || ""],
    ["url", file.url || ""],
    ["download", file.download_url || ""],
    ["signed", file.signed_download_url || ""],
    ["mime", file.mime_type || ""],
    ["size", typeof file.size === "number" ? String(file.size) : ""],
    ["content", getOutputFileContentFingerprint(file)],
    ["message", file.message_id || ""],
    ["source", file.source_kind || ""]
  ];
  const hasIdentity = fields.some(([, value]) => value);
  if (!hasIdentity) {
    return "";
  }
  const parts = fields.map(([name, value]) => `${name}:${value}`);
  return `snapshot:${createHash("sha256").update(parts.join("\0")).digest("hex").slice(0, 24)}`;
}

function getOutputFileContentFingerprint(file: Hub53AIOutputFile): string {
  const base64 = typeof file.base64 === "string" ? file.base64 : "";
  if (base64) {
    return createHash("sha256").update("base64\0").update(base64).digest("hex").slice(0, 24);
  }
  const content = typeof file.content === "string" ? file.content : "";
  if (content) {
    return createHash("sha256").update("content\0").update(content).digest("hex").slice(0, 24);
  }
  return "";
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

function numberOr(...values: unknown[]): number | undefined {
  for (const value of values) {
    const parsed = typeof value === "number" ? value : typeof value === "string" && value.trim() ? Number(value) : NaN;
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}
