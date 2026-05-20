import { randomUUID } from "node:crypto";
import { existsSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import WebSocket from "ws";

import type { ControlAction, SessionMessage, SessionSummary, TimelineEvent } from "./models";

export type GatewayConfig = {
  baseUrl: string;
  botId: string;
  secret: string;
  hostKind?: string;
  requestTimeoutMs: number;
  streamReconnectMs: number;
  runtimeRoot?: string;
  exposeRawThinking?: boolean;
  preferResponsesApi?: boolean;
  modelOverride?: string;
};

export type GatewaySession = SessionSummary;
export type GatewayEvent = TimelineEvent;

type GatewayFrame =
  | {
      type: "event";
      event: string;
      seq?: number;
      payload?: Record<string, unknown>;
    }
  | {
      type: "res";
      id: string;
      ok: boolean;
      payload?: unknown;
      error?: {
        code?: string;
        message?: string;
        details?: unknown;
      };
    };

type GatewayClient = {
  listSessions(limit?: number): Promise<GatewaySession[]>;
  createSession(title: string, initialPrompt?: string): Promise<GatewaySession>;
  getSession(sessionId: string): Promise<GatewaySession>;
  getSessionMessages(sessionId: string, limit?: number): Promise<SessionMessage[]>;
  sendMessage(sessionId: string, content: string): Promise<void>;
  controlSession(sessionId: string, action: ControlAction, title?: string): Promise<void>;
  listEvents(sessionId: string, afterSeq?: number): Promise<GatewayEvent[]>;
  subscribe(
    sessionId: string,
    afterSeq: number,
    handlers: {
      onEvent: (event: GatewayEvent) => void;
      onDisconnect: (error?: Error) => void;
    }
  ): () => void;
  stop(): Promise<void>;
};

type GatewayRequestOptions = {
  timeoutMs?: number | null;
};

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout | null;
};

type SessionSubscription = {
  handlers: Set<{
    onEvent: (event: GatewayEvent) => void;
    onDisconnect: (error?: Error) => void;
  }>;
  lastSeq: number;
  openedAtMs: number;
  activeRunIds: Set<string>;
  renderedAssistantTextByRun: Map<string, string>;
  renderedThinkingTextByRun: Map<string, string>;
};

const DEFAULT_SCOPES = ["operator.read", "operator.write"];
const MAX_DELTA_CHUNK_CHARS = 240;
const MIN_DELTA_CHUNK_CHARS = 80;

type RpcTransport = {
  request(method: string, params: Record<string, unknown>, options?: GatewayRequestOptions): Promise<any>;
  onEvent(listener: (frame: Extract<GatewayFrame, { type: "event" }>) => void): void;
  onDisconnect(listener: (error?: Error) => void): void;
  stop(): Promise<void>;
};

export function createGatewayClient(config: Partial<GatewayConfig>): GatewayClient {
  const resolved = resolveGatewayConfig(config);
  const transport = createTransport(resolved);
  const subscriptions = new Map<string, SessionSubscription>();
  const responsesRuns = new Map<string, AbortController>();

  const ensureSubscribed = async (sessionId: string) => {
    await transport.request("sessions.subscribe", {});
    await transport.request("sessions.messages.subscribe", { key: sessionId });
  };

  const emitEvents = (sessionId: string, events: GatewayEvent[]) => {
    const subscription = subscriptions.get(sessionId);
    if (!subscription) {
      return;
    }

    for (const event of events) {
      if (event.seq <= subscription.lastSeq) {
        continue;
      }
      subscription.lastSeq = Math.max(subscription.lastSeq, event.seq);
      for (const handler of subscription.handlers) {
        handler.onEvent(event);
      }
    }
  };

  transport.onEvent((frame) => {
    const sessionId = extractSessionKey(frame.payload);
    if (!sessionId) {
      return;
    }
    const subscription = subscriptions.get(sessionId);
    if (!subscription) {
      return;
    }

    if (isStaleChatFrame(frame, subscription)) {
      return;
    }

    const events = mapGatewayFrameToEvents(frame, subscription.lastSeq, subscription, resolved.exposeRawThinking);
    updateActiveRunTracking(frame, subscription);
    emitEvents(sessionId, events);
  });

  transport.onDisconnect((error) => {
    for (const subscription of subscriptions.values()) {
      for (const handler of subscription.handlers) {
        handler.onDisconnect(error);
      }
    }
  });

  return {
    async listSessions(limit = 50) {
      const payload = await transport.request("sessions.list", {
        limit,
        includeGlobal: true,
        includeUnknown: true,
        includeDerivedTitles: true,
        includeLastMessage: true
      });
      return extractSessions(payload, resolved.hostKind);
    },
    async createSession(title) {
      const payload = await transport.request("sessions.create", {
        label: title,
        agentId: "main"
      });
      return {
        ...normalizeSession(extractCreatedSession(payload), title, resolved.hostKind),
        title
      };
    },
    async getSession(sessionId) {
      const sessions = await this.listSessions(200);
      const match = sessions.find((session) => session.id === sessionId);
      return match ?? fallbackSession(sessionId, resolved.hostKind);
    },
    async getSessionMessages(sessionId, limit = 200) {
      const payload = await transport.request("chat.history", {
        sessionKey: sessionId,
        limit
      });
      return extractMessages(sessionId, payload);
    },
    async sendMessage(sessionId, content) {
      if (resolved.preferResponsesApi) {
        try {
          await startResponsesApiRun({
            config: resolved,
            sessionId,
            content,
            activeRuns: responsesRuns,
            nextSeq: () => (subscriptions.get(sessionId)?.lastSeq ?? 0) + 1,
            emit: (event) => emitEvents(sessionId, [event])
          });
          return;
        } catch (error) {
          if (!isResponsesApiUnavailableError(error)) {
            throw error;
          }
        }
      }

      await transport.request("chat.send", {
        sessionKey: sessionId,
        message: content,
        deliver: false,
        idempotencyKey: randomUUID()
      });
    },
    async controlSession(sessionId, action, title) {
      if (action === "stop") {
        const activeRun = responsesRuns.get(sessionId);
        if (activeRun) {
          activeRun.abort();
          responsesRuns.delete(sessionId);
          emitEvents(sessionId, [
            {
              id: `${sessionId}:responses:interrupted:${Date.now()}`,
              sessionId,
              seq: (subscriptions.get(sessionId)?.lastSeq ?? 0) + 1,
              kind: "run.interrupted",
              payload: { transport: "responses-http", reason: "aborted by user" },
              createdAt: new Date().toISOString()
            }
          ]);
          return;
        }

        await transport.request("chat.abort", {
          sessionKey: sessionId
        });
        return;
      }
      if (action === "rename") {
        await transport.request("sessions.patch", {
          key: sessionId,
          label: title ?? "Renamed session"
        });
        return;
      }
      if (action === "archive") {
        return;
      }
      if (action === "retry") {
        const messages = await this.getSessionMessages(sessionId, 50);
        const lastUserMessage = [...messages].reverse().find((message) => message.role === "user");
        if (!lastUserMessage?.content.trim()) {
          throw new Error("cannot retry a session without a previous user message");
        }
        await this.sendMessage(sessionId, lastUserMessage.content);
      }
    },
    async listEvents(sessionId, afterSeq = 0) {
      const history = await transport.request("chat.history", {
        sessionKey: sessionId,
        limit: 200
      });
      return synthesizeEventsFromHistory(sessionId, history, afterSeq);
    },
    subscribe(sessionId, afterSeq, handlers) {
      const subscription = subscriptions.get(sessionId) ?? {
        handlers: new Set(),
        lastSeq: afterSeq,
        openedAtMs: Date.now(),
        activeRunIds: new Set<string>(),
        renderedAssistantTextByRun: new Map<string, string>(),
        renderedThinkingTextByRun: new Map<string, string>()
      };
      subscription.lastSeq = Math.max(subscription.lastSeq, afterSeq);
      subscription.handlers.add(handlers);
      subscriptions.set(sessionId, subscription);

      void ensureSubscribed(sessionId).catch((error) => {
        handlers.onDisconnect(error instanceof Error ? error : new Error(String(error)));
      });

      return () => {
        const existing = subscriptions.get(sessionId);
        if (!existing) {
          return;
        }
        existing.handlers.delete(handlers);
        if (existing.handlers.size === 0) {
          subscriptions.delete(sessionId);
          void transport
            .request("sessions.messages.unsubscribe", { key: sessionId }, { timeoutMs: 2_000 })
            .catch(() => {});
        }
      };
    },
    async stop() {
      subscriptions.clear();
      for (const run of responsesRuns.values()) {
        run.abort();
      }
      responsesRuns.clear();
      await transport.stop();
    }
  };
}

export function resolveGatewayConfig(config: Partial<GatewayConfig>): GatewayConfig {
  return {
    baseUrl: normalizeGatewayUrl(String(config.baseUrl ?? "")),
    botId: String(config.botId ?? ""),
    secret: String(config.secret ?? ""),
    requestTimeoutMs: Number(config.requestTimeoutMs ?? 15_000),
    streamReconnectMs: Number(config.streamReconnectMs ?? 2_000),
    hostKind: typeof config.hostKind === "string" ? config.hostKind : undefined,
    runtimeRoot: typeof config.runtimeRoot === "string" ? config.runtimeRoot : undefined,
    exposeRawThinking: config.exposeRawThinking !== false,
    preferResponsesApi: config.preferResponsesApi === true,
    modelOverride: typeof config.modelOverride === "string" ? config.modelOverride.trim() : ""
  };
}

function createTransport(config: GatewayConfig): RpcTransport {
  const officialModulePath = resolveOfficialGatewayClientModule(config.runtimeRoot);
  if (officialModulePath && shouldUseOfficialGatewayClient(config)) {
    return createOfficialTransport(config, officialModulePath);
  }
  return new RpcSocketClient(config);
}

function shouldUseOfficialGatewayClient(config: GatewayConfig): boolean {
  try {
    const url = new URL(config.baseUrl);
    return url.port === "28789";
  } catch {
    return false;
  }
}

function resolveOfficialGatewayClientModule(runtimeRoot?: string): string | null {
  const candidates = [
    runtimeRoot ? join(runtimeRoot, "node_modules", "openclaw", "dist") : null,
    runtimeRoot ? resolve(runtimeRoot, "..", "..", "..", "node_modules", "openclaw", "dist") : null,
    process.env.HOME
      ? join(process.env.HOME, "Library", "Application Support", "QClaw", "openclaw", "node_modules", "openclaw", "dist")
      : null
  ].filter((value): value is string => Boolean(value));

  for (const candidate of candidates) {
    if (!existsSync(candidate)) {
      continue;
    }
    const file = readdirSync(candidate).find((entry) => /^client-.*\.js$/.test(entry));
    if (file) {
      return join(candidate, file);
    }
  }

  return null;
}

function createOfficialTransport(config: GatewayConfig, modulePath: string): RpcTransport {
  const listeners = new Set<(frame: Extract<GatewayFrame, { type: "event" }>) => void>();
  const disconnectListeners = new Set<(error?: Error) => void>();
  let clientPromise: Promise<any> | null = null;
  let stopped = false;

  const ensureClient = async () => {
    if (clientPromise) {
      return await clientPromise;
    }

    clientPromise = (async () => {
      const module = (await import(pathToFileURL(modulePath).href)) as Record<string, unknown>;
      const GatewayClientCtor = resolveGatewayClientCtor(module);
      if (!GatewayClientCtor) {
        throw new Error(`could not locate GatewayClient export in ${modulePath}`);
      }

      return await new Promise<any>((resolve, reject) => {
        const client = new GatewayClientCtor({
          url: config.baseUrl,
          token: config.secret,
          clientName: "cli",
          clientDisplayName: "Claw Control Center",
          clientVersion: "claw-control-center",
          mode: "cli",
          scopes: DEFAULT_SCOPES,
          requestTimeoutMs: config.requestTimeoutMs,
          onHelloOk: () => resolve(client),
          onEvent: (event: { event: string; seq?: number; payload?: Record<string, unknown> }) => {
            listeners.forEach((listener) =>
              listener({
                type: "event",
                event: event.event,
                seq: event.seq,
                payload: event.payload
              })
            );
          },
          onConnectError: (error: Error) => {
            clientPromise = null;
            reject(error);
          },
          onClose: (_code: number, reason: string, error?: Error) => {
            clientPromise = null;
            if (stopped) {
              return;
            }
            const disconnectError =
              error instanceof Error ? error : reason ? new Error(reason) : new Error("gateway client closed");
            disconnectListeners.forEach((listener) => listener(disconnectError));
          }
        });

        client.start();
      });
    })();

    return await clientPromise;
  };

  return {
    async request(method, params) {
      const client = await ensureClient();
      return await client.request(method, params);
    },
    onEvent(listener) {
      listeners.add(listener);
    },
    onDisconnect(listener) {
      disconnectListeners.add(listener);
    },
    async stop() {
      stopped = true;
      if (!clientPromise) {
        return;
      }
      const client = await clientPromise.catch(() => null);
      clientPromise = null;
      if (client && typeof client.stopAndWait === "function") {
        await client.stopAndWait({ timeoutMs: 1_000 }).catch(() => client.stop());
        return;
      }
      if (client && typeof client.stop === "function") {
        client.stop();
      }
    }
  };
}

function resolveGatewayClientCtor(module: Record<string, unknown>): (new (opts: any) => any) | null {
  const directCandidates = [module.GatewayClient, module.default, module.t];
  for (const candidate of directCandidates) {
    if (typeof candidate === "function") {
      return candidate as new (opts: any) => any;
    }
  }

  for (const candidate of Object.values(module)) {
    if (typeof candidate === "function") {
      return candidate as new (opts: any) => any;
    }
  }

  return null;
}

class ResponsesApiUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ResponsesApiUnavailableError";
  }
}

type StartResponsesApiRunInput = {
  config: GatewayConfig;
  sessionId: string;
  content: string;
  activeRuns: Map<string, AbortController>;
  nextSeq: () => number;
  emit: (event: GatewayEvent) => void;
};

async function startResponsesApiRun(input: StartResponsesApiRunInput): Promise<void> {
  const controller = new AbortController();
  const runId = `responses:${randomUUID()}`;
  const response = await fetch(responsesApiUrl(input.config.baseUrl), {
    method: "POST",
    signal: controller.signal,
    headers: responsesApiHeaders(input.config, input.sessionId),
    body: JSON.stringify({
      model: "openclaw",
      stream: true,
      input: input.content,
      user: input.sessionId,
      metadata: {
        source: "claw-control-center",
        sessionId: input.sessionId
      }
    })
  }).catch((error) => {
    throw new ResponsesApiUnavailableError(error instanceof Error ? error.message : String(error));
  });

  if (response.status === 404 || response.status === 405 || response.status === 501) {
    await response.body?.cancel().catch(() => {});
    throw new ResponsesApiUnavailableError(`responses api unavailable: HTTP ${response.status}`);
  }
  if (!response.ok) {
    const errorText = await response.text().catch(() => "");
    throw new Error(`responses api failed: HTTP ${response.status}${errorText ? ` ${errorText}` : ""}`);
  }

  input.activeRuns.get(input.sessionId)?.abort();
  input.activeRuns.set(input.sessionId, controller);
  void consumeResponsesApiStream({
    response,
    controller,
    runId,
    sessionId: input.sessionId,
    exposeRawThinking: input.config.exposeRawThinking !== false,
    nextSeq: input.nextSeq,
    emit: input.emit
  }).finally(() => {
    if (input.activeRuns.get(input.sessionId) === controller) {
      input.activeRuns.delete(input.sessionId);
    }
  });
}

function responsesApiUrl(baseUrl: string): string {
  const parsed = new URL(baseUrl);
  if (parsed.protocol === "ws:") {
    parsed.protocol = "http:";
  } else if (parsed.protocol === "wss:") {
    parsed.protocol = "https:";
  }
  parsed.pathname = "/v1/responses";
  parsed.search = "";
  parsed.hash = "";
  return parsed.toString();
}

function responsesApiHeaders(config: GatewayConfig, sessionId: string): Record<string, string> {
  const headers: Record<string, string> = {
    "Accept": "text/event-stream",
    "Content-Type": "application/json",
    "x-openclaw-agent-id": "main",
    "x-openclaw-session-key": sessionId,
    "x-openclaw-message-channel": "claw-control-center"
  };
  if (config.secret) {
    headers.Authorization = `Bearer ${config.secret}`;
  }
  if (config.modelOverride) {
    headers["x-openclaw-model"] = config.modelOverride;
  }
  return headers;
}

function isResponsesApiUnavailableError(error: unknown): boolean {
  return error instanceof ResponsesApiUnavailableError;
}

type ConsumeResponsesApiStreamInput = {
  response: Response;
  controller: AbortController;
  runId: string;
  sessionId: string;
  exposeRawThinking: boolean;
  nextSeq: () => number;
  emit: (event: GatewayEvent) => void;
};

async function consumeResponsesApiStream(input: ConsumeResponsesApiStreamInput): Promise<void> {
  let renderedText = "";
  let completed = false;
  let started = false;
  let finalMessageEmitted = false;

  const emit = (kind: string, payload: Record<string, unknown>) => {
    input.emit({
      id: `${input.sessionId}:responses:${kind}:${input.nextSeq()}`,
      sessionId: input.sessionId,
      seq: input.nextSeq(),
      kind,
      payload: {
        ...payload,
        transport: "responses-http",
        runId: input.runId
      },
      createdAt: new Date().toISOString()
    });
  };

  try {
    for await (const sse of iterSseEvents(input.response)) {
      if (sse.done) {
        break;
      }
      const data = parseSseData(sse.data);
      const type = sse.event || readStringFromUnknown(data, ["type"]) || "";
      if (!type) {
        continue;
      }

      if (type === "response.created" || type === "response.in_progress") {
        if (!started) {
          emit("run.started", { eventType: type, responseId: readStringFromUnknown(data, ["response", "id"]) });
          started = true;
        }
        continue;
      }

      if (isReasoningSseEvent(type)) {
        const thinking = extractResponsesTextDelta(data);
        if (thinking.trim()) {
          emit(
            "assistant.thinking",
            buildThinkingPayload(thinking, input.exposeRawThinking, {
              eventType: type,
              state: type.endsWith(".done") ? "final" : "delta",
              mode: type.endsWith(".done") ? "replace" : "append",
              replace: type.endsWith(".done")
            })
          );
        }
        continue;
      }

      if (type === "response.output_text.delta") {
        const text = extractResponsesTextDelta(data);
        if (text.trim()) {
          for (const chunk of splitDeltaText(text)) {
            renderedText += chunk;
            emit("assistant.delta", {
              content: chunk,
              state: "delta",
              mode: "append",
              replace: false,
              eventType: type
            });
          }
        }
        continue;
      }

      if (type === "response.output_text.done") {
        const text = extractResponsesTextDelta(data) || renderedText;
        if (text.trim()) {
          finalMessageEmitted = true;
          emit("assistant.message", {
            content: normalizeFinalAssistantContent(text, renderedText),
            state: "final",
            mode: "replace",
            replace: true,
            eventType: type
          });
        }
        continue;
      }

      const toolEvent = mapResponsesToolEvent(input.sessionId, input.nextSeq(), type, data);
      if (toolEvent) {
        input.emit(toolEvent);
        continue;
      }

      if (type === "response.completed") {
        completed = true;
        if (!finalMessageEmitted) {
          const text = extractResponseOutputText(data) || renderedText;
          if (text.trim()) {
            emit("assistant.message", {
              content: normalizeFinalAssistantContent(text, renderedText),
              state: "final",
              mode: "replace",
              replace: true,
              eventType: type
            });
          }
        }
        emit("run.completed", {
          eventType: type,
          responseId: readStringFromUnknown(data, ["response", "id"]) ?? readStringFromUnknown(data, ["id"]),
          usage: readUnknownPath(data, ["response", "usage"]) ?? readUnknownPath(data, ["usage"])
        });
        continue;
      }

      if (type === "response.failed") {
        completed = true;
        emit("run.failed", {
          eventType: type,
          error:
            readStringFromUnknown(data, ["error", "message"]) ??
            readStringFromUnknown(data, ["response", "error", "message"]) ??
            "responses api stream failed"
        });
      }
    }

    if (!completed && !input.controller.signal.aborted) {
      emit("run.completed", { eventType: "response.stream.done" });
    }
  } catch (error) {
    if (input.controller.signal.aborted) {
      return;
    }
    emit("run.failed", {
      eventType: "response.stream.error",
      error: error instanceof Error ? error.message : String(error)
    });
  }
}

type SseEvent = {
  event: string;
  data: string;
  done: boolean;
};

async function* iterSseEvents(response: Response): AsyncGenerator<SseEvent> {
  const body = response.body;
  if (!body) {
    return;
  }

  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });

    let separatorIndex = findSseSeparator(buffer);
    while (separatorIndex >= 0) {
      const rawEvent = buffer.slice(0, separatorIndex);
      buffer = buffer.slice(buffer[separatorIndex] === "\r" ? separatorIndex + 4 : separatorIndex + 2);
      const event = parseSseEvent(rawEvent);
      if (event) {
        yield event;
      }
      separatorIndex = findSseSeparator(buffer);
    }
  }

  buffer += decoder.decode();
  const trailing = parseSseEvent(buffer);
  if (trailing) {
    yield trailing;
  }
}

function findSseSeparator(buffer: string): number {
  const lf = buffer.indexOf("\n\n");
  const crlf = buffer.indexOf("\r\n\r\n");
  if (lf < 0) {
    return crlf;
  }
  if (crlf < 0) {
    return lf;
  }
  return Math.min(lf, crlf);
}

function parseSseEvent(raw: string): SseEvent | null {
  const lines = raw.split(/\r?\n/);
  let event = "";
  const data: string[] = [];
  for (const line of lines) {
    if (!line || line.startsWith(":")) {
      continue;
    }
    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
    } else if (line.startsWith("data:")) {
      data.push(line.slice("data:".length).trimStart());
    }
  }
  const joinedData = data.join("\n");
  if (!event && !joinedData) {
    return null;
  }
  return {
    event,
    data: joinedData,
    done: joinedData === "[DONE]"
  };
}

function parseSseData(data: string): unknown {
  if (!data || data === "[DONE]") {
    return {};
  }
  try {
    return JSON.parse(data);
  } catch {
    return { text: data };
  }
}

function isReasoningSseEvent(type: string): boolean {
  const normalized = type.toLowerCase();
  return normalized.includes("reasoning") || normalized.includes("thinking");
}

function extractResponsesTextDelta(data: unknown): string {
  return (
    readStringFromUnknown(data, ["delta"]) ??
    readStringFromUnknown(data, ["text"]) ??
    readStringFromUnknown(data, ["content"]) ??
    readStringFromUnknown(data, ["item", "content"]) ??
    readStringFromUnknown(data, ["part", "text"]) ??
    ""
  );
}

function extractResponseOutputText(data: unknown): string {
  const direct =
    readStringFromUnknown(data, ["output_text"]) ??
    readStringFromUnknown(data, ["response", "output_text"]) ??
    readStringFromUnknown(data, ["text"]) ??
    readStringFromUnknown(data, ["response", "text"]);
  if (direct) {
    return direct;
  }

  const output = readUnknownPath(data, ["response", "output"]) ?? readUnknownPath(data, ["output"]);
  if (!Array.isArray(output)) {
    return "";
  }

  return output
    .flatMap((item) => {
      const content = readUnknownPath(item, ["content"]);
      if (!Array.isArray(content)) {
        return [];
      }
      return content.flatMap((part) => {
        const text = readStringFromUnknown(part, ["text"]) ?? readStringFromUnknown(part, ["content"]);
        return text ? [text] : [];
      });
    })
    .join("");
}

function mapResponsesToolEvent(
  sessionId: string,
  seq: number,
  eventType: string,
  data: unknown
): GatewayEvent | null {
  const item = toRecord(readUnknownPath(data, ["item"]) ?? readUnknownPath(data, ["output_item"]));
  const type = typeof item.type === "string" ? item.type : "";
  if (!type.includes("function_call")) {
    return null;
  }

  const name = typeof item.name === "string" ? item.name : "function";
  const kind = eventType.endsWith(".done") ? "tool.result" : "tool.call";
  return {
    id: `${sessionId}:responses:${kind}:${seq}`,
    sessionId,
    seq,
    kind,
    payload: {
      data: {
        name,
        args: item.arguments,
        result: item.output
      },
      eventType,
      transport: "responses-http"
    },
    createdAt: new Date().toISOString()
  };
}

function readStringFromUnknown(value: unknown, path: string[]): string | undefined {
  const current = readUnknownPath(value, path);
  return typeof current === "string" && current.trim() ? current : undefined;
}

function readUnknownPath(value: unknown, path: string[]): unknown {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

class RpcSocketClient {
  private readonly listeners = new Set<(frame: Extract<GatewayFrame, { type: "event" }>) => void>();
  private readonly disconnectListeners = new Set<(error?: Error) => void>();
  private readonly pending = new Map<string, PendingRequest>();
  private socket: WebSocket | null = null;
  private connectPromise: Promise<void> | null = null;
  private ready = false;
  private challengeNonce: string | null = null;
  private stopped = false;

  constructor(private readonly config: GatewayConfig) {}

  onEvent(listener: (frame: Extract<GatewayFrame, { type: "event" }>) => void) {
    this.listeners.add(listener);
  }

  onDisconnect(listener: (error?: Error) => void) {
    this.disconnectListeners.add(listener);
  }

  async request(method: string, params: Record<string, unknown>, options?: GatewayRequestOptions): Promise<any> {
    await this.ensureConnected();
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new Error("gateway websocket is not connected");
    }

    const id = randomUUID();
    const frame = {
      type: "req",
      id,
      method,
      params
    };

    const timeoutMs =
      options?.timeoutMs === null ? null : Math.max(1, Math.floor(options?.timeoutMs ?? this.config.requestTimeoutMs));

    const response = new Promise<unknown>((resolve, reject) => {
      const timeout =
        timeoutMs === null
          ? null
          : setTimeout(() => {
              this.pending.delete(id);
              reject(new Error(`gateway request timeout for ${method}`));
            }, timeoutMs);
      this.pending.set(id, { resolve, reject, timeout });
    });

    this.socket.send(JSON.stringify(frame));
    return await response;
  }

  async stop(): Promise<void> {
    this.stopped = true;
    const socket = this.socket;
    this.socket = null;
    this.ready = false;
    this.challengeNonce = null;
    this.connectPromise = null;
    if (!socket) {
      return;
    }

    await new Promise<void>((resolve) => {
      socket.once("close", () => resolve());
      socket.close();
    });
  }

  private async ensureConnected(): Promise<void> {
    if (this.ready && this.socket?.readyState === WebSocket.OPEN) {
      return;
    }
    if (this.connectPromise) {
      await this.connectPromise;
      return;
    }
    this.connectPromise = this.connect();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  private async connect(): Promise<void> {
    if (this.stopped) {
      throw new Error("gateway client stopped");
    }

    await new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(this.config.baseUrl);
      this.socket = socket;
      this.ready = false;
      this.challengeNonce = null;
      let settled = false;

      const finishError = (error: Error) => {
        if (settled) {
          return;
        }
        settled = true;
        if (this.socket === socket) {
          this.socket = null;
        }
        this.ready = false;
        this.challengeNonce = null;
        reject(error);
      };

      const finishReady = () => {
        if (settled) {
          return;
        }
        settled = true;
        resolve();
      };

      socket.on("open", () => {});
      socket.on("message", (raw) => {
        try {
          const frame = JSON.parse(raw.toString()) as GatewayFrame;
          this.handleFrame(frame, finishReady, finishError);
        } catch (error) {
          finishError(error instanceof Error ? error : new Error(String(error)));
        }
      });
      socket.on("error", (error) => {
        finishError(error instanceof Error ? error : new Error(String(error)));
      });
      socket.on("close", (_code, reason) => {
        const error = reason.length > 0 ? new Error(reason.toString()) : undefined;
        this.flushPending(error ?? new Error("gateway websocket closed"));
        this.ready = false;
        this.challengeNonce = null;
        if (this.socket === socket) {
          this.socket = null;
        }
        if (!settled) {
          finishError(error ?? new Error("gateway websocket closed before handshake completed"));
        }
        for (const listener of this.disconnectListeners) {
          listener(error);
        }
      });
    });
  }

  private handleFrame(
    frame: GatewayFrame,
    markReady: () => void,
    rejectConnect: (error: Error) => void
  ) {
    if (frame.type === "event") {
      if (frame.event === "connect.challenge") {
        const nonce = typeof frame.payload?.nonce === "string" ? frame.payload.nonce : null;
        if (!nonce) {
          rejectConnect(new Error("gateway connect challenge missing nonce"));
          return;
        }
        this.challengeNonce = nonce;
        this.sendConnect();
        return;
      }

      for (const listener of this.listeners) {
        listener(frame);
      }
      return;
    }

    const pending = this.pending.get(frame.id);
    if (!pending) {
      return;
    }

    this.pending.delete(frame.id);
    if (pending.timeout) {
      clearTimeout(pending.timeout);
    }

    if (!frame.ok) {
      const message = frame.error?.message ?? "gateway request failed";
      const details = frame.error?.details ? `: ${JSON.stringify(frame.error.details)}` : "";
      const error = new Error(`${message}${details}`);
      if (frame.id === "connect-handshake") {
        rejectConnect(error);
        return;
      }
      pending.reject(error);
      return;
    }

    if (frame.id === "connect-handshake") {
      this.ready = true;
      pending.resolve(frame.payload);
      markReady();
      return;
    }

    pending.resolve(frame.payload);
  }

  private sendConnect() {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN || !this.challengeNonce) {
      return;
    }

    const frame = {
      type: "req",
      id: "connect-handshake",
      method: "connect",
      params: {
        minProtocol: 4,
        maxProtocol: 4,
        client: {
          id: "gateway-client",
          displayName: "Claw Control Center",
          version: "claw-control-center",
          platform: process.platform,
          mode: "backend",
          instanceId: "claw-control-center"
        },
        role: "operator",
        scopes: DEFAULT_SCOPES,
        auth: {
          token: this.config.secret
        }
      }
    };

    const timeout = setTimeout(() => {
      this.pending.delete("connect-handshake");
    }, this.config.requestTimeoutMs);
    this.pending.set("connect-handshake", {
      resolve: () => {},
      reject: () => {},
      timeout
    });
    this.socket.send(JSON.stringify(frame));
  }

  private flushPending(error: Error) {
    for (const [id, pending] of this.pending.entries()) {
      if (pending.timeout) {
        clearTimeout(pending.timeout);
      }
      this.pending.delete(id);
      pending.reject(error);
    }
  }
}

function normalizeGatewayUrl(raw: string): string {
  const value = raw.trim();
  if (!value) {
    return "";
  }

  const parsed = new URL(value.includes("://") ? value : `ws://${value}`);
  if (parsed.protocol === "http:") {
    parsed.protocol = "ws:";
  }
  if (parsed.protocol === "https:") {
    parsed.protocol = "wss:";
  }
  if (!parsed.pathname || parsed.pathname === "/") {
    parsed.pathname = "/";
  }
  return parsed.toString().replace(/\/$/, "");
}

function extractSessions(payload: unknown, hostKind = "openclaw"): GatewaySession[] {
  const sessions = Array.isArray((payload as { sessions?: unknown[] } | null)?.sessions)
    ? ((payload as { sessions: unknown[] }).sessions ?? [])
    : [];
  return sessions.map((session) => normalizeSession(session, "Untitled session", hostKind));
}

function extractCreatedSession(payload: unknown): Record<string, unknown> {
  return typeof payload === "object" && payload ? (payload as Record<string, unknown>) : {};
}

function extractMessages(sessionId: string, payload: unknown): SessionMessage[] {
  const rawMessages = Array.isArray((payload as { messages?: unknown[] } | null)?.messages)
    ? ((payload as { messages: unknown[] }).messages ?? [])
    : [];

  return rawMessages
    .map((message) => normalizeMessage(sessionId, message))
    .filter((message): message is SessionMessage => message !== null);
}

function normalizeSession(payload: unknown, fallbackTitle = "Untitled session", hostKind = "openclaw"): GatewaySession {
  const entry = payload && typeof payload === "object" ? (payload as Record<string, unknown>) : {};
  const key = String(entry.key ?? entry.sessionKey ?? entry.sessionId ?? randomUUID());
  const updatedAtMs = Number(entry.updatedAt ?? entry.startedAt ?? Date.now());
  const createdAt = toIsoString(entry.startedAt ?? entry.updatedAt ?? Date.now());
  const updatedAt = toIsoString(updatedAtMs);
  const title = readableSessionTitle(entry, fallbackTitle);

  return {
    id: key,
    title,
    status: normalizeStatus(entry.status),
    hostKind,
    runnerCommand: "openclaw-gateway",
    createdAt,
    updatedAt,
    lastEventSeq: Number(entry.messageSeq ?? 0)
  };
}

function readableSessionTitle(entry: Record<string, unknown>, fallbackTitle: string): string {
  const candidates = [entry.label, entry.displayName, entry.key];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate;
    }
  }
  return fallbackTitle;
}

function normalizeMessage(sessionId: string, payload: unknown): SessionMessage | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const message = payload as Record<string, unknown>;
  const role = typeof message.role === "string" ? message.role : "assistant";
  const content = extractTextContent(message.content) ?? (typeof message.content === "string" ? message.content : "");
  if (!content.trim()) {
    return null;
  }

  const rawSeq =
    message.__openclaw && typeof message.__openclaw === "object"
      ? (message.__openclaw as Record<string, unknown>).seq
      : undefined;
  const seq = typeof rawSeq === "number" ? rawSeq : 0;

  return {
    id: `${sessionId}:${role}:${seq}`,
    sessionId,
    role,
    content,
    createdAt: toIsoString(message.timestamp ?? Date.now())
  };
}

function synthesizeEventsFromHistory(sessionId: string, payload: unknown, afterSeq: number): GatewayEvent[] {
  const rawMessages = Array.isArray((payload as { messages?: unknown[] } | null)?.messages)
    ? ((payload as { messages: unknown[] }).messages ?? [])
    : [];

  return rawMessages
    .flatMap((message) => {
      if (!message || typeof message !== "object") {
        return [];
      }

      const entry = message as Record<string, unknown>;
      const role = typeof entry.role === "string" ? entry.role : "";
      if (role !== "assistant") {
        return [];
      }

      const rawMeta =
        entry.__openclaw && typeof entry.__openclaw === "object"
          ? (entry.__openclaw as Record<string, unknown>)
          : {};
      const seq = typeof rawMeta.seq === "number" ? rawMeta.seq : 0;
      if (seq <= afterSeq) {
        return [];
      }

      return [
        {
          id: `${sessionId}:history:${seq}`,
          sessionId,
          seq,
          kind: "assistant.message",
          payload: {
            content: extractTextContent(entry.content) ?? ""
          },
          createdAt: toIsoString(entry.timestamp ?? Date.now())
        } satisfies GatewayEvent
      ];
    })
    .sort((left, right) => left.seq - right.seq);
}

function mapGatewayFrameToEvents(
  frame: Extract<GatewayFrame, { type: "event" }>,
  lastSeq: number,
  subscription?: SessionSubscription,
  exposeRawThinking = false
): GatewayEvent[] {
  const sessionId = extractSessionKey(frame.payload);
  const payload = toRecord(frame.payload);
  const message = toRecord(payload.message);
  const messageMeta = toRecord(message.__openclaw);
  const session = toRecord(payload.session);
  if (!sessionId) {
    return [];
  }

  if (frame.event === "chat") {
    const state = String(payload.state ?? "delta");
    const deltaText = typeof payload.deltaText === "string" ? payload.deltaText : "";
    const messageContent = extractTextContent(message.content) ?? "";
    let content = state === "delta" ? deltaText || messageContent : messageContent || deltaText;
    const thinking = extractThinkingSignal(payload, message.content);
    if (!content.trim() && !thinking?.trim()) {
      return [];
    }
    const isDelta = state === "delta";
    const runKey = typeof payload.runId === "string" && payload.runId ? payload.runId : sessionId;
    let nextSeq = lastSeq + 1;
    const events: GatewayEvent[] = [];

    if (thinking?.trim()) {
      const previousThinking = subscription?.renderedThinkingTextByRun.get(runKey) ?? "";
      const shouldSkipThinking = isDelta && previousThinking && previousThinking.startsWith(thinking);
      if (!shouldSkipThinking) {
        const replaceThinking =
          payload.replace === true || (isDelta && Boolean(previousThinking) && thinking.startsWith(previousThinking));
        events.push({
          id: `${sessionId}:thinking:${nextSeq}`,
          sessionId,
          seq: nextSeq,
          kind: "assistant.thinking",
          payload: buildThinkingPayload(thinking, exposeRawThinking, {
            state,
            mode: isDelta ? (replaceThinking ? "replace" : "append") : "replace",
            replace: replaceThinking,
            runId: payload.runId,
            rawSeq: payload.seq
          }),
          createdAt: toIsoString(message.timestamp ?? Date.now())
        });
        nextSeq += 1;
        if (isDelta && subscription) {
          subscription.renderedThinkingTextByRun.set(
            runKey,
            replaceThinking ? thinking : `${previousThinking}${thinking}`
          );
        }
      }
    }

    if (!content.trim()) {
      return events;
    }

    const previousRendered = subscription?.renderedAssistantTextByRun.get(runKey) ?? "";
    if (!isDelta && previousRendered.trim()) {
      content = normalizeFinalAssistantContent(content, previousRendered);
    }
    if (isDelta && previousRendered && previousRendered.startsWith(content)) {
      return events;
    }
    const replace = payload.replace === true || (isDelta && Boolean(previousRendered) && content.startsWith(previousRendered));
    const mode = isDelta ? (replace ? "replace" : "append") : "replace";
    const chunks = isDelta && mode === "append" ? splitDeltaText(content) : [content];
    const createdAt = toIsoString(message.timestamp ?? Date.now());
    chunks.forEach((chunk, index) => {
      const seq = nextSeq + index;
      events.push({
        id: `${sessionId}:chat:${seq}`,
        sessionId,
        seq,
        kind: isDelta ? "assistant.delta" : "assistant.message",
        payload: {
          content: chunk,
          state,
          mode,
          replace,
          runId: payload.runId,
          rawSeq: payload.seq,
          ...(isDelta && mode === "append" && chunks.length > 1
            ? {
                syntheticChunk: {
                  index,
                  total: chunks.length
                }
              }
            : {})
        },
        createdAt
      });
    });
    if (isDelta && subscription) {
      subscription.renderedAssistantTextByRun.set(
        runKey,
        mode === "replace" ? content : `${previousRendered}${chunks.join("")}`
      );
    }
    return events;
  }

  if (frame.event === "session.message") {
    const role = typeof message.role === "string" ? message.role : "";
    if (role !== "assistant" && role !== "user") {
      return [];
    }
    const content = extractTextContent(message.content) ?? "";
    const thinking = role === "assistant" ? extractThinkingContent(message.content) : null;
    if (!content.trim() && !thinking?.trim()) {
      return [];
    }
    const rawSeq = Number(payload.messageSeq ?? messageMeta.seq);
    let seq = Number.isFinite(rawSeq) ? Math.max(lastSeq + 1, rawSeq) : lastSeq + 1;
    const createdAt = toIsoString(message.timestamp ?? Date.now());
    const events: GatewayEvent[] = [];
    if (thinking?.trim()) {
      events.push({
        id: `${sessionId}:thinking:${seq}`,
        sessionId,
        seq,
        kind: "assistant.thinking",
        payload: buildThinkingPayload(thinking, exposeRawThinking, {
          state: "final",
          mode: "replace",
          replace: true,
          rawSeq: payload.messageSeq ?? messageMeta.seq
        }),
        createdAt
      });
      seq += 1;
    }
    if (content.trim()) {
      events.push({
        id: `${sessionId}:message:${seq}`,
        sessionId,
        seq,
        kind: role === "user" ? "user.message" : "assistant.message",
        payload: {
          content
        },
        createdAt
      });
    }
    return events;
  }

  if (frame.event === "session.tool") {
    const seq = Number(payload.seq ?? lastSeq + 1);
    const kind = String(payload.phase ?? "tool");
    return [
      {
        id: `${sessionId}:tool:${seq}`,
        sessionId,
        seq,
        kind: kind === "result" ? "tool.result" : "tool.call",
        payload: frame.payload ?? {},
        createdAt: new Date().toISOString()
      }
    ];
  }

  if (frame.event === "sessions.changed") {
    const phase = String(payload.phase ?? "");
    const seq = lastSeq + 1;
    if (phase === "start") {
      return [
        {
          id: `${sessionId}:status:${seq}`,
          sessionId,
          seq,
          kind: "run.started",
          payload,
          createdAt: toIsoString(payload.startedAt ?? payload.ts ?? Date.now())
        }
      ];
    }
    if (phase === "end") {
      const status = String(payload.status ?? session.status ?? "");
      const eventKind =
        status === "done" ? "run.completed" : status === "aborted" ? "run.interrupted" : "status.update";
      return [
        {
          id: `${sessionId}:status:${seq}`,
          sessionId,
          seq,
          kind: eventKind,
          payload,
          createdAt: toIsoString(payload.endedAt ?? payload.ts ?? Date.now())
        }
      ];
    }
    return [
      {
        id: `${sessionId}:status:${seq}`,
        sessionId,
        seq,
        kind: "status.update",
        payload,
        createdAt: toIsoString(payload.updatedAt ?? payload.ts ?? Date.now())
      }
    ];
  }

  return [];
}

function extractSessionKey(payload: unknown): string | null {
  const record = toRecord(payload);
  const session = toRecord(record.session);
  const sessionKey = record.sessionKey ?? session.key;
  return typeof sessionKey === "string" && sessionKey.trim() ? sessionKey : null;
}

function isStaleChatFrame(frame: Extract<GatewayFrame, { type: "event" }>, subscription: SessionSubscription): boolean {
  if (frame.event !== "chat") {
    return false;
  }

  const payload = toRecord(frame.payload);
  const runId = typeof payload.runId === "string" ? payload.runId : "";
  if (runId && subscription.activeRunIds.has(runId)) {
    return false;
  }

  const message = toRecord(payload.message);
  const timestamp = Number(message.timestamp ?? payload.ts ?? 0);
  if (!Number.isFinite(timestamp) || timestamp <= 0) {
    return false;
  }

  return timestamp < subscription.openedAtMs - 5_000;
}

function updateActiveRunTracking(frame: Extract<GatewayFrame, { type: "event" }>, subscription: SessionSubscription) {
  if (frame.event !== "sessions.changed") {
    return;
  }

  const payload = toRecord(frame.payload);
  const runId = typeof payload.runId === "string" ? payload.runId : "";
  if (!runId) {
    return;
  }

  const phase = String(payload.phase ?? "");
  if (phase === "start") {
    subscription.activeRunIds.add(runId);
  } else if (phase === "end") {
    subscription.activeRunIds.delete(runId);
  }
}

function extractTextContent(content: unknown): string | null {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return null;
  }

  const lines = content
    .flatMap((item) => {
      if (!item || typeof item !== "object") {
        return [];
      }
      const record = item as Record<string, unknown>;
      if (record.type === "text" && typeof record.text === "string") {
        return [record.text];
      }
      return [];
    })
    .filter((value) => value.trim().length > 0);

  return lines.length > 0 ? lines.join("\n") : null;
}

function extractThinkingContent(content: unknown): string | null {
  if (!Array.isArray(content)) {
    return null;
  }

  const lines = content
    .flatMap((item) => {
      if (!item || typeof item !== "object") {
        return [];
      }
      const record = item as Record<string, unknown>;
      if (record.type === "thinking" && typeof record.thinking === "string") {
        return [record.thinking];
      }
      return [];
    })
    .filter((value) => value.trim().length > 0);

  return lines.length > 0 ? lines.join("\n") : null;
}

function extractThinkingSignal(payload: Record<string, unknown>, messageContent: unknown): string | null {
  const nestedData = toRecord(payload.data);
  const candidates = [
    payload.thinking,
    payload.thinkingText,
    payload.reasoning,
    payload.reasoningText,
    payload.reasoning_content,
    payload.deltaThinking,
    payload.deltaReasoning,
    nestedData.thinking,
    nestedData.thinkingText,
    nestedData.reasoning,
    nestedData.reasoningText,
    nestedData.reasoning_content,
    extractThinkingContent(messageContent)
  ];

  for (const candidate of candidates) {
    const text = extractStringContent(candidate);
    if (text?.trim()) {
      return text;
    }
  }
  return null;
}

function extractStringContent(value: unknown): string | null {
  if (typeof value === "string") {
    return value;
  }
  if (!Array.isArray(value)) {
    return null;
  }
  const text = value
    .flatMap((entry) => {
      if (!entry || typeof entry !== "object") {
        return [];
      }
      const record = entry as Record<string, unknown>;
      if (typeof record.text === "string") {
        return [record.text];
      }
      if (typeof record.thinking === "string") {
        return [record.thinking];
      }
      if (typeof record.content === "string") {
        return [record.content];
      }
      return [];
    })
    .join("");
  return text || null;
}

function buildThinkingPayload(
  thinking: string,
  exposeRawThinking: boolean,
  extra: Record<string, unknown>
): Record<string, unknown> {
  const content = collapseRepeatedGrowingPrefix(thinking);
  if (exposeRawThinking) {
    return {
      content,
      privateContentOmitted: false,
      rawThinkingVisible: true,
      textLength: content.length,
      ...extra
    };
  }

  return {
    content: "Claw emitted a private thinking update. Raw reasoning is hidden.",
    privateContentOmitted: true,
    textLength: content.length,
    ...extra
  };
}

function normalizeFinalAssistantContent(content: string, previousRendered: string): string {
  const trimmedPrevious = previousRendered.trim();
  const trimmedContent = content.trim();
  if (!trimmedPrevious || !trimmedContent || trimmedContent === trimmedPrevious) {
    return trimmedContent || content;
  }

  const index = trimmedContent.lastIndexOf(trimmedPrevious);
  if (index >= 0) {
    const trailingLength = trimmedContent.length - index - trimmedPrevious.length;
    const endsWithPreviousAnswer = trimmedPrevious.length >= 10 && trailingLength <= 8;
    const previousDominatesSnapshot = trimmedPrevious.length >= Math.min(160, trimmedContent.length * 0.45);
    if (endsWithPreviousAnswer || previousDominatesSnapshot) {
      return trimmedPrevious;
    }
  }

  return content;
}

function collapseRepeatedGrowingPrefix(value: string): string {
  const leadingWhitespace = value.match(/^\s*/)?.[0] ?? "";
  const text = value.trimStart();
  if (text.length < 40) {
    return value;
  }

  for (let split = Math.floor(text.length / 2); split >= 20; split -= 1) {
    const first = text.slice(0, split).trim();
    const second = text.slice(split).trimStart();
    if (first.length >= 20 && second.startsWith(first)) {
      return `${leadingWhitespace}${second}`;
    }
  }

  return value;
}

function splitDeltaText(text: string): string[] {
  if (text.length <= MAX_DELTA_CHUNK_CHARS) {
    return [text];
  }

  const chunks: string[] = [];
  let cursor = 0;
  while (cursor < text.length) {
    const remaining = text.length - cursor;
    if (remaining <= MAX_DELTA_CHUNK_CHARS) {
      chunks.push(text.slice(cursor));
      break;
    }

    const hardLimit = cursor + MAX_DELTA_CHUNK_CHARS;
    const softLimit = cursor + MIN_DELTA_CHUNK_CHARS;
    let splitAt = findReadableSplit(text, softLimit, hardLimit);
    if (splitAt <= cursor) {
      splitAt = hardLimit;
    }

    chunks.push(text.slice(cursor, splitAt));
    cursor = splitAt;
  }

  return chunks;
}

function findReadableSplit(text: string, minIndex: number, maxIndex: number): number {
  const boundedMax = Math.min(maxIndex, text.length);
  const candidates = ["\n\n", "\n", "。", "！", "？", ". ", "! ", "? ", "；", "; ", "，", ", ", " "];
  for (const marker of candidates) {
    const index = text.lastIndexOf(marker, boundedMax);
    if (index >= minIndex) {
      return index + marker.length;
    }
  }
  return boundedMax;
}

function normalizeStatus(status: unknown): GatewaySession["status"] {
  switch (status) {
    case "running":
      return "running";
    case "done":
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "aborted":
    case "interrupted":
      return "interrupted";
    case "archived":
      return "archived";
    default:
      return "idle";
  }
}

function toIsoString(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value).toISOString();
  }
  if (typeof value === "string" && value.trim()) {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      return new Date(asNumber).toISOString();
    }
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString();
    }
  }
  return new Date().toISOString();
}

function fallbackSession(sessionId: string, hostKind = "openclaw"): GatewaySession {
  const now = new Date().toISOString();
  return {
    id: sessionId,
    title: sessionId,
    status: "idle",
    hostKind,
    runnerCommand: "openclaw-gateway",
    createdAt: now,
    updatedAt: now,
    lastEventSeq: 0
  };
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}

export type { GatewayClient };
