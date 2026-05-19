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
  requestTimeoutMs: number;
  streamReconnectMs: number;
  runtimeRoot?: string;
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
};

const DEFAULT_SCOPES = ["operator.read", "operator.write"];

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

  const ensureSubscribed = async (sessionId: string) => {
    await transport.request("sessions.subscribe", {});
    await transport.request("sessions.messages.subscribe", { key: sessionId });
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

    const events = mapGatewayFrameToEvents(frame, subscription.lastSeq);
    for (const event of events) {
      subscription.lastSeq = Math.max(subscription.lastSeq, event.seq);
      for (const handler of subscription.handlers) {
        handler.onEvent(event);
      }
    }
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
      return extractSessions(payload);
    },
    async createSession(title) {
      const payload = await transport.request("sessions.create", {
        label: title,
        agentId: "main"
      });
      return {
        ...normalizeSession(extractCreatedSession(payload), title),
        title
      };
    },
    async getSession(sessionId) {
      const sessions = await this.listSessions(200);
      const match = sessions.find((session) => session.id === sessionId);
      return match ?? fallbackSession(sessionId);
    },
    async getSessionMessages(sessionId, limit = 200) {
      const payload = await transport.request("chat.history", {
        sessionKey: sessionId,
        limit
      });
      return extractMessages(sessionId, payload);
    },
    async sendMessage(sessionId, content) {
      await transport.request("chat.send", {
        sessionKey: sessionId,
        message: content,
        deliver: false,
        idempotencyKey: randomUUID()
      });
    },
    async controlSession(sessionId, action, title) {
      if (action === "stop") {
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
        lastSeq: afterSeq
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
    runtimeRoot: typeof config.runtimeRoot === "string" ? config.runtimeRoot : undefined
  };
}

function createTransport(config: GatewayConfig): RpcTransport {
  const officialModulePath = resolveOfficialGatewayClientModule(config.runtimeRoot);
  if (officialModulePath) {
    return createOfficialTransport(config, officialModulePath);
  }
  return new RpcSocketClient(config);
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
        minProtocol: 3,
        maxProtocol: 3,
        client: {
          id: "cli",
          displayName: "Claw Control Center",
          version: "claw-control-center",
          platform: process.platform,
          mode: "cli"
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

function extractSessions(payload: unknown): GatewaySession[] {
  const sessions = Array.isArray((payload as { sessions?: unknown[] } | null)?.sessions)
    ? ((payload as { sessions: unknown[] }).sessions ?? [])
    : [];
  return sessions.map((session) => normalizeSession(session));
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

function normalizeSession(payload: unknown, fallbackTitle = "Untitled session"): GatewaySession {
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
    hostKind: "qclaw",
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
  lastSeq: number
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
    const content = extractTextContent(message.content) ?? "";
    if (!content.trim()) {
      return [];
    }
    return [
      {
        id: `${sessionId}:chat:${Number(payload.seq ?? lastSeq + 1)}`,
        sessionId,
        seq: Number(payload.seq ?? lastSeq + 1),
        kind: "assistant.delta",
        payload: {
          content,
          state,
          runId: payload.runId
        },
        createdAt: toIsoString(message.timestamp ?? Date.now())
      }
    ];
  }

  if (frame.event === "session.message") {
    const role = typeof message.role === "string" ? message.role : "";
    if (role !== "assistant") {
      return [];
    }
    const content = extractTextContent(message.content) ?? "";
    if (!content.trim()) {
      return [];
    }
    const seq = Number(payload.messageSeq ?? messageMeta.seq ?? lastSeq + 1);
    return [
      {
        id: `${sessionId}:message:${seq}`,
        sessionId,
        seq,
        kind: "assistant.message",
        payload: {
          content,
          thinking: extractThinkingContent(message.content)
        },
        createdAt: toIsoString(message.timestamp ?? Date.now())
      }
    ];
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

function fallbackSession(sessionId: string): GatewaySession {
  const now = new Date().toISOString();
  return {
    id: sessionId,
    title: sessionId,
    status: "idle",
    hostKind: "qclaw",
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
