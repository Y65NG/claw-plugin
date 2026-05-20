import type { OpenClawPluginApi } from "openclaw/plugin-sdk";

export type AgentEventProbeRecord = {
  probeSeq: number;
  runId: string;
  sessionKey?: string;
  stream: string;
  sourceSeq: number;
  createdAt: string;
  dataKeys: string[];
  phase?: string;
  kind?: string;
  textLength?: number;
  textSample?: string;
  privateContentOmitted?: boolean;
};

export type AgentEventProbeSnapshot = {
  available: boolean;
  registered: boolean;
  registrationPath?: string;
  totalEvents: number;
  assistantTextEvents: number;
  assistantTextChars: number;
  eventsByStream: Record<string, number>;
  lastEventAt?: string;
  lastError?: string;
  recentEvents: AgentEventProbeRecord[];
};

export type AgentEventProbe = {
  register(api: OpenClawPluginApi): void;
  getSnapshot(): AgentEventProbeSnapshot;
  getEvents(afterSeq?: number): AgentEventProbeRecord[];
};

type PluginApiWithAgentEvents = OpenClawPluginApi & {
  agent?: {
    events?: {
      registerAgentEventSubscription?: (subscription: unknown) => void;
    };
  };
  registerAgentEventSubscription?: (subscription: unknown) => void;
};

const RECENT_EVENT_LIMIT = 200;
const SAMPLE_LIMIT = 160;

export function createAgentEventProbe(): AgentEventProbe {
  let available = false;
  let registered = false;
  let registrationPath: string | undefined;
  let lastError: string | undefined;
  let probeSeq = 0;
  let totalEvents = 0;
  let assistantTextEvents = 0;
  let assistantTextChars = 0;
  let lastEventAt: string | undefined;
  const eventsByStream: Record<string, number> = {};
  const recentEvents: AgentEventProbeRecord[] = [];

  function register(api: OpenClawPluginApi) {
    const apiWithEvents = api as PluginApiWithAgentEvents;
    const nestedRegister = apiWithEvents.agent?.events?.registerAgentEventSubscription;
    const legacyRegister = apiWithEvents.registerAgentEventSubscription;
    const registerFn = typeof nestedRegister === "function" ? nestedRegister : legacyRegister;
    registrationPath =
      typeof nestedRegister === "function"
        ? "api.agent.events.registerAgentEventSubscription"
        : typeof legacyRegister === "function"
          ? "api.registerAgentEventSubscription"
          : undefined;
    available = typeof registerFn === "function";

    if (!registerFn) {
      lastError = "registerAgentEventSubscription is not available in this host";
      api.logger.warn(lastError);
      return;
    }

    try {
      registerFn({
        id: "claw-control-center.agent-event-probe",
        description: "Records sanitized raw agent event summaries for streaming diagnostics.",
        streams: ["assistant", "tool", "lifecycle", "thinking", "error", "command_output"],
        handle(event: unknown) {
          recordEvent(event);
        }
      });
      registered = true;
      lastError = undefined;
      api.logger.info(`Claw Control Center agent event probe registered via ${registrationPath}`);
    } catch (error) {
      registered = false;
      lastError = error instanceof Error ? error.message : String(error);
      api.logger.warn(`Claw Control Center agent event probe registration failed: ${lastError}`);
    }
  }

  function recordEvent(event: unknown) {
    const record = toRecord(event);
    const data = toRecord(record.data);
    const stream = typeof record.stream === "string" ? record.stream : "unknown";
    const text = extractVisibleText(data);
    const createdAt = toIsoString(record.ts ?? Date.now());
    const isPrivateStream = stream === "thinking";
    const next: AgentEventProbeRecord = {
      probeSeq: ++probeSeq,
      runId: typeof record.runId === "string" ? record.runId : "",
      sessionKey: typeof record.sessionKey === "string" ? record.sessionKey : undefined,
      stream,
      sourceSeq: Number(record.seq ?? 0),
      createdAt,
      dataKeys: Object.keys(data).sort(),
      phase: typeof data.phase === "string" ? data.phase : undefined,
      kind: typeof data.kind === "string" ? data.kind : undefined,
      textLength: text ? text.length : undefined,
      textSample: text && !isPrivateStream ? sampleText(text) : undefined,
      privateContentOmitted: text && isPrivateStream ? true : undefined
    };

    totalEvents += 1;
    eventsByStream[stream] = (eventsByStream[stream] ?? 0) + 1;
    lastEventAt = createdAt;
    if (stream === "assistant" && text) {
      assistantTextEvents += 1;
      assistantTextChars += text.length;
    }

    recentEvents.push(next);
    while (recentEvents.length > RECENT_EVENT_LIMIT) {
      recentEvents.shift();
    }
  }

  return {
    register,
    getSnapshot() {
      return {
        available,
        registered,
        registrationPath,
        totalEvents,
        assistantTextEvents,
        assistantTextChars,
        eventsByStream: { ...eventsByStream },
        lastEventAt,
        lastError,
        recentEvents: recentEvents.slice(-20)
      };
    },
    getEvents(afterSeq = 0) {
      return recentEvents.filter((event) => event.probeSeq > afterSeq);
    }
  };
}

function extractVisibleText(data: Record<string, unknown>): string | undefined {
  const direct = [data.text, data.delta, data.content, data.message];
  for (const value of direct) {
    const text = extractText(value);
    if (text) {
      return text;
    }
  }
  return undefined;
}

function extractText(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim()) {
    return value;
  }
  if (Array.isArray(value)) {
    const chunks = value.flatMap((entry) => {
      const record = toRecord(entry);
      if (typeof record.text === "string") {
        return [record.text];
      }
      if (typeof record.content === "string") {
        return [record.content];
      }
      return [];
    });
    return chunks.length > 0 ? chunks.join("\n") : undefined;
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return extractText(record.text) ?? extractText(record.content);
  }
  return undefined;
}

function sampleText(text: string): string {
  return text.length > SAMPLE_LIMIT ? `${text.slice(0, SAMPLE_LIMIT)}...` : text;
}

function toIsoString(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value).toISOString();
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return new Date(parsed).toISOString();
    }
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString();
    }
  }
  return new Date().toISOString();
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}
