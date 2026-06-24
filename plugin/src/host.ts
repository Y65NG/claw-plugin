export type HostKind = "openclaw" | "qclaw" | "hermes" | "workbuddy";

import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

export type PluginConfig = {
  gateway?: {
    baseUrl?: string;
    botId?: string;
    secret?: string;
    requestTimeoutMs?: number;
    streamReconnectMs?: number;
    preferResponsesApi?: boolean;
    modelOverride?: string;
  };
  hub53ai?: {
    enabled?: boolean;
    botId?: string;
    secret?: string;
    wsUrl?: string;
    accessPolicy?: "open" | "allowlist";
    allowFrom?: string[];
    sendThinkingMessage?: boolean;
    reconnectBaseMs?: number;
    maxReconnectAttempts?: number;
    detectCreatedFiles?: boolean;
    fileWorkspaceDirs?: string[];
    createdFilesMaxFileBytes?: number;
    createdFilesMaxCount?: number;
    createdFilesExclude?: string[];
    artifactUploadTimeoutMs?: number;
    diagnosticLogs?: boolean;
    ledgerDebug?: boolean;
    duplicateTrace?: boolean;
    debug?: {
      all?: boolean;
      ledger?: boolean;
      duplicates?: boolean;
    };
  };
  console?: {
    enabled?: boolean;
    host?: string;
    port?: number;
    showRawThinking?: boolean;
  };
  persistence?: {
    maxSessions?: number;
  };
  logging?: {
    level?: "debug" | "info" | "warn" | "error";
  };
};

export type ResolvedPluginConfig = {
  gateway: {
    baseUrl: string;
    botId: string;
    secret: string;
    requestTimeoutMs: number;
    streamReconnectMs: number;
    preferResponsesApi: boolean;
    modelOverride: string;
  };
  hub53ai: {
    enabled: boolean;
    botId: string;
    secret: string;
    wsUrl: string;
    accessPolicy: "open" | "allowlist";
    allowFrom: string[];
    sendThinkingMessage: boolean;
    reconnectBaseMs: number;
    maxReconnectAttempts: number;
    detectCreatedFiles: boolean;
    fileWorkspaceDirs: string[];
    createdFilesMaxFileBytes: number;
    createdFilesMaxCount: number;
    createdFilesExclude: string[];
    artifactUploadTimeoutMs: number;
    diagnosticLogs: boolean;
    ledgerDebug: boolean;
    duplicateTrace: boolean;
    debug: {
      all: boolean;
      ledger: boolean;
      duplicates: boolean;
    };
  };
  console: {
    enabled: boolean;
    host: string;
    port: number;
    showRawThinking: boolean;
  };
  persistence: {
    maxSessions: number;
  };
  logging: {
    level: "debug" | "info" | "warn" | "error";
  };
};

export type HostRuntimeInfo = {
  modelPrimary?: string;
  enabledSkills: string[];
  cronScheduler?: {
    enabled?: boolean;
    storePath?: string;
    jobCount?: number;
    nextWakeAt?: string;
    lastError?: string;
  };
  cronTasks?: Array<{
    id: string;
    name: string;
    enabled: boolean;
    status?: string;
    agentId?: string;
    schedule?: string;
    nextRunAt?: string;
    lastRunAt?: string;
    payloadKind?: string;
  }>;
};

const SENSITIVE_KEY_PATTERN = /(token|secret|password|key|credential)/i;
const HOST_KIND_BY_MARKER: Array<{ marker: string; kind: HostKind }> = [
  { marker: "/library/application support/qclaw/", kind: "qclaw" },
  { marker: "/.qclaw/", kind: "qclaw" },
  { marker: "/.hermes/", kind: "hermes" },
  { marker: "/.workbuddy/", kind: "workbuddy" },
  { marker: "/.openclaw/", kind: "openclaw" }
];

export function detectHostKind(pathHint?: string): HostKind {
  const normalized = String(pathHint || "")
    .replace(/\\/g, "/")
    .toLowerCase();
  for (const { marker, kind } of HOST_KIND_BY_MARKER) {
    if (normalized.includes(marker) || normalized.endsWith(marker.slice(0, -1))) {
      return kind;
    }
  }
  return "openclaw";
}

export function resolvePluginConfig(config?: PluginConfig): ResolvedPluginConfig {
  return {
    gateway: {
      baseUrl: config?.gateway?.baseUrl?.trim() ?? "",
      botId: config?.gateway?.botId?.trim() ?? "",
      secret: config?.gateway?.secret?.trim() ?? "",
      requestTimeoutMs: config?.gateway?.requestTimeoutMs ?? 15_000,
      streamReconnectMs: config?.gateway?.streamReconnectMs ?? 2_000,
      preferResponsesApi: config?.gateway?.preferResponsesApi ?? false,
      modelOverride: config?.gateway?.modelOverride?.trim() ?? ""
    },
    hub53ai: {
      enabled: config?.hub53ai?.enabled ?? false,
      botId: config?.hub53ai?.botId?.trim() ?? "",
      secret: config?.hub53ai?.secret?.trim() ?? "",
      wsUrl: config?.hub53ai?.wsUrl?.trim() ?? "",
      accessPolicy: config?.hub53ai?.accessPolicy === "allowlist" ? "allowlist" : "open",
      allowFrom: Array.isArray(config?.hub53ai?.allowFrom)
        ? config.hub53ai.allowFrom.map((entry) => String(entry).trim()).filter(Boolean)
        : [],
      sendThinkingMessage: config?.hub53ai?.sendThinkingMessage ?? true,
      reconnectBaseMs: config?.hub53ai?.reconnectBaseMs ?? 2_000,
      maxReconnectAttempts: config?.hub53ai?.maxReconnectAttempts ?? 10,
      detectCreatedFiles: config?.hub53ai?.detectCreatedFiles ?? false,
      fileWorkspaceDirs: Array.isArray(config?.hub53ai?.fileWorkspaceDirs)
        ? config.hub53ai.fileWorkspaceDirs.map((entry) => String(entry).trim()).filter(Boolean)
        : [],
      createdFilesMaxFileBytes: config?.hub53ai?.createdFilesMaxFileBytes ?? 10 * 1024 * 1024,
      createdFilesMaxCount: config?.hub53ai?.createdFilesMaxCount ?? 20,
      createdFilesExclude: Array.isArray(config?.hub53ai?.createdFilesExclude)
        ? config.hub53ai.createdFilesExclude.map((entry) => String(entry).trim()).filter(Boolean)
        : [],
      artifactUploadTimeoutMs: config?.hub53ai?.artifactUploadTimeoutMs ?? 1_500,
      diagnosticLogs: config?.hub53ai?.diagnosticLogs ?? config?.hub53ai?.debug?.all ?? false,
      ledgerDebug: config?.hub53ai?.ledgerDebug ?? config?.hub53ai?.debug?.ledger ?? false,
      duplicateTrace: config?.hub53ai?.duplicateTrace ?? config?.hub53ai?.debug?.duplicates ?? false,
      debug: {
        all: config?.hub53ai?.debug?.all ?? config?.hub53ai?.diagnosticLogs ?? false,
        ledger: config?.hub53ai?.debug?.ledger ?? config?.hub53ai?.ledgerDebug ?? false,
        duplicates: config?.hub53ai?.debug?.duplicates ?? config?.hub53ai?.duplicateTrace ?? false
      }
    },
    console: {
      enabled: config?.console?.enabled ?? true,
      host: config?.console?.host ?? "127.0.0.1",
      port: config?.console?.port ?? 4318,
      showRawThinking: config?.console?.showRawThinking ?? true
    },
    persistence: {
      maxSessions: config?.persistence?.maxSessions ?? 100
    },
    logging: {
      level: config?.logging?.level ?? "info"
    }
  };
}

export function resolvePluginConfigWithHostDefaults(configPath: string, config?: PluginConfig): ResolvedPluginConfig {
  const resolved = resolvePluginConfig(config);
  const hostConfig = readHostGatewayConfig(resolveHostConfigPath(configPath));

  if (
    hostConfig.baseUrl &&
    (!resolved.gateway.baseUrl || shouldPreferHostGateway(resolved.gateway.baseUrl, hostConfig.baseUrl))
  ) {
    resolved.gateway.baseUrl = hostConfig.baseUrl;
  }
  if (hostConfig.secret && (!resolved.gateway.secret || resolved.gateway.baseUrl === hostConfig.baseUrl)) {
    resolved.gateway.secret = hostConfig.secret;
  }
  if (!resolved.hub53ai.botId && hostConfig.hub53ai?.botId) {
    resolved.hub53ai.botId = hostConfig.hub53ai.botId;
  }
  if (!resolved.hub53ai.secret && hostConfig.hub53ai?.secret) {
    resolved.hub53ai.secret = hostConfig.hub53ai.secret;
  }
  if (!resolved.hub53ai.wsUrl && hostConfig.hub53ai?.wsUrl) {
    resolved.hub53ai.wsUrl = hostConfig.hub53ai.wsUrl;
  }

  return resolved;
}

function shouldPreferHostGateway(configuredBaseUrl: string, hostBaseUrl: string): boolean {
  if (configuredBaseUrl === hostBaseUrl) {
    return false;
  }
  const configured = parseGatewayUrl(configuredBaseUrl);
  const host = parseGatewayUrl(hostBaseUrl);
  if (!configured || !host) {
    return false;
  }
  return configured.isLoopback && host.isLoopback;
}

function parseGatewayUrl(value: string): { isLoopback: boolean } | undefined {
  try {
    const url = new URL(value);
    const hostname = url.hostname.replace(/^\[|\]$/g, "").toLowerCase();
    return {
      isLoopback: hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1"
    };
  } catch {
    return undefined;
  }
}

export function sanitizePluginConfig<T extends PluginConfig | ResolvedPluginConfig>(config?: T): T | {} {
  if (!config) {
    return {};
  }

  return JSON.parse(JSON.stringify(config), (key, value) => {
    if (typeof value === "string" && key && SENSITIVE_KEY_PATTERN.test(key)) {
      return "[redacted]";
    }
    return value;
  }) as PluginConfig;
}

export function readHostRuntimeInfo(configPath: string): HostRuntimeInfo {
  const resolvedPath = resolveHostConfigPath(configPath);

  try {
    const raw = readFileSync(resolvedPath, "utf8");
    const parsed = JSON.parse(raw) as {
      agents?: {
        defaults?: {
          model?: {
            primary?: unknown;
          };
        };
      };
      skills?: {
        entries?: Record<string, { enabled?: unknown } | undefined>;
      };
    };

    const modelPrimary =
      typeof parsed.agents?.defaults?.model?.primary === "string" &&
      parsed.agents.defaults.model.primary.trim().length > 0
        ? parsed.agents.defaults.model.primary.trim()
        : undefined;

    const enabledSkills = Object.entries(parsed.skills?.entries ?? {})
      .filter(([, config]) => config?.enabled === true)
      .map(([skillName]) => skillName)
      .sort((left, right) => left.localeCompare(right));

    const cronInfo = readHostCronInfo(resolvedPath);

    return {
      modelPrimary,
      enabledSkills,
      ...cronInfo
    };
  } catch {
    return {
      enabledSkills: []
    };
  }
}

function readHostCronInfo(configPath: string): Pick<HostRuntimeInfo, "cronScheduler" | "cronTasks"> {
  const cronPath = resolveHostCronPath(configPath);
  if (!cronPath) {
    return {};
  }

  try {
    const raw = readFileSync(cronPath, "utf8");
    const parsed = JSON.parse(raw) as {
      jobs?: unknown[];
      tasks?: unknown[];
    };
    const jobs = Array.isArray(parsed.jobs) ? parsed.jobs : Array.isArray(parsed.tasks) ? parsed.tasks : [];
    const cronTasks = jobs.map((job, index) => normalizeHostCronTask(job, index));
    return {
      cronScheduler: {
        enabled: true,
        storePath: cronPath,
        jobCount: cronTasks.length
      },
      cronTasks
    };
  } catch {
    return {};
  }
}

function resolveHostCronPath(configPath: string): string | undefined {
  const configDir = dirname(configPath);
  const candidates = [
    join(configDir, "cron", "jobs.json"),
    join(dirname(configDir), "cron", "jobs.json"),
    join(homedir(), ".openclaw", "cron", "jobs.json"),
    join(homedir(), ".qclaw", "cron", "jobs.json")
  ];
  return candidates.find((candidate) => existsSync(candidate));
}

function normalizeHostCronTask(raw: unknown, index: number): NonNullable<HostRuntimeInfo["cronTasks"]>[number] {
  const job = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const id = readString(job, ["id", "jobId", "key", "name"]) ?? `cron-${index + 1}`;
  const name = readString(job, ["name", "title", "description"]) ?? id;
  const status = readString(job, ["status", "state"]);
  const enabled = typeof job.enabled === "boolean" ? job.enabled : status?.toLowerCase() !== "disabled";
  const agentId = readString(job, ["agentId", "agent", "owner"]);
  const schedule = formatHostCronSchedule(job.schedule ?? job.cron ?? job.expr ?? job.everyMs ?? job.at ?? job.atMs);
  const nextRunAt = optionalIsoString(job.nextRunAt ?? job.nextRunAtMs ?? job.nextAt ?? job.nextAtMs);
  const lastRunAt = optionalIsoString(job.lastRunAt ?? job.lastRunAtMs ?? job.lastAt ?? job.lastAtMs);
  const payloadKind = readString(job, ["payloadKind"]) ?? readNestedString(job, ["payload", "kind"]);

  return {
    id,
    name,
    enabled,
    ...(status ? { status } : {}),
    ...(agentId ? { agentId } : {}),
    ...(schedule ? { schedule } : {}),
    ...(nextRunAt ? { nextRunAt } : {}),
    ...(lastRunAt ? { lastRunAt } : {}),
    ...(payloadKind ? { payloadKind } : {})
  };
}

function formatHostCronSchedule(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return `every ${formatDurationMs(value)}`;
  }
  if (!value || typeof value !== "object") {
    return undefined;
  }

  const schedule = value as Record<string, unknown>;
  const kind = readString(schedule, ["kind", "type"]);
  if (kind === "cron") {
    const expr = readString(schedule, ["expr", "cron", "expression"]);
    const timezone = readString(schedule, ["timezone", "timeZone", "tz"]);
    return [`cron${expr ? ` ${expr}` : ""}`, timezone].filter(Boolean).join(" · ");
  }
  if (kind === "every") {
    const everyMs = readNumber(schedule, ["everyMs", "intervalMs", "ms"]);
    return everyMs ? `every ${formatDurationMs(everyMs)}` : "every";
  }
  if (kind === "at") {
    const at = optionalIsoString(schedule.at ?? schedule.atMs);
    return at ? `at ${at}` : "at";
  }

  const expr = readString(schedule, ["expr", "cron", "expression"]);
  if (expr) {
    return expr;
  }
  const everyMs = readNumber(schedule, ["everyMs", "intervalMs", "ms"]);
  if (everyMs) {
    return `every ${formatDurationMs(everyMs)}`;
  }
  const at = optionalIsoString(schedule.at ?? schedule.atMs);
  return at ? `at ${at}` : undefined;
}

function readString(record: Record<string, unknown>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function readNestedString(record: Record<string, unknown>, path: string[]): string | undefined {
  let current: unknown = record;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return typeof current === "string" && current.trim() ? current.trim() : undefined;
}

function readNumber(record: Record<string, unknown>, keys: string[]): number | undefined {
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

function optionalIsoString(value: unknown): string | undefined {
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
  return undefined;
}

function formatDurationMs(value: number): string {
  if (value % 86_400_000 === 0) {
    return `${value / 86_400_000}d`;
  }
  if (value % 3_600_000 === 0) {
    return `${value / 3_600_000}h`;
  }
  if (value % 60_000 === 0) {
    return `${value / 60_000}m`;
  }
  if (value % 1_000 === 0) {
    return `${value / 1_000}s`;
  }
  return `${value}ms`;
}

function readHostGatewayConfig(configPath: string): {
  baseUrl?: string;
  secret?: string;
  hub53ai?: {
    botId?: string;
    secret?: string;
    wsUrl?: string;
  };
} {
  try {
    const raw = readFileSync(configPath, "utf8");
    const parsed = JSON.parse(raw) as {
      gateway?: {
        host?: unknown;
        port?: unknown;
        auth?: {
          mode?: unknown;
          token?: unknown;
          password?: unknown;
        };
      };
      port?: unknown;
      auth?: {
        mode?: unknown;
        token?: unknown;
        password?: unknown;
      };
      channels?: {
        "53aihub"?: {
          botId?: unknown;
          secret?: unknown;
          token?: unknown;
          WSUrl?: unknown;
          websocketUrl?: unknown;
        };
      };
    };

    const gateway = parsed.gateway ?? {};
    const port = typeof gateway.port === "number" ? gateway.port : Number(gateway.port ?? parsed.port ?? 28789);
    const host = typeof gateway.host === "string" && gateway.host.trim() ? gateway.host.trim() : "127.0.0.1";
    const baseUrl = Number.isFinite(port) && port > 0 ? `ws://${host}:${port}` : undefined;
    const auth = gateway.auth ?? parsed.auth ?? {};
    const authMode = typeof auth.mode === "string" ? auth.mode : "token";
    const secret =
      authMode === "password"
        ? typeof auth.password === "string"
          ? auth.password
          : undefined
        : typeof auth.token === "string"
          ? auth.token
          : undefined;

    const legacyHub = parsed.channels?.["53aihub"];
    const hub53ai = legacyHub
      ? {
          botId: typeof legacyHub.botId === "string" ? legacyHub.botId : undefined,
          secret:
            typeof legacyHub.secret === "string"
              ? legacyHub.secret
              : typeof legacyHub.token === "string"
                ? legacyHub.token
                : undefined,
          wsUrl:
            typeof legacyHub.WSUrl === "string"
              ? legacyHub.WSUrl
              : typeof legacyHub.websocketUrl === "string"
                ? legacyHub.websocketUrl
                : undefined
        }
      : undefined;

    return { baseUrl, secret, hub53ai };
  } catch {
    return {};
  }
}

export function resolveHostConfigPath(configPath: string): string {
  const candidates = [
    configPath,
    join(homedir(), ".qclaw", "openclaw.json"),
    join(homedir(), ".openclaw", "openclaw.json")
  ];

  return candidates.find((candidate) => existsSync(candidate)) ?? configPath;
}
