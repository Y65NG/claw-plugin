export type HostKind = "openclaw" | "qclaw";

import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export type PluginConfig = {
  gateway?: {
    baseUrl?: string;
    botId?: string;
    secret?: string;
    requestTimeoutMs?: number;
    streamReconnectMs?: number;
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
};

const SENSITIVE_KEY_PATTERN = /(token|secret|password|key|credential)/i;

export function detectHostKind(pathHint?: string): HostKind {
  const normalized = (pathHint ?? "").replaceAll("\\", "/").toLowerCase();
  if (normalized.includes("/.qclaw") || normalized.includes("/.qclow")) {
    return "qclaw";
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
      streamReconnectMs: config?.gateway?.streamReconnectMs ?? 2_000
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
      maxReconnectAttempts: config?.hub53ai?.maxReconnectAttempts ?? 10
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

  if (!resolved.gateway.baseUrl && hostConfig.baseUrl) {
    resolved.gateway.baseUrl = hostConfig.baseUrl;
  }
  if (!resolved.gateway.secret && hostConfig.secret) {
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

    return {
      modelPrimary,
      enabledSkills
    };
  } catch {
    return {
      enabledSkills: []
    };
  }
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

function resolveHostConfigPath(configPath: string): string {
  const candidates = [
    configPath,
    join(homedir(), ".qclaw", "openclaw.json"),
    join(homedir(), ".openclaw", "openclaw.json")
  ];

  return candidates.find((candidate) => existsSync(candidate)) ?? configPath;
}
