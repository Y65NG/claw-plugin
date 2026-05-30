import { createHash } from "node:crypto";
import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

type InstallInput = {
  packageRoot: string;
  extensionsDir: string;
  configPath: string;
  gateway?: string;
  botId?: string;
  secret?: string;
  preferResponsesApi?: boolean;
  gatewayModel?: string;
  hubWsUrl?: string;
  hubBotId?: string;
  hubSecret?: string;
  hubEnabled?: boolean;
  consoleHost?: string;
  consolePort?: number;
};

type ParsedArgs = {
  target?: string;
  gateway?: string;
  "bot-id"?: string;
  secret?: string;
  "prefer-responses-api"?: string;
  "gateway-model"?: string;
  "hub-ws-url"?: string;
  "hub-bot-id"?: string;
  "hub-secret"?: string;
  "hub-enabled"?: string;
  "extensions-dir"?: string;
  "config-path"?: string;
  "console-host"?: string;
  "console-port"?: string;
};

type OpenClawConfig = {
  gateway?: {
    host?: unknown;
    port?: unknown;
    auth?: {
      mode?: unknown;
      token?: unknown;
      password?: unknown;
    };
    http?: {
      endpoints?: Record<string, unknown>;
    };
  };
  channels?: {
    "53aihub"?: {
      enabled?: unknown;
      botId?: unknown;
      secret?: unknown;
      token?: unknown;
      WSUrl?: unknown;
      websocketUrl?: unknown;
      accessPolicy?: unknown;
      allowFrom?: unknown;
      sendThinkingMessage?: unknown;
    };
  };
  plugins?: {
    enabled?: unknown;
    allow?: unknown;
    load?: {
      paths?: unknown;
    };
    entries?: Record<string, unknown>;
  };
};

const PLUGIN_ID = "claw-control-center";
const LEGACY_PLUGIN_ID = "53ai-openclaw";
const COPY_ITEMS = ["dist", "openclaw.plugin.json", "package.json", "bin", "web-dist"] as const;

export async function installIntoQClaw(input: InstallInput): Promise<{
  configPath: string;
  extensionsDir: string;
  destination: string;
  gatewayBaseUrl: string;
  hub53aiConfigured: boolean;
  pluginBuild: string;
}> {
  return installIntoHost(input, "QClaw");
}

export async function installIntoOpenClaw(input: InstallInput): Promise<{
  configPath: string;
  extensionsDir: string;
  destination: string;
  gatewayBaseUrl: string;
  hub53aiConfigured: boolean;
  pluginBuild: string;
}> {
  return installIntoHost(input, "OpenClaw");
}

async function installIntoHost(
  input: InstallInput,
  hostLabel: "Claw" | "QClaw" | "OpenClaw"
): Promise<{
  configPath: string;
  extensionsDir: string;
  destination: string;
  gatewayBaseUrl: string;
  hub53aiConfigured: boolean;
  pluginBuild: string;
}> {
  await mkdir(input.extensionsDir, { recursive: true });
  const destination = join(input.extensionsDir, PLUGIN_ID);
  await mkdir(destination, { recursive: true });

  await copyPublishablePackage(input.packageRoot, destination);

  await mkdir(dirname(input.configPath), { recursive: true });
  const config = await readOpenClawConfig(input.configPath);
  const inferredGateway = inferGatewaySettings(config);
  const inferredHub53AI = inferHub53AISettings(config);

  const gatewayBaseUrl = input.gateway?.trim() || inferredGateway.baseUrl;
  if (!gatewayBaseUrl) {
    throw new Error(`missing gateway URL and no local ${hostLabel} gateway could be inferred`);
  }

  const secret = input.secret?.trim() || inferredGateway.secret;
  if (!secret) {
    throw new Error(`missing gateway secret and no local ${hostLabel} gateway token could be inferred`);
  }

  const botId = input.botId?.trim();
  const preferResponsesApi = input.preferResponsesApi ?? false;
  const gatewayModel = input.gatewayModel?.trim();
  const hubWsUrl = input.hubWsUrl?.trim() || inferredHub53AI.wsUrl;
  const hubBotId = input.hubBotId?.trim() || inferredHub53AI.botId;
  const hubSecret = input.hubSecret?.trim() || inferredHub53AI.secret;
  const hubConfigured = Boolean(hubWsUrl && hubBotId && hubSecret);
  const hubInputProvided = Boolean(input.hubWsUrl?.trim() || input.hubBotId?.trim() || input.hubSecret?.trim());
  const hubEnabled = input.hubEnabled ?? (hubConfigured ? (hubInputProvided ? true : inferredHub53AI.enabled) : false);
  if (hubEnabled && !hubConfigured) {
    throw new Error("hub53ai requires --hub-ws-url, --hub-bot-id, and --hub-secret when enabled");
  }
  const consoleHost = input.consoleHost?.trim();
  const consolePort = normalizePort(input.consolePort);

  const plugins = ensureObject(config, "plugins");
  plugins.enabled = true;
  plugins.allow = dedupeStrings([
    ...(Array.isArray(plugins.allow) ? plugins.allow.filter((entry) => entry !== LEGACY_PLUGIN_ID) : []),
    PLUGIN_ID
  ]);

  const load = ensureObject(plugins, "load");
  load.paths = dedupeStrings([...(Array.isArray(load.paths) ? load.paths : []), input.extensionsDir]);

  const entries = ensureObject(plugins, "entries");
  const legacyEntry =
    entries[LEGACY_PLUGIN_ID] && typeof entries[LEGACY_PLUGIN_ID] === "object" && !Array.isArray(entries[LEGACY_PLUGIN_ID])
      ? (entries[LEGACY_PLUGIN_ID] as Record<string, unknown>)
      : undefined;
  if (legacyEntry) {
    legacyEntry.enabled = false;
    entries[LEGACY_PLUGIN_ID] = legacyEntry;
  }
  const previousEntry = ensureObject(entries, PLUGIN_ID);
  const previousConfig = ensureObject(previousEntry, "config");
  const previousGateway = ensureObject(previousConfig, "gateway");
  previousGateway.baseUrl = gatewayBaseUrl;
  previousGateway.secret = secret;
  previousGateway.preferResponsesApi = preferResponsesApi;
  if (botId) {
    previousGateway.botId = botId;
  }
  if (gatewayModel) {
    previousGateway.modelOverride = gatewayModel;
  }

  if (preferResponsesApi) {
    const gatewayConfig = ensureObject(config, "gateway");
    const gatewayHttp = ensureObject(gatewayConfig, "http");
    const gatewayHttpEndpoints = ensureObject(gatewayHttp, "endpoints");
    const responsesEndpoint = ensureObject(gatewayHttpEndpoints, "responses");
    responsesEndpoint.enabled = true;
  }

  if (hubConfigured || input.hubEnabled !== undefined) {
    const previousHub = ensureObject(previousConfig, "hub53ai");
    previousHub.enabled = hubEnabled;
    if (hubBotId) {
      previousHub.botId = hubBotId;
    }
    if (hubSecret) {
      previousHub.secret = hubSecret;
    }
    if (hubWsUrl) {
      previousHub.wsUrl = hubWsUrl;
    }
    previousHub.accessPolicy = inferredHub53AI.accessPolicy || previousHub.accessPolicy || "open";
    previousHub.allowFrom = inferredHub53AI.allowFrom ?? previousHub.allowFrom ?? [];
    previousHub.sendThinkingMessage =
      inferredHub53AI.sendThinkingMessage ?? previousHub.sendThinkingMessage ?? true;
  }

  if (consoleHost || consolePort !== undefined) {
    const previousConsole = ensureObject(previousConfig, "console");
    if (consoleHost) {
      previousConsole.host = consoleHost;
    }
    if (consolePort !== undefined) {
      previousConsole.port = consolePort;
    }
  }

  previousEntry.enabled = true;

  await writeFile(input.configPath, `${JSON.stringify(config, null, 2)}\n`);
  return {
    configPath: input.configPath,
    extensionsDir: input.extensionsDir,
    destination,
    gatewayBaseUrl,
    hub53aiConfigured: hubConfigured,
    pluginBuild: await readPluginBuildInfo(destination)
  };
}

export async function runInstallCommand(input: {
  argv?: string[];
  packageRoot: string;
}): Promise<void> {
  const argv = input.argv ?? process.argv.slice(2);
  if (argv[0] !== "install") {
    throw new Error("expected subcommand: install");
  }

  const args = parseArgs(argv.slice(1));
  const destination = resolveInstallDestination(args);

  const result = await installIntoHost(
    {
      packageRoot: input.packageRoot,
      extensionsDir: destination.extensionsDir,
      configPath: destination.configPath,
      gateway: args.gateway,
      botId: args["bot-id"],
      secret: args.secret,
      preferResponsesApi: parseOptionalBoolean(args["prefer-responses-api"]),
      gatewayModel: args["gateway-model"],
      hubWsUrl: args["hub-ws-url"],
      hubBotId: args["hub-bot-id"],
      hubSecret: args["hub-secret"],
      hubEnabled: parseOptionalBoolean(args["hub-enabled"]),
      consoleHost: args["console-host"],
      consolePort: args["console-port"] ? Number(args["console-port"]) : undefined
    },
    "Claw"
  );

  process.stdout.write(
    [
      `Installed ${PLUGIN_ID} into Claw.`,
      `Extensions: ${result.extensionsDir}`,
      `Config: ${result.configPath}`,
      `Gateway: ${result.gatewayBaseUrl}`,
      `53AIHub: ${result.hub53aiConfigured ? "configured" : "not configured"}`,
      `Plugin build: ${result.pluginBuild}`,
      "Restart your Claw host to load the plugin."
    ].join("\n") + "\n"
  );
}

function resolveInstallDestination(args: ParsedArgs): {
  configPath: string;
  extensionsDir: string;
} {
  if (args.target !== undefined) {
    throw new Error("--target is no longer supported; pass --config-path and --extensions-dir from your Claw host");
  }

  const explicitConfigPath = args["config-path"] ? resolve(args["config-path"]) : undefined;
  const explicitExtensionsDir = args["extensions-dir"] ? resolve(args["extensions-dir"]) : undefined;
  if (!explicitConfigPath || !explicitExtensionsDir) {
    throw new Error("missing --config-path and --extensions-dir; provide the paths generated by your Claw host");
  }

  return {
    configPath: explicitConfigPath,
    extensionsDir: explicitExtensionsDir
  };
}

function parseArgs(argv: string[]): ParsedArgs {
  const parsed: ParsedArgs = {};
  for (let index = 0; index < argv.length; index += 1) {
    const entry = argv[index]!;
    if (!entry.startsWith("--")) {
      continue;
    }
    const next = argv[index + 1];
    if (next && !next.startsWith("--")) {
      parsed[entry.slice(2) as keyof ParsedArgs] = next;
      index += 1;
    } else {
      parsed[entry.slice(2) as keyof ParsedArgs] = "true";
    }
  }
  return parsed;
}

async function copyPublishablePackage(packageRoot: string, destination: string) {
  if (!existsSync(packageRoot)) {
    throw new Error(`package root does not exist: ${packageRoot}`);
  }

  for (const relativePath of COPY_ITEMS) {
    const source = join(packageRoot, relativePath);
    if (!existsSync(source)) {
      continue;
    }
    const target = join(destination, relativePath);
    await rm(target, { recursive: true, force: true });
    await cp(source, target, { recursive: true, force: true });
  }
}

async function readPluginBuildInfo(destination: string): Promise<string> {
  const packagePath = join(destination, "package.json");
  const entryPath = join(destination, "dist", "index.cjs");
  let version = "unknown";
  try {
    const packageJson = JSON.parse(await readFile(packagePath, "utf8")) as { version?: unknown };
    if (typeof packageJson.version === "string" && packageJson.version.trim()) {
      version = packageJson.version.trim();
    }
  } catch {
    version = "unknown";
  }

  try {
    const entry = await readFile(entryPath);
    const digest = createHash("sha256").update(entry).digest("hex").slice(0, 12);
    return `${PLUGIN_ID}@${version} sha256:${digest}`;
  } catch {
    return `${PLUGIN_ID}@${version} sha256:missing`;
  }
}

async function readOpenClawConfig(configPath: string): Promise<OpenClawConfig & Record<string, unknown>> {
  if (!existsSync(configPath)) {
    return {};
  }

  return JSON.parse(await readFile(configPath, "utf8")) as OpenClawConfig & Record<string, unknown>;
}

function inferGatewaySettings(config: OpenClawConfig): { baseUrl: string; secret: string } {
  const gateway = config.gateway ?? {};
  const host = typeof gateway.host === "string" && gateway.host.trim() ? gateway.host.trim() : "127.0.0.1";
  const port = Number(gateway.port ?? 28789);
  const baseUrl = Number.isFinite(port) && port > 0 ? `ws://${host}:${port}` : "";
  const auth = gateway.auth ?? {};
  const mode = typeof auth.mode === "string" ? auth.mode : "token";
  const secret =
    mode === "password"
      ? typeof auth.password === "string"
        ? auth.password
        : ""
      : typeof auth.token === "string"
        ? auth.token
        : "";
  return { baseUrl, secret };
}

function inferHub53AISettings(config: OpenClawConfig): {
  enabled: boolean;
  botId: string;
  secret: string;
  wsUrl: string;
  accessPolicy?: string;
  allowFrom?: string[];
  sendThinkingMessage?: boolean;
} {
  const legacy = config.channels?.["53aihub"];
  if (!legacy) {
    return {
      enabled: false,
      botId: "",
      secret: "",
      wsUrl: ""
    };
  }

  return {
    enabled: legacy.enabled !== false,
    botId: typeof legacy.botId === "string" ? legacy.botId : "",
    secret:
      typeof legacy.secret === "string"
        ? legacy.secret
        : typeof legacy.token === "string"
          ? legacy.token
          : "",
    wsUrl:
      typeof legacy.WSUrl === "string"
        ? legacy.WSUrl
        : typeof legacy.websocketUrl === "string"
          ? legacy.websocketUrl
          : "",
    accessPolicy: typeof legacy.accessPolicy === "string" ? legacy.accessPolicy : undefined,
    allowFrom: Array.isArray(legacy.allowFrom)
      ? legacy.allowFrom.map((entry) => String(entry)).filter(Boolean)
      : undefined,
    sendThinkingMessage:
      typeof legacy.sendThinkingMessage === "boolean" ? legacy.sendThinkingMessage : undefined
  };
}

function parseOptionalBoolean(value: string | undefined): boolean | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value === "" || value === "true" || value === "1" || value === "yes") {
    return true;
  }
  if (value === "false" || value === "0" || value === "no") {
    return false;
  }
  throw new Error(`invalid boolean value: ${value}`);
}

function normalizePort(port: number | undefined): number | undefined {
  if (port === undefined) {
    return undefined;
  }
  if (!Number.isFinite(port) || port <= 0) {
    throw new Error("console port must be a positive number");
  }
  return Math.floor(port);
}

function ensureObject(record: Record<string, unknown>, key: string): Record<string, any> {
  const existing = record[key];
  if (existing && typeof existing === "object" && !Array.isArray(existing)) {
    return existing as Record<string, any>;
  }
  const created: Record<string, any> = {};
  record[key] = created;
  return created;
}

function dedupeStrings(values: unknown[]): string[] {
  return Array.from(new Set(values.filter((value): value is string => typeof value === "string")));
}
