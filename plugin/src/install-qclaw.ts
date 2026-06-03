import { createHash } from "node:crypto";
import { createInterface } from "node:readline/promises";
import { cp, mkdir, open as openFile, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import type { FileHandle } from "node:fs/promises";
import { parse as parseYaml, stringify as stringifyYaml } from "yaml";

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

export type HostDefinition = {
  id: string;
  label: string;
  configPath: string;
  extensionsDir: string;
  installKind?: "openclaw" | "hermes";
  incompatibilityReason?: string;
};

type InstallDestination = {
  configPath: string;
  extensionsDir: string;
  label: string;
  installKind: "openclaw" | "hermes";
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
const HERMES_PLATFORM_ID = "53aihub";
const HERMES_PLUGIN_KEY = `platforms/${HERMES_PLATFORM_ID}`;
const HERMES_ENV_KEYS = {
  botId: "HUB53AI_BOT_ID",
  secret: "HUB53AI_SECRET",
  wsUrl: "HUB53AI_WS_URL"
} as const;
const SUPPORTED_ARGS = new Set([
  "gateway",
  "bot-id",
  "secret",
  "prefer-responses-api",
  "gateway-model",
  "hub-ws-url",
  "hub-bot-id",
  "hub-secret",
  "hub-enabled",
  "extensions-dir",
  "config-path",
  "console-host",
  "console-port"
]);

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

export async function installIntoHermes(input: InstallInput): Promise<{
  configPath: string;
  extensionsDir: string;
  destination: string;
  hub53aiConfigured: boolean;
  pluginBuild: string;
}> {
  const hubWsUrl = input.hubWsUrl?.trim();
  const hubBotId = input.hubBotId?.trim();
  const hubSecret = input.hubSecret?.trim();
  if (!hubWsUrl || !hubBotId || !hubSecret) {
    throw new Error("Hermes install requires --hub-ws-url, --hub-bot-id, and --hub-secret");
  }

  const platformsDir = normalizeHermesPlatformsDir(input.extensionsDir);
  const destination = join(platformsDir, HERMES_PLATFORM_ID);
  await mkdir(destination, { recursive: true });
  await copyHermesPlatformPackage(input.packageRoot, destination);
  await updateHermesConfig(input.configPath);
  await updateHermesEnv(input.configPath, {
    botId: hubBotId,
    secret: hubSecret,
    wsUrl: hubWsUrl
  });

  return {
    configPath: input.configPath,
    extensionsDir: platformsDir,
    destination,
    hub53aiConfigured: true,
    pluginBuild: await readHermesPluginBuildInfo(destination)
  };
}

async function installIntoHost(
  input: InstallInput,
  hostLabel: string
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
  const explicitGatewayBaseUrl = input.gateway?.trim();
  const explicitGatewaySecret = input.secret?.trim();

  const gatewayBaseUrl = explicitGatewayBaseUrl || inferredGateway.baseUrl;
  if (!gatewayBaseUrl) {
    throw new Error(`missing gateway URL and no local ${hostLabel} gateway could be inferred`);
  }

  const secret = explicitGatewaySecret || inferredGateway.secret;
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
  if (explicitGatewayBaseUrl) {
    previousGateway.baseUrl = explicitGatewayBaseUrl;
  } else {
    delete previousGateway.baseUrl;
  }
  if (explicitGatewaySecret) {
    previousGateway.secret = explicitGatewaySecret;
  } else {
    delete previousGateway.secret;
  }
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
  hostDefinitions?: HostDefinition[];
  selectHosts?: (hosts: HostDefinition[], incompatibleHosts: HostDefinition[]) => Promise<HostDefinition[]>;
  selectHost?: (hosts: HostDefinition[]) => Promise<HostDefinition>;
  ttyPath?: string;
}): Promise<void> {
  const argv = input.argv ?? process.argv.slice(2);
  if (argv[0] !== "install") {
    throw new Error("expected subcommand: install");
  }

  const args = parseArgs(argv.slice(1));
  const destinations = await resolveInstallDestinations(args, {
    hostDefinitions: input.hostDefinitions,
    selectHosts: input.selectHosts,
    selectHost: input.selectHost,
    ttyPath: input.ttyPath
  });

  for (const destination of destinations) {
    const installInput: InstallInput = {
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
    };
    const result =
      destination.installKind === "hermes"
        ? await installIntoHermes(installInput)
        : await installIntoHost(installInput, destination.label);

    process.stdout.write(
      [
        `Installed ${PLUGIN_ID} into ${destination.label}.`,
        `Extensions: ${result.extensionsDir}`,
        `Config: ${result.configPath}`,
        ...(destination.installKind === "hermes" ? [] : [`Gateway: ${(result as Awaited<ReturnType<typeof installIntoHost>>).gatewayBaseUrl}`]),
        `53AIHub: ${result.hub53aiConfigured ? "configured" : "not configured"}`,
        `Plugin build: ${result.pluginBuild}`,
        `Restart ${destination.label} to load the plugin.`
      ].join("\n") + "\n"
    );
  }
}

async function resolveInstallDestinations(
  args: ParsedArgs,
  options: {
    hostDefinitions?: HostDefinition[];
    selectHosts?: (hosts: HostDefinition[], incompatibleHosts: HostDefinition[]) => Promise<HostDefinition[]>;
    selectHost?: (hosts: HostDefinition[]) => Promise<HostDefinition>;
    ttyPath?: string;
  } = {}
): Promise<InstallDestination[]> {
  const explicitConfigPath = args["config-path"] ? resolve(args["config-path"]) : undefined;
  const explicitExtensionsDir = args["extensions-dir"] ? resolve(args["extensions-dir"]) : undefined;

  if (explicitConfigPath && explicitExtensionsDir) {
    const hermes = isHermesDestination(explicitConfigPath, explicitExtensionsDir);
    return [{
      configPath: explicitConfigPath,
      extensionsDir: hermes ? normalizeHermesPlatformsDir(explicitExtensionsDir) : explicitExtensionsDir,
      label: hermes ? "Hermes" : "Claw",
      installKind: hermes ? "hermes" : "openclaw"
    }];
  }

  if (explicitConfigPath || explicitExtensionsDir) {
    throw new Error("pass --config-path and --extensions-dir together, or omit both to auto-detect Claw");
  }

  const detected = detectInstallHosts(options.hostDefinitions ?? getDefaultHostDefinitions());
  const compatible = detected;
  const incompatible: HostDefinition[] = [];
  if (compatible.length === 1) {
    return [toInstallDestination(compatible[0]!)];
  }
  if (compatible.length > 1) {
    const selected = options.selectHosts
      ? await options.selectHosts(compatible, incompatible)
      : options.selectHost
        ? [await options.selectHost(compatible)]
        : await promptForInstallHosts(compatible, incompatible, options.ttyPath ?? "/dev/tty");
    return validateSelectedHosts(selected, compatible).map(toInstallDestination);
  }
  throw new Error(
    [
      "could not auto-detect an installed Claw host.",
      "Pass --config-path and --extensions-dir to install into a specific Claw.",
      "",
      "Supported default locations:",
      ...formatHostList(options.hostDefinitions ?? getDefaultHostDefinitions())
    ].join("\n")
  );
}

function getDefaultHostDefinitions(): HostDefinition[] {
  const home = homedir();
  const qclawHome = resolve(home, ".qclaw");
  const openClawHome = resolve(home, ".openclaw");
  const hermesHome = resolve(home, ".hermes");
  return [
    {
      id: "qclaw",
      label: "QClaw",
      configPath: join(qclawHome, "openclaw.json"),
      extensionsDir: resolve(home, "Library/Application Support/QClaw/openclaw/config/extensions")
    },
    {
      id: "openclaw",
      label: "OpenClaw",
      configPath: join(openClawHome, "openclaw.json"),
      extensionsDir: resolve(openClawHome, "extensions")
    },
    {
      id: "hermes",
      label: "Hermes",
      configPath: join(hermesHome, "config.yaml"),
      extensionsDir: resolve(hermesHome, "plugins", "platforms"),
      installKind: "hermes"
    }
  ];
}

function detectInstallHosts(hosts: HostDefinition[]): HostDefinition[] {
  const seen = new Set<string>();
  return hosts.filter((host) => {
    const key = `${resolve(host.configPath)}\0${resolve(host.extensionsDir)}`;
    if (seen.has(key) || !existsSync(host.configPath)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

async function promptForInstallHosts(
  hosts: HostDefinition[],
  incompatibleHosts: HostDefinition[],
  ttyPath: string
): Promise<HostDefinition[]> {
  let handle: FileHandle | undefined;
  try {
    handle = await openFile(ttyPath, "r+");
    const input = handle.createReadStream();
    const output = handle.createWriteStream();
    const readline = createInterface({ input, output });
    try {
      output.write("Multiple Claw installations were detected.\n");
      output.write("Choose one or more locations to install claw-control-center:\n");
      hosts.forEach((host, index) => {
        output.write(`${index + 1}. ${host.label}\n`);
        output.write(`   Config: ${host.configPath}\n`);
        output.write(`   Extensions: ${host.extensionsDir}\n`);
      });
      if (incompatibleHosts.length > 0) {
        output.write("\nDetected but not installable by this OpenClaw plugin installer:\n");
        for (const host of incompatibleHosts) {
          output.write(`- ${host.label}: ${host.incompatibilityReason ?? "incompatible plugin format"}\n`);
        }
      }
      const answer = await readline.question(`Install location(s) [1-${hosts.length}, comma-separated, or all]: `);
      const selectedIndexes = parseInstallSelection(answer, hosts.length);
      if (selectedIndexes.length === 0) {
        throw new Error(`invalid install location: ${answer}`);
      }
      return selectedIndexes.map((index) => hosts[index - 1]!);
    } finally {
      readline.close();
      input.destroy();
      output.end();
    }
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("invalid install location:")) {
      throw error;
    }
    throw new Error(
      [
        "multiple Claw installations were detected, but no interactive terminal was available.",
        "Run the installer again with --config-path and --extensions-dir.",
        "",
        "Detected locations:",
        ...formatHostList(hosts)
      ].join("\n")
    );
  } finally {
    await handle?.close().catch(() => undefined);
  }
}

function toInstallDestination(host: HostDefinition): InstallDestination {
  return {
    configPath: resolve(host.configPath),
    extensionsDir: resolve(host.extensionsDir),
    label: host.label,
    installKind: host.installKind ?? "openclaw"
  };
}

function validateSelectedHosts(selected: HostDefinition[], compatible: HostDefinition[]): HostDefinition[] {
  const compatibleIds = new Set(compatible.map((host) => host.id));
  const invalid = selected.find((host) => !compatibleIds.has(host.id));
  if (invalid) {
    throw new Error(`selected host is not installable: ${invalid.label}`);
  }
  const seen = new Set<string>();
  return selected.filter((host) => {
    if (seen.has(host.id)) {
      return false;
    }
    seen.add(host.id);
    return true;
  });
}

function parseInstallSelection(answer: string, hostCount: number): number[] {
  const trimmed = answer.trim().toLowerCase();
  if (trimmed === "all" || trimmed === "*") {
    return Array.from({ length: hostCount }, (_, index) => index + 1);
  }
  const indexes = trimmed
    .split(",")
    .map((part) => Number(part.trim()))
    .filter((index) => Number.isInteger(index) && index >= 1 && index <= hostCount);
  return Array.from(new Set(indexes));
}

function formatHostList(hosts: HostDefinition[]): string[] {
  return hosts.flatMap((host) => [
    `- ${host.label}`,
    `  Config: ${host.configPath}`,
    `  Extensions: ${host.extensionsDir}`,
    ...(host.incompatibilityReason ? [`  Note: ${host.incompatibilityReason}`] : [])
  ]);
}

function parseArgs(argv: string[]): ParsedArgs {
  const parsed: ParsedArgs = {};
  for (let index = 0; index < argv.length; index += 1) {
    const entry = argv[index]!;
    if (!entry.startsWith("--")) {
      continue;
    }
    const key = entry.slice(2);
    if (key === "target") {
      throw new Error("--target has been removed; omit it to auto-detect Claw or pass --config-path and --extensions-dir");
    }
    if (!SUPPORTED_ARGS.has(key)) {
      throw new Error(`unknown option: --${key}`);
    }
    const next = argv[index + 1];
    if (next && !next.startsWith("--")) {
      parsed[key as keyof ParsedArgs] = next;
      index += 1;
    } else {
      parsed[key as keyof ParsedArgs] = "true";
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
  await sanitizeExtensionPackageJson(destination);
}

async function sanitizeExtensionPackageJson(destination: string): Promise<void> {
  const packageJsonPath = join(destination, "package.json");
  if (!existsSync(packageJsonPath)) {
    return;
  }
  const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8")) as Record<string, unknown>;
  delete packageJson.dependencies;
  delete packageJson.optionalDependencies;
  await writeFile(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`);
}

async function copyHermesPlatformPackage(packageRoot: string, destination: string) {
  const source = join(packageRoot, "hermes", "platforms", HERMES_PLATFORM_ID);
  if (!existsSync(source)) {
    throw new Error(`Hermes platform package does not exist: ${source}`);
  }
  await rm(destination, { recursive: true, force: true });
  await mkdir(dirname(destination), { recursive: true });
  await cp(source, destination, { recursive: true, force: true });
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

async function readHermesPluginBuildInfo(destination: string): Promise<string> {
  const manifestPath = join(destination, "plugin.yaml");
  try {
    const manifest = parseYaml(await readFile(manifestPath, "utf8")) as { version?: unknown };
    const version = typeof manifest?.version === "string" || typeof manifest?.version === "number" ? String(manifest.version) : "unknown";
    const adapter = await readFile(join(destination, "adapter.py"));
    const digest = createHash("sha256").update(adapter).digest("hex").slice(0, 12);
    return `${PLUGIN_ID}/hermes@${version} sha256:${digest}`;
  } catch {
    return `${PLUGIN_ID}/hermes@unknown sha256:missing`;
  }
}

async function readOpenClawConfig(configPath: string): Promise<OpenClawConfig & Record<string, unknown>> {
  if (!existsSync(configPath)) {
    return {};
  }

  return JSON.parse(await readFile(configPath, "utf8")) as OpenClawConfig & Record<string, unknown>;
}

async function updateHermesConfig(configPath: string): Promise<void> {
  await mkdir(dirname(configPath), { recursive: true });
  const config = await readHermesConfig(configPath);
  const plugins = ensureObject(config, "plugins");
  plugins.enabled = dedupeStrings([...(Array.isArray(plugins.enabled) ? plugins.enabled : []), HERMES_PLUGIN_KEY, HERMES_PLATFORM_ID]);

  const platforms = ensureObject(config, "platforms");
  const platform = ensureObject(platforms, HERMES_PLATFORM_ID);
  platform.enabled = true;
  ensureObject(platform, "extra");

  const display = ensureObject(config, "display");
  const displayPlatforms = ensureObject(display, "platforms");
  const displayPlatform = ensureObject(displayPlatforms, HERMES_PLATFORM_ID);
  displayPlatform.show_reasoning = true;

  await writeFile(configPath, stringifyYaml(config));
}

async function readHermesConfig(configPath: string): Promise<Record<string, unknown>> {
  if (!existsSync(configPath)) {
    return {};
  }
  const parsed = parseYaml(await readFile(configPath, "utf8"));
  return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : {};
}

async function updateHermesEnv(configPath: string, values: { botId: string; secret: string; wsUrl: string }): Promise<void> {
  const envPath = join(dirname(configPath), ".env");
  await mkdir(dirname(envPath), { recursive: true });
  const existing = existsSync(envPath) ? await readFile(envPath, "utf8") : "";
  const updates = new Map<string, string>([
    [HERMES_ENV_KEYS.botId, values.botId],
    [HERMES_ENV_KEYS.secret, values.secret],
    [HERMES_ENV_KEYS.wsUrl, values.wsUrl]
  ]);
  const seen = new Set<string>();
  const lines = existing.split(/\r?\n/).map((line) => {
    const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=/);
    if (!match || !updates.has(match[1]!)) {
      return line;
    }
    const key = match[1]!;
    seen.add(key);
    return `${key}=${quoteEnvValue(updates.get(key)!)}`;
  });
  for (const [key, value] of updates) {
    if (!seen.has(key)) {
      lines.push(`${key}=${quoteEnvValue(value)}`);
    }
  }
  await writeFile(envPath, `${lines.filter((line, index, all) => line || index < all.length - 1).join("\n")}\n`);
}

function quoteEnvValue(value: string): string {
  return JSON.stringify(value);
}

function isHermesDestination(configPath: string, extensionsDir: string): boolean {
  const tail = extensionsDir.split(/[\\/]/).filter(Boolean).slice(-2).join("/");
  return configPath.endsWith("config.yaml") && (tail === "plugins/platforms" || tail.endsWith("/plugins"));
}

function normalizeHermesPlatformsDir(extensionsDir: string): string {
  const parts = extensionsDir.split(/[\\/]/).filter(Boolean);
  if (parts.at(-1) === "platforms" && parts.at(-2) === "plugins") {
    return extensionsDir;
  }
  if (parts.at(-1) === "plugins") {
    return join(extensionsDir, "platforms");
  }
  return extensionsDir;
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
