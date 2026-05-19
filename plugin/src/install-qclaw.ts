import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";

type InstallInput = {
  packageRoot: string;
  extensionsDir: string;
  configPath: string;
  gateway?: string;
  botId?: string;
  secret?: string;
  consoleHost?: string;
  consolePort?: number;
};

type InstallTarget = "qclaw" | "openclaw";

type ParsedArgs = {
  target?: string;
  gateway?: string;
  "bot-id"?: string;
  secret?: string;
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
const DEFAULT_QCLAW_HOME = resolve(homedir(), ".qclaw");
const DEFAULT_OPENCLAW_HOME = resolve(homedir(), ".openclaw");
const DEFAULT_EXTENSIONS_DIR = resolve(
  homedir(),
  "Library/Application Support/QClaw/openclaw/config/extensions"
);
const DEFAULT_OPENCLAW_EXTENSIONS_DIR = resolve(DEFAULT_OPENCLAW_HOME, "extensions");
const COPY_ITEMS = ["dist", "openclaw.plugin.json", "package.json", "bin", "web-dist"] as const;

const INSTALL_TARGETS: Record<
  InstallTarget,
  {
    label: string;
    defaultConfigPath: string;
    defaultExtensionsDir: string;
  }
> = {
  qclaw: {
    label: "QClaw",
    defaultConfigPath: join(DEFAULT_QCLAW_HOME, "openclaw.json"),
    defaultExtensionsDir: DEFAULT_EXTENSIONS_DIR
  },
  openclaw: {
    label: "OpenClaw",
    defaultConfigPath: join(DEFAULT_OPENCLAW_HOME, "openclaw.json"),
    defaultExtensionsDir: DEFAULT_OPENCLAW_EXTENSIONS_DIR
  }
};

export async function installIntoQClaw(input: InstallInput): Promise<{
  configPath: string;
  extensionsDir: string;
  destination: string;
  gatewayBaseUrl: string;
}> {
  return installIntoHost(input, "QClaw");
}

export async function installIntoOpenClaw(input: InstallInput): Promise<{
  configPath: string;
  extensionsDir: string;
  destination: string;
  gatewayBaseUrl: string;
}> {
  return installIntoHost(input, "OpenClaw");
}

async function installIntoHost(
  input: InstallInput,
  hostLabel: "QClaw" | "OpenClaw"
): Promise<{
  configPath: string;
  extensionsDir: string;
  destination: string;
  gatewayBaseUrl: string;
}> {
  await mkdir(input.extensionsDir, { recursive: true });
  const destination = join(input.extensionsDir, PLUGIN_ID);
  await mkdir(destination, { recursive: true });

  await copyPublishablePackage(input.packageRoot, destination);

  await mkdir(dirname(input.configPath), { recursive: true });
  const config = await readOpenClawConfig(input.configPath);
  const inferredGateway = inferGatewaySettings(config);

  const gatewayBaseUrl = input.gateway?.trim() || inferredGateway.baseUrl;
  if (!gatewayBaseUrl) {
    throw new Error(`missing gateway URL and no local ${hostLabel} gateway could be inferred`);
  }

  const secret = input.secret?.trim() || inferredGateway.secret;
  if (!secret) {
    throw new Error(`missing gateway secret and no local ${hostLabel} gateway token could be inferred`);
  }

  const botId = input.botId?.trim();
  const consoleHost = input.consoleHost?.trim();
  const consolePort = normalizePort(input.consolePort);

  const plugins = ensureObject(config, "plugins");
  plugins.enabled = true;
  plugins.allow = dedupeStrings([...(Array.isArray(plugins.allow) ? plugins.allow : []), PLUGIN_ID]);

  const load = ensureObject(plugins, "load");
  load.paths = dedupeStrings([...(Array.isArray(load.paths) ? load.paths : []), input.extensionsDir]);

  const entries = ensureObject(plugins, "entries");
  const previousEntry = ensureObject(entries, PLUGIN_ID);
  const previousConfig = ensureObject(previousEntry, "config");
  const previousGateway = ensureObject(previousConfig, "gateway");
  previousGateway.baseUrl = gatewayBaseUrl;
  previousGateway.secret = secret;
  if (botId) {
    previousGateway.botId = botId;
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
    gatewayBaseUrl
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
  const target = parseInstallTarget(args.target);
  if (!target) {
    throw new Error("expected --target qclaw or --target openclaw");
  }
  const targetInfo = INSTALL_TARGETS[target];

  const configPath = resolve(args["config-path"] ?? targetInfo.defaultConfigPath);
  const extensionsDir = resolve(args["extensions-dir"] ?? targetInfo.defaultExtensionsDir);

  const install = target === "openclaw" ? installIntoOpenClaw : installIntoQClaw;
  const result = await install({
    packageRoot: input.packageRoot,
    extensionsDir,
    configPath,
    gateway: args.gateway,
    botId: args["bot-id"],
    secret: args.secret,
    consoleHost: args["console-host"],
    consolePort: args["console-port"] ? Number(args["console-port"]) : undefined
  });

  process.stdout.write(
    [
      `Installed ${PLUGIN_ID} into ${targetInfo.label}.`,
      `Extensions: ${result.extensionsDir}`,
      `Config: ${result.configPath}`,
      `Gateway: ${result.gatewayBaseUrl}`,
      `Restart ${targetInfo.label} to load the plugin.`
    ].join("\n") + "\n"
  );
}

function parseArgs(argv: string[]): ParsedArgs {
  const parsed: ParsedArgs = {};
  for (let index = 0; index < argv.length; index += 1) {
    const entry = argv[index]!;
    if (!entry.startsWith("--")) {
      continue;
    }
    parsed[entry.slice(2) as keyof ParsedArgs] = argv[index + 1] ?? "";
    index += 1;
  }
  return parsed;
}

function parseInstallTarget(target: string | undefined): InstallTarget | undefined {
  if (target === "qclaw" || target === "openclaw") {
    return target;
  }
  return undefined;
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
