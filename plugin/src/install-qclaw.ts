import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { chmod, cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { promisify } from "node:util";
import { select } from "@inquirer/prompts";
import { parse as parseYaml, stringify as stringifyYaml } from "yaml";
import { detectHostKind } from "./host";
import { writeCodexChannelInstallConfig } from "./codex-channel";
import { DEFAULT_CODEX_WORKSPACE_ROOT } from "./codex-workspace";
import { detectCodexInstallation, getDefaultCodexBinaryCandidates } from "./codex-runtime";

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

type WorkBuddyInstallInput = {
  packageRoot: string;
  workbuddyHome: string;
  hubWsUrl?: string;
  hubBotId?: string;
  hubSecret?: string;
  cleanupWorkBuddyChannelProcesses?: (targets: string[]) => Promise<void>;
};

type CodexInstallInput = {
  packageRoot: string;
  installRoot: string;
  hubWsUrl?: string;
  hubBotId?: string;
  hubSecret?: string;
  codexBinPath?: string;
  nodeBinPath?: string;
  workspaceRoot?: string;
  launchAgent?: CodexLaunchAgentOptions;
};

type CodexLaunchAgentOptions = {
  enabled?: boolean;
  label?: string;
  launchAgentsDir?: string;
  logDir?: string;
  uid?: number;
  platform?: NodeJS.Platform;
  runLaunchctl?: LaunchctlRunner;
};

type LaunchctlRunner = (args: string[]) => Promise<unknown>;

type CodexLaunchAgentStatus = {
  enabled: boolean;
  label: string;
  plistPath: string;
  stdoutPath: string;
  stderrPath: string;
  loaded: boolean;
  serviceTarget: string;
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
  "workbuddy-home"?: string;
};

export type HostDefinition = {
  id: string;
  label: string;
  configPath: string;
  extensionsDir: string;
  installKind?: "openclaw" | "hermes" | "workbuddy" | "codex";
  workbuddyHome?: string;
  codexBinPath?: string;
  incompatibilityReason?: string;
};

type InstallDestination = {
  configPath: string;
  extensionsDir: string;
  label: string;
  installKind: "openclaw" | "hermes" | "workbuddy" | "codex";
  workbuddyHome?: string;
  codexBinPath?: string;
};

export type PromptSelectHost = (
  hosts: HostDefinition[],
  incompatibleHosts: HostDefinition[]
) => Promise<HostDefinition>;

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
      detectCreatedFiles?: unknown;
      fileWorkspaceDirs?: unknown;
      createdFilesMaxFileBytes?: unknown;
      createdFilesMaxCount?: unknown;
      createdFilesExclude?: unknown;
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

type Hub53AIInstallSettings = {
  enabled: boolean;
  botId: string;
  secret: string;
  wsUrl: string;
  accessPolicy?: string;
  allowFrom?: string[];
  sendThinkingMessage?: boolean;
};

const PLUGIN_ID = "claw-control-center";
const LEGACY_PLUGIN_ID = "53ai-openclaw";
const COPY_ITEMS = [
  "dist",
  "openclaw.plugin.json",
  ".codebuddy-plugin",
  ".mcp.json",
  "package.json",
  "bin",
  "web-dist"
] as const;
const HERMES_PLATFORM_ID = "53aihub";
const HERMES_PLUGIN_KEY = `platforms/${HERMES_PLATFORM_ID}`;
const WORKBUDDY_MARKETPLACE_ID = "my-experts";
const WORKBUDDY_PLUGIN_ID = "53aihub-workbuddy";
const WORKBUDDY_SHARED_SESSION_ID = "53aihub-workbuddy-shared";
const WORKBUDDY_HISTORY_SCOPE = "all";
const CODEX_CHANNEL_INSTALL_ROOT = join(homedir(), ".53ai", "codex-channel");
const CODEX_CHANNEL_LAUNCH_AGENT_LABEL = "com.53ai.codex-channel";
const HERMES_ENV_KEYS = {
  botId: "HUB53AI_BOT_ID",
  secret: "HUB53AI_SECRET",
  wsUrl: "HUB53AI_WS_URL"
} as const;
const execFileAsync = promisify(execFile);
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
  "console-port",
  "workbuddy-home"
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

export async function installIntoWorkBuddy(input: WorkBuddyInstallInput): Promise<{
  marketplacePath: string;
  destination: string;
  hub53aiConfigured: boolean;
  pluginBuild: string;
}> {
  const hubWsUrl = input.hubWsUrl?.trim();
  const hubBotId = input.hubBotId?.trim();
  const hubSecret = input.hubSecret?.trim();
  if (!hubWsUrl || !hubBotId || !hubSecret) {
    throw new Error("WorkBuddy install requires --hub-ws-url, --hub-bot-id, and --hub-secret");
  }

  const marketplaceRoot = join(input.workbuddyHome, "plugins", "marketplaces", WORKBUDDY_MARKETPLACE_ID);
  const pluginsRoot = join(marketplaceRoot, "plugins");
  const destination = join(pluginsRoot, WORKBUDDY_PLUGIN_ID);
  const channelEntryPath = join(destination, "dist", "codebuddy-channel.cjs");
  const supervisorEntryPath = join(destination, "dist", "workbuddy-supervisor.cjs");
  const workspaceDir = join(input.workbuddyHome, "channels", "53aihub-workspace");
  await mkdir(pluginsRoot, { recursive: true });
  await mkdir(destination, { recursive: true });
  await mkdir(workspaceDir, { recursive: true });
  await copyPublishablePackage(input.packageRoot, destination);
  await updateWorkBuddyMcpConfig(join(destination, ".mcp.json"), {
    botId: hubBotId,
    secret: hubSecret,
    wsUrl: hubWsUrl,
    supervisorEntryPath,
    channelEntryPath,
    workbuddyHome: input.workbuddyHome,
    workspaceDir,
    sessionId: WORKBUDDY_SHARED_SESSION_ID,
    historyScope: WORKBUDDY_HISTORY_SCOPE
  });
  const marketplacePath = await updateWorkBuddyMarketplace(marketplaceRoot);
  await updateWorkBuddyKnownMarketplaces(input.workbuddyHome, marketplaceRoot);
  await updateWorkBuddyEnabledPlugins(input.workbuddyHome);
  await (input.cleanupWorkBuddyChannelProcesses ?? cleanupOrphanedWorkBuddyChannelProcesses)([
    channelEntryPath,
    supervisorEntryPath,
    `--session-id ${WORKBUDDY_SHARED_SESSION_ID}`,
    `--session-id ${WORKBUDDY_SHARED_SESSION_ID.replace(/ /g, "\\ ")}`
  ]);

  return {
    marketplacePath,
    destination,
    hub53aiConfigured: true,
    pluginBuild: await readPluginBuildInfo(destination)
  };
}

export async function installIntoCodex(input: CodexInstallInput): Promise<{
  installRoot: string;
  destination: string;
  configPath: string;
  startScriptPath: string;
  channelEntryPath: string;
  workspaceRoot: string;
  codexBinPath: string;
  codexVersion: string;
  hubBotId: string;
  launchAgent: CodexLaunchAgentStatus;
  hub53aiConfigured: boolean;
  pluginBuild: string;
}> {
  const hubWsUrl = input.hubWsUrl?.trim();
  const hubBotId = input.hubBotId?.trim();
  const hubSecret = input.hubSecret?.trim();
  if (!hubWsUrl || !hubBotId || !hubSecret) {
    throw new Error("Codex install requires --hub-ws-url, --hub-bot-id, and --hub-secret");
  }

  const detectedCodex = await detectCodexInstallation(
    input.codexBinPath ? { candidatePaths: [input.codexBinPath] } : {}
  );
  const installRoot = resolve(input.installRoot);
  const destination = join(installRoot, "plugin");
  const configPath = join(installRoot, "config.json");
  const startScriptPath = join(installRoot, "start-codex-channel.sh");
  const workspaceRoot = resolve(input.workspaceRoot || DEFAULT_CODEX_WORKSPACE_ROOT);
  const channelEntryPath = join(destination, "dist", "codex-channel.cjs");
  const nodeBinPath = input.nodeBinPath || process.execPath;

  await mkdir(destination, { recursive: true });
  await copyPublishablePackage(input.packageRoot, destination);
  await writeCodexChannelInstallConfig(configPath, {
    wsUrl: hubWsUrl,
    botId: hubBotId,
    secret: hubSecret,
    codexBinPath: detectedCodex.binPath,
    codexVersion: detectedCodex.version,
    workspaceRoot,
    channelEntryPath
  });
  await writeCodexChannelStartScript(startScriptPath, configPath, channelEntryPath, nodeBinPath);
  const launchAgent = await installCodexLaunchAgent({
    installRoot,
    startScriptPath,
    configPath,
    channelEntryPath,
    nodeBinPath,
    options: input.launchAgent
  });

  return {
    installRoot,
    destination,
    configPath,
    startScriptPath,
    channelEntryPath,
    workspaceRoot,
    codexBinPath: detectedCodex.binPath,
    codexVersion: detectedCodex.version,
    hubBotId,
    launchAgent,
    hub53aiConfigured: true,
    pluginBuild: await readPluginBuildInfo(destination)
  };
}

function formatCodexInstallResult(result: Awaited<ReturnType<typeof installIntoCodex>>): string[] {
  return [
    "Installed 53AIHub Codex channel.",
    `Plugin: ${result.destination}`,
    `Config: ${result.configPath}`,
    `Start script: ${result.startScriptPath}`,
    `Codex: ${result.codexBinPath} (${result.codexVersion})`,
    `Workspace root: ${result.workspaceRoot}`,
    `Bot ID: ${result.hubBotId}`,
    `LaunchAgent: ${result.launchAgent.label} (${result.launchAgent.loaded ? "loaded" : "not loaded"})`,
    `LaunchAgent plist: ${result.launchAgent.plistPath}`,
    `Logs: ${result.launchAgent.stdoutPath} ${result.launchAgent.stderrPath}`,
    `53AIHub: ${result.hub53aiConfigured ? "configured" : "not configured"}`,
    `Plugin build: ${result.pluginBuild}`,
    "Codex channel is managed by LaunchAgent and should connect automatically."
  ];
}

async function writeCodexChannelStartScript(
  startScriptPath: string,
  configPath: string,
  channelEntryPath: string,
  nodeBinPath: string
): Promise<void> {
  await mkdir(dirname(startScriptPath), { recursive: true });
  await writeFile(
    startScriptPath,
    [
      "#!/bin/sh",
      "set -eu",
      `export HUB53AI_CODEX_CHANNEL_CONFIG=${quoteShellValue(configPath)}`,
      `exec ${quoteShellValue(nodeBinPath)} ${quoteShellValue(channelEntryPath)}`,
      ""
    ].join("\n")
  );
  await chmod(startScriptPath, 0o755);
}

async function installCodexLaunchAgent(input: {
  installRoot: string;
  startScriptPath: string;
  configPath: string;
  channelEntryPath: string;
  nodeBinPath: string;
  options?: CodexLaunchAgentOptions;
}): Promise<CodexLaunchAgentStatus> {
  const options = input.options ?? {};
  const label = options.label || CODEX_CHANNEL_LAUNCH_AGENT_LABEL;
  const launchAgentsDir = options.launchAgentsDir || join(homedir(), "Library", "LaunchAgents");
  const logDir = options.logDir || join(input.installRoot, "logs");
  const plistPath = join(launchAgentsDir, `${label}.plist`);
  const stdoutPath = join(logDir, "codex-channel.out.log");
  const stderrPath = join(logDir, "codex-channel.err.log");
  const platform = options.platform || process.platform;
  const uid = options.uid ?? (typeof process.getuid === "function" ? process.getuid() : undefined);
  const enabled = options.enabled ?? platform === "darwin";
  const serviceTarget = uid === undefined ? "" : `gui/${uid}/${label}`;

  await mkdir(launchAgentsDir, { recursive: true });
  await mkdir(logDir, { recursive: true });
  await writeFile(
    plistPath,
    buildCodexLaunchAgentPlist({
      label,
      startScriptPath: input.startScriptPath,
      configPath: input.configPath,
      channelEntryPath: input.channelEntryPath,
      nodeBinPath: input.nodeBinPath,
      workingDirectory: input.installRoot,
      stdoutPath,
      stderrPath
    })
  );

  if (!enabled) {
    return {
      enabled: false,
      label,
      plistPath,
      stdoutPath,
      stderrPath,
      loaded: false,
      serviceTarget
    };
  }
  if (platform !== "darwin") {
    throw new Error("Codex LaunchAgent autostart is only supported on macOS");
  }
  if (uid === undefined) {
    throw new Error("Cannot determine user id for Codex LaunchAgent");
  }

  const runLaunchctl = options.runLaunchctl || defaultLaunchctlRunner;
  const domain = `gui/${uid}`;
  await runLaunchctl(["bootout", domain, plistPath]).catch(() => undefined);
  await runLaunchctl(["bootstrap", domain, plistPath]);
  await runLaunchctl(["enable", serviceTarget]);
  await runLaunchctl(["kickstart", "-k", serviceTarget]);
  await runLaunchctl(["print", serviceTarget]);

  return {
    enabled: true,
    label,
    plistPath,
    stdoutPath,
    stderrPath,
    loaded: true,
    serviceTarget
  };
}

async function defaultLaunchctlRunner(args: string[]): Promise<void> {
  await execFileAsync("launchctl", args);
}

function buildCodexLaunchAgentPlist(input: {
  label: string;
  startScriptPath: string;
  configPath: string;
  channelEntryPath: string;
  nodeBinPath: string;
  workingDirectory: string;
  stdoutPath: string;
  stderrPath: string;
}): string {
  return [
    `<?xml version="1.0" encoding="UTF-8"?>`,
    `<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">`,
    `<plist version="1.0">`,
    `<dict>`,
    `  <key>Label</key>`,
    `  <string>${escapePlistString(input.label)}</string>`,
    `  <key>ProgramArguments</key>`,
    `  <array>`,
    `    <string>${escapePlistString(input.nodeBinPath)}</string>`,
    `    <string>${escapePlistString(input.channelEntryPath)}</string>`,
    `  </array>`,
    `  <key>EnvironmentVariables</key>`,
    `  <dict>`,
    `    <key>HUB53AI_CODEX_CHANNEL_CONFIG</key>`,
    `    <string>${escapePlistString(input.configPath)}</string>`,
    `    <key>HUB53AI_CODEX_CHANNEL_START_SCRIPT</key>`,
    `    <string>${escapePlistString(input.startScriptPath)}</string>`,
    `  </dict>`,
    `  <key>WorkingDirectory</key>`,
    `  <string>${escapePlistString(input.workingDirectory)}</string>`,
    `  <key>RunAtLoad</key>`,
    `  <true/>`,
    `  <key>KeepAlive</key>`,
    `  <true/>`,
    `  <key>ProcessType</key>`,
    `  <string>Interactive</string>`,
    `  <key>ThrottleInterval</key>`,
    `  <integer>10</integer>`,
    `  <key>StandardOutPath</key>`,
    `  <string>${escapePlistString(input.stdoutPath)}</string>`,
    `  <key>StandardErrorPath</key>`,
    `  <string>${escapePlistString(input.stderrPath)}</string>`,
    `</dict>`,
    `</plist>`,
    ``
  ].join("\n");
}

function escapePlistString(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
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
    previousHub.detectCreatedFiles =
      typeof previousHub.detectCreatedFiles === "boolean" ? previousHub.detectCreatedFiles : true;
    previousHub.fileWorkspaceDirs = Array.isArray(previousHub.fileWorkspaceDirs) ? previousHub.fileWorkspaceDirs : [];
    previousHub.createdFilesMaxFileBytes =
      typeof previousHub.createdFilesMaxFileBytes === "number" ? previousHub.createdFilesMaxFileBytes : 10 * 1024 * 1024;
    previousHub.createdFilesMaxCount =
      typeof previousHub.createdFilesMaxCount === "number" ? previousHub.createdFilesMaxCount : 20;
    previousHub.createdFilesExclude = Array.isArray(previousHub.createdFilesExclude)
      ? previousHub.createdFilesExclude
      : [];
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
  promptSelectHost?: PromptSelectHost;
  ttyPath?: string;
}): Promise<void> {
  const argv = input.argv ?? process.argv.slice(2);
  if (argv[0] !== "install" && argv[0] !== "install-workbuddy" && argv[0] !== "install-codex") {
    throw new Error("expected subcommand: install, install-workbuddy, or install-codex");
  }

  const args = parseArgs(argv.slice(1));
  if (argv[0] === "install-codex") {
    const result = await installIntoCodex({
      packageRoot: input.packageRoot,
      installRoot: CODEX_CHANNEL_INSTALL_ROOT,
      hubWsUrl: args["hub-ws-url"],
      hubBotId: args["hub-bot-id"],
      hubSecret: args["hub-secret"]
    });
    process.stdout.write(formatCodexInstallResult(result).join("\n") + "\n");
    return;
  }

  if (argv[0] === "install-workbuddy") {
    const result = await installIntoWorkBuddy({
      packageRoot: input.packageRoot,
      workbuddyHome: resolve(args["workbuddy-home"] ?? join(homedir(), ".workbuddy")),
      hubWsUrl: args["hub-ws-url"],
      hubBotId: args["hub-bot-id"],
      hubSecret: args["hub-secret"]
    });
    process.stdout.write(
      [
        `Installed ${WORKBUDDY_PLUGIN_ID} into WorkBuddy local marketplace.`,
        `Plugin: ${result.destination}`,
        `Marketplace: ${result.marketplacePath}`,
        `53AIHub: ${result.hub53aiConfigured ? "configured" : "not configured"}`,
        `Plugin build: ${result.pluginBuild}`,
        "Restart WorkBuddy or refresh local marketplaces to load the channel plugin."
      ].join("\n") + "\n"
    );
    return;
  }

  const destinations = await resolveInstallDestinations(args, {
    hostDefinitions: input.hostDefinitions,
    selectHosts: input.selectHosts,
    selectHost: input.selectHost,
    promptSelectHost: input.promptSelectHost,
    ttyPath: input.ttyPath
  });

  for (const destination of destinations) {
    if (destination.installKind === "codex") {
      const result = await installIntoCodex({
        packageRoot: input.packageRoot,
        installRoot: destination.extensionsDir,
        hubWsUrl: args["hub-ws-url"],
        hubBotId: args["hub-bot-id"],
        hubSecret: args["hub-secret"],
        codexBinPath: destination.codexBinPath || destination.configPath
      });

      process.stdout.write(formatCodexInstallResult(result).join("\n") + "\n");
      continue;
    }

    if (destination.installKind === "workbuddy") {
      const result = await installIntoWorkBuddy({
        packageRoot: input.packageRoot,
        workbuddyHome: destination.workbuddyHome ?? resolve(args["workbuddy-home"] ?? join(homedir(), ".workbuddy")),
        hubWsUrl: args["hub-ws-url"],
        hubBotId: args["hub-bot-id"],
        hubSecret: args["hub-secret"]
      });

      process.stdout.write(
        [
          `Installed ${WORKBUDDY_PLUGIN_ID} into WorkBuddy local marketplace.`,
          `Plugin: ${result.destination}`,
          `Marketplace: ${result.marketplacePath}`,
          `53AIHub: ${result.hub53aiConfigured ? "configured" : "not configured"}`,
          `Plugin build: ${result.pluginBuild}`,
          "Restart WorkBuddy or refresh local marketplaces to load the channel plugin."
        ].join("\n") + "\n"
      );
      continue;
    }

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
    promptSelectHost?: PromptSelectHost;
    ttyPath?: string;
  } = {}
): Promise<InstallDestination[]> {
  const explicitConfigPath = args["config-path"] ? resolve(args["config-path"]) : undefined;
  const explicitExtensionsDir = args["extensions-dir"] ? resolve(args["extensions-dir"]) : undefined;
  const explicitWorkBuddyHome = args["workbuddy-home"] ? resolve(args["workbuddy-home"]) : undefined;

  if (explicitWorkBuddyHome) {
    if (explicitConfigPath || explicitExtensionsDir) {
      throw new Error("pass --workbuddy-home by itself, or pass --config-path and --extensions-dir together for Claw-style hosts");
    }
    return [toInstallDestination(createWorkBuddyHostDefinition(explicitWorkBuddyHome))];
  }

  if (explicitConfigPath && explicitExtensionsDir) {
    const hermes = isHermesDestination(explicitConfigPath, explicitExtensionsDir);
    const clawLabel = inferDetectedClawLabel(explicitConfigPath, explicitExtensionsDir);
    return [{
      configPath: explicitConfigPath,
      extensionsDir: hermes ? normalizeHermesPlatformsDir(explicitExtensionsDir) : explicitExtensionsDir,
      label: hermes ? "Hermes" : clawLabel,
      installKind: hermes ? "hermes" : "openclaw"
    }];
  }

  if (explicitConfigPath || explicitExtensionsDir) {
    throw new Error("pass --config-path and --extensions-dir together, or omit both to auto-detect compatible agents");
  }

  const detected = detectInstallHosts(options.hostDefinitions ?? getDefaultHostDefinitions());
  const compatible = detected;
  const incompatible: HostDefinition[] = [];
  if (compatible.length > 0) {
    const selected = options.selectHosts
      ? validateSingleSelectedHost(await options.selectHosts(compatible, incompatible), compatible)
      : options.selectHost
        ? validateSingleSelectedHost(await options.selectHost(compatible), compatible)
        : validateSingleSelectedHost(await (options.promptSelectHost ?? promptForInstallHost)(compatible, incompatible), compatible);
    return [toInstallDestination(selected)];
  }
  throw new Error(
    [
      "could not auto-detect an installed compatible agent.",
      "Pass --config-path and --extensions-dir to install into a specific Claw-style host.",
      "Pass --workbuddy-home to install into a specific WorkBuddy home.",
      "Use install-codex to install the 53AIHub Codex channel when Codex is installed locally.",
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
  const workbuddyHome = resolve(home, ".workbuddy");
  const codexHost = createCodexHostDefinition();
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
    },
    createWorkBuddyHostDefinition(workbuddyHome),
    ...(codexHost ? [codexHost] : [])
  ];
}

function inferDetectedClawLabel(configPath: string, extensionsDir: string): "QClaw" | "OpenClaw" | "Hermes" | "WorkBuddy" {
  const kind = detectHostKind(`${configPath}\n${extensionsDir}`);
  if (kind === "qclaw") return "QClaw";
  if (kind === "hermes") return "Hermes";
  if (kind === "workbuddy") return "WorkBuddy";
  return "OpenClaw";
}

function createWorkBuddyHostDefinition(workbuddyHome: string): HostDefinition {
  return {
    id: "workbuddy",
    label: "WorkBuddy",
    configPath: join(workbuddyHome, "settings.json"),
    extensionsDir: join(workbuddyHome, "plugins", "marketplaces", WORKBUDDY_MARKETPLACE_ID, "plugins"),
    installKind: "workbuddy",
    workbuddyHome
  };
}

function createCodexHostDefinition(): HostDefinition | undefined {
  const detected = getDefaultCodexBinaryCandidates().find((candidate) => existsSync(candidate.path));
  if (!detected) {
    return undefined;
  }
  return {
    id: "codex",
    label: "Codex",
    configPath: detected.path,
    extensionsDir: CODEX_CHANNEL_INSTALL_ROOT,
    installKind: "codex",
    codexBinPath: detected.path
  };
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

async function promptForInstallHost(
  hosts: HostDefinition[],
  incompatibleHosts: HostDefinition[]
): Promise<HostDefinition> {
  try {
    if (!process.stdin.isTTY || !process.stdout.isTTY) {
      throw new Error("interactive terminal is required");
    }
    const choices = [
      ...hosts.map((host) => ({
        name: host.label,
        value: host,
        short: host.label,
        description: `Config: ${host.configPath}\nExtensions: ${host.extensionsDir}`
      })),
      ...incompatibleHosts.map((host) => ({
        name: host.label,
        value: host,
        short: host.label,
        disabled: host.incompatibilityReason ?? "incompatible plugin format",
        description: `Config: ${host.configPath}\nExtensions: ${host.extensionsDir}`
      }))
    ];

    return await select<HostDefinition>({
      message: "Choose the local agent to connect with this 53AIHub agent:",
      choices,
      pageSize: Math.min(Math.max(choices.length, 5), 12),
      loop: true
    });
  } catch (error) {
    throw new Error(
      [
        "multiple compatible agents were detected, but no interactive terminal was available.",
        "Run the installer again in an interactive terminal, pass --config-path and --extensions-dir, pass --workbuddy-home, or use install-codex.",
        "",
        "Detected locations:",
        ...formatHostList(hosts),
        ...(error instanceof Error && error.message ? ["", `Prompt error: ${error.message}`] : [])
      ].join("\n")
    );
  }
}

function toInstallDestination(host: HostDefinition): InstallDestination {
  return {
    configPath: resolve(host.configPath),
    extensionsDir: resolve(host.extensionsDir),
    label: host.label,
    installKind: host.installKind ?? "openclaw",
    workbuddyHome: host.workbuddyHome ? resolve(host.workbuddyHome) : undefined,
    codexBinPath: host.codexBinPath ? resolve(host.codexBinPath) : undefined
  };
}

function validateSingleSelectedHost(selected: HostDefinition | HostDefinition[], compatible: HostDefinition[]): HostDefinition {
  if (Array.isArray(selected)) {
    if (selected.length !== 1) {
      throw new Error("select exactly one compatible agent for this 53AIHub agent");
    }
    return validateSingleSelectedHost(selected[0]!, compatible);
  }
  const compatibleIds = new Set(compatible.map((host) => host.id));
  if (!compatibleIds.has(selected.id)) {
    throw new Error(`selected host is not installable: ${selected.label}`);
  }
  return selected;
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
      throw new Error("--target has been removed; omit it to auto-detect compatible agents or pass explicit path options");
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

async function updateWorkBuddyMcpConfig(
  mcpPath: string,
  values: {
    botId: string;
    secret: string;
    wsUrl: string;
    supervisorEntryPath: string;
    channelEntryPath: string;
    workbuddyHome: string;
    workspaceDir: string;
    sessionId: string;
    historyScope: string;
  }
): Promise<void> {
  if (!existsSync(mcpPath)) {
    throw new Error(`WorkBuddy MCP config does not exist: ${mcpPath}`);
  }
  const config = JSON.parse(await readFile(mcpPath, "utf8")) as Record<string, any>;
  const servers =
    config.mcpServers && typeof config.mcpServers === "object" && !Array.isArray(config.mcpServers)
      ? config.mcpServers
      : (config.mcpServers = {});
  const server =
    servers["53aihub-channel"] &&
    typeof servers["53aihub-channel"] === "object" &&
    !Array.isArray(servers["53aihub-channel"])
      ? servers["53aihub-channel"]
      : (servers["53aihub-channel"] = {});
  const env = server.env && typeof server.env === "object" && !Array.isArray(server.env) ? server.env : (server.env = {});
  server.command = typeof server.command === "string" && server.command.trim() ? server.command : "node";
  server.args = [values.supervisorEntryPath];
  env.HUB53AI_WS_URL = values.wsUrl;
  env.HUB53AI_BOT_ID = values.botId;
  env.HUB53AI_SECRET = values.secret;
  env.HUB53AI_ACCESS_POLICY = typeof env.HUB53AI_ACCESS_POLICY === "string" ? env.HUB53AI_ACCESS_POLICY : "open";
  env.HUB53AI_ALLOW_FROM = typeof env.HUB53AI_ALLOW_FROM === "string" ? env.HUB53AI_ALLOW_FROM : "";
  env.HUB53AI_SEND_THINKING_MESSAGE =
    typeof env.HUB53AI_SEND_THINKING_MESSAGE === "string" ? env.HUB53AI_SEND_THINKING_MESSAGE : "true";
  env.HUB53AI_CHANNEL_ENTRY_PATH = values.channelEntryPath;
  env.HUB53AI_WORKBUDDY_HOME = values.workbuddyHome;
  env.HUB53AI_WORKBUDDY_WORKSPACE = values.workspaceDir;
  env.HUB53AI_WORKBUDDY_HISTORY_SCOPE = values.historyScope;
  env.HUB53AI_WORKBUDDY_SESSION_ID = values.sessionId;
  await writeFile(mcpPath, `${JSON.stringify(config, null, 2)}\n`);
}

async function updateWorkBuddyMarketplace(marketplaceRoot: string): Promise<string> {
  const marketplaceDir = join(marketplaceRoot, ".codebuddy-plugin");
  const marketplacePath = join(marketplaceDir, "marketplace.json");
  await mkdir(marketplaceDir, { recursive: true });
  const manifest = await readWorkBuddyMarketplaceManifest(marketplacePath);
  const plugins = Array.isArray(manifest.plugins) ? manifest.plugins : [];
  manifest.name = typeof manifest.name === "string" && manifest.name.trim() ? manifest.name : WORKBUDDY_MARKETPLACE_ID;
  manifest.description =
    typeof manifest.description === "string" && manifest.description.trim()
      ? manifest.description
      : "Local WorkBuddy plugins";
  manifest.owner =
    manifest.owner && typeof manifest.owner === "object" && !Array.isArray(manifest.owner)
      ? manifest.owner
      : { name: "Local" };
  manifest.plugins = [
    ...plugins.filter((plugin) => !(plugin && typeof plugin === "object" && (plugin as any).name === WORKBUDDY_PLUGIN_ID)),
    {
      name: WORKBUDDY_PLUGIN_ID,
      description: "53AIHub Channel plugin for WorkBuddy and CodeBuddy.",
      version: "0.1.13",
      source: `./plugins/${WORKBUDDY_PLUGIN_ID}`,
      category: "productivity",
      author: {
        name: "53AI"
      },
      homepage: "https://www.53ai.com",
      license: "MIT"
    }
  ];
  await writeFile(marketplacePath, `${JSON.stringify(manifest, null, 2)}\n`);
  return marketplacePath;
}

async function updateWorkBuddyKnownMarketplaces(workbuddyHome: string, marketplaceRoot: string): Promise<string> {
  const knownPath = join(workbuddyHome, "plugins", "known_marketplaces.json");
  await mkdir(dirname(knownPath), { recursive: true });
  const known = await readJsonObject(knownPath);
  known[WORKBUDDY_MARKETPLACE_ID] = {
    type: "directory",
    source: {
      source: "directory",
      path: marketplaceRoot
    },
    installLocation: marketplaceRoot,
    isBuiltIn: false,
    autoUpdate: false,
    description: "Local WorkBuddy plugins",
    lastUpdated: new Date().toISOString()
  };
  await writeFile(knownPath, `${JSON.stringify(known, null, 2)}\n`);
  return knownPath;
}

async function updateWorkBuddyEnabledPlugins(workbuddyHome: string): Promise<string> {
  const settingsPath = join(workbuddyHome, "settings.json");
  await mkdir(dirname(settingsPath), { recursive: true });
  const settings = await readJsonObject(settingsPath);
  const enabledPlugins =
    settings.enabledPlugins && typeof settings.enabledPlugins === "object" && !Array.isArray(settings.enabledPlugins)
      ? settings.enabledPlugins
      : (settings.enabledPlugins = {});
  enabledPlugins[`${WORKBUDDY_PLUGIN_ID}@${WORKBUDDY_MARKETPLACE_ID}`] = true;
  settings.channelsEnabled = true;
  await writeFile(settingsPath, `${JSON.stringify(settings, null, 2)}\n`);
  return settingsPath;
}

async function cleanupOrphanedWorkBuddyChannelProcesses(targets: string[]): Promise<void> {
  if (process.platform === "win32") {
    return;
  }

  let stdout = "";
  try {
    const result = await execFileAsync("/bin/ps", ["-axo", "pid=,ppid=,command="], {
      maxBuffer: 1024 * 1024
    });
    stdout = String(result.stdout);
  } catch {
    return;
  }

  const orphanPids = stdout
    .split("\n")
    .map((line) => {
      const match = line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/);
      if (!match) {
        return undefined;
      }
      const [, pid, _ppid, command] = match;
      const matchesTarget = targets.some((target) => {
        if (!target) {
          return false;
        }
        if (target.startsWith("--session-id")) {
          return /(?:^|\/)codebuddy\s+--serve\b/.test(command) && command.includes(target);
        }
        return command.includes(target);
      });
      const isRelevantRuntime = /\bnode\b/.test(command) || /\bcodebuddy\b/.test(command);
      if (pid === String(process.pid) || !matchesTarget || !isRelevantRuntime) {
        return undefined;
      }
      return pid;
    })
    .filter((pid): pid is string => Boolean(pid));

  await Promise.all(
    orphanPids.map(async (pid) => {
      try {
        await execFileAsync("/bin/kill", [pid]);
      } catch {
        // The process may have exited between ps and kill.
      }
    })
  );
}

async function readWorkBuddyMarketplaceManifest(marketplacePath: string): Promise<Record<string, any>> {
  return readJsonObject(marketplacePath);
}

async function readJsonObject(path: string): Promise<Record<string, any>> {
  if (!existsSync(path)) {
    return {};
  }
  try {
    const parsed = JSON.parse(await readFile(path, "utf8"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, any> : {};
  } catch {
    return {};
  }
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

function quoteShellValue(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
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

function inferHub53AISettings(config: OpenClawConfig): Hub53AIInstallSettings {
  const pluginHub = readPluginHub53AISettings(config);
  if (pluginHub) {
    return pluginHub;
  }

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

function readPluginHub53AISettings(config: OpenClawConfig): Hub53AIInstallSettings | undefined {
  const entry = config.plugins?.entries?.[PLUGIN_ID];
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    return undefined;
  }
  const entryConfig = (entry as Record<string, unknown>).config;
  if (!entryConfig || typeof entryConfig !== "object" || Array.isArray(entryConfig)) {
    return undefined;
  }
  const hub = (entryConfig as Record<string, unknown>).hub53ai;
  if (!hub || typeof hub !== "object" || Array.isArray(hub)) {
    return undefined;
  }
  const record = hub as Record<string, unknown>;
  return {
    enabled: record.enabled !== false,
    botId: typeof record.botId === "string" ? record.botId : "",
    secret: typeof record.secret === "string" ? record.secret : "",
    wsUrl:
      typeof record.wsUrl === "string"
        ? record.wsUrl
        : typeof record.WSUrl === "string"
          ? record.WSUrl
          : typeof record.websocketUrl === "string"
            ? record.websocketUrl
            : "",
    accessPolicy: typeof record.accessPolicy === "string" ? record.accessPolicy : undefined,
    allowFrom: Array.isArray(record.allowFrom)
      ? record.allowFrom.map((entry) => String(entry)).filter(Boolean)
      : undefined,
    sendThinkingMessage:
      typeof record.sendThinkingMessage === "boolean" ? record.sendThinkingMessage : undefined
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
