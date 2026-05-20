import { existsSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";

import { createAgentEventProbe, type AgentEventProbe } from "./agent-event-probe";
import { createConsoleServer } from "./console-server";
import { createGatewayClient } from "./gateway-client";
import {
  detectHostKind,
  readHostRuntimeInfo,
  resolvePluginConfigWithHostDefaults,
  sanitizePluginConfig,
  type PluginConfig
} from "./host";

const plugin = {
  id: "claw-control-center",
  name: "Claw Control Center",
  register(api: OpenClawPluginApi) {
    let runtime: Awaited<ReturnType<typeof createRuntime>> | undefined;
    const pluginConfig = (api.pluginConfig ?? {}) as PluginConfig;
    const agentEventProbe = createAgentEventProbe();
    agentEventProbe.register(api);

    api.registerService({
      id: "claw-control-center-service",
      async start(ctx: { stateDir: string }) {
        if (runtime) {
          return;
        }

        runtime = await createRuntime({
          rootDir: api.rootDir ?? process.cwd(),
          stateDir: ctx.stateDir,
          configPath: resolveConfigPath(ctx),
          pluginConfig,
          version: api.version ?? "dev",
          agentEventProbe
        });
        await runtime.start();
        api.logger.info(`Claw Control Center gateway console started at ${runtime.baseUrl}`);
      },
      async stop() {
        if (!runtime) {
          return;
        }
        await runtime.stop();
        runtime = undefined;
      }
    });
  }
};

export default plugin;
export {
  createHub53AIBridge,
  parseIncomingMessage as parseHub53AIIncomingMessage,
  type Hub53AIConfig,
  type Hub53AIIncomingMessage,
  type Hub53AIOutgoingChunk,
  type Hub53AIStatusSnapshot
} from "./53aihub-client";
export { createConsoleServer } from "./console-server";
export { createGatewayClient } from "./gateway-client";
export { installIntoOpenClaw, installIntoQClaw, runInstallCommand } from "./install-qclaw";

function resolveConfigPath(ctx: { stateDir: string }): string {
  const candidates = [
    ctx.stateDir ? join(ctx.stateDir, "openclaw.json") : undefined,
    process.env.HOME ? join(process.env.HOME, ".qclaw", "openclaw.json") : undefined,
    process.env.HOME ? join(process.env.HOME, ".openclaw", "openclaw.json") : undefined
  ].filter((candidate): candidate is string => Boolean(candidate));

  return candidates.find((candidate) => existsSync(candidate)) ?? candidates[0] ?? "openclaw.json";
}

function resolveWebDir(rootDir: string): string | undefined {
  const candidates = [
    join(rootDir, "web-dist"),
    join(rootDir, "..", "web", "dist"),
    join(rootDir, "web", "dist")
  ];

  return candidates.find((candidate) => existsSync(candidate));
}

async function createRuntime(input: {
  rootDir: string;
  stateDir: string;
  configPath: string;
  pluginConfig: PluginConfig;
  version: string;
  agentEventProbe?: AgentEventProbe;
}) {
  const config = resolvePluginConfigWithHostDefaults(input.configPath, input.pluginConfig);
  if (!config.gateway.baseUrl) {
    throw new Error("Missing gateway.baseUrl and no local OpenClaw/QClaw gateway could be inferred");
  }
  if (!config.gateway.secret) {
    throw new Error("Missing gateway.secret and no local gateway auth token could be inferred");
  }

  const hostKind = detectHostKind(input.stateDir);
  const gatewayConfig = {
    ...config.gateway,
    hostKind,
    runtimeRoot: input.rootDir,
    exposeRawThinking: config.console.showRawThinking
  };
  const hostRuntime = readHostRuntimeInfo(input.configPath);
  const gateway = createGatewayClient(gatewayConfig);
  const server = createConsoleServer({
    stateDir: input.stateDir,
    configPath: input.configPath,
    hostKind,
    pluginVersion: input.version,
    token: randomUUID(),
    gatewayConfig,
    hub53aiConfig: config.hub53ai,
    consoleConfig: config.console,
    persistence: config.persistence,
    hostRuntime,
    gateway,
    agentEventProbe: input.agentEventProbe,
    webDir: resolveWebDir(input.rootDir)
  });

  return {
    ...server,
    sanitizedConfig: sanitizePluginConfig(config)
  };
}
