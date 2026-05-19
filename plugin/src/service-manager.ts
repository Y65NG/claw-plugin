import { spawn as nodeSpawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";

import type { PluginConfig } from "./host";
import { resolveServiceBinaryPath } from "./host";

export type LaunchSpec = {
  command: string;
  args: string[];
  env: NodeJS.ProcessEnv;
};

type BuildLaunchSpecInput = {
  rootDir: string;
  stateDir: string;
  configPath: string;
  pluginConfig?: PluginConfig;
  platform?: NodeJS.Platform;
  arch?: NodeJS.Architecture;
  token?: string;
};

type SpawnLike = (command: string, args?: readonly string[], options?: { env?: NodeJS.ProcessEnv }) => ChildProcessLike;

type ChildProcessLike = Pick<ChildProcess, "pid" | "kill" | "once">;

type ServiceManagerOptions = {
  spawn?: SpawnLike;
  restartDelayMs?: number;
};

export function buildServiceLaunchSpec(input: BuildLaunchSpecInput): LaunchSpec {
  const platform = input.platform ?? process.platform;
  const arch = input.arch ?? process.arch;
  const command = resolveServiceBinaryPath(input.rootDir, { platform, arch });

  return {
    command,
    args: [
      `--state-dir=${input.stateDir}`,
      `--config-path=${input.configPath}`,
      `--host=${input.pluginConfig?.service?.host ?? "127.0.0.1"}`,
      `--port=${input.pluginConfig?.service?.port ?? 4317}`
    ],
    env: {
      ...process.env,
      CLAW_SERVICE_PORT: String(input.pluginConfig?.service?.port ?? 4317),
      CLAW_SERVICE_HOST: input.pluginConfig?.service?.host ?? "127.0.0.1",
      CLAW_PLUGIN_TOKEN: input.token ?? "",
      RUNNER_COMMAND: input.pluginConfig?.runner?.command ?? "",
      RUNNER_ARGS_JSON: JSON.stringify(input.pluginConfig?.runner?.args ?? []),
      RUNNER_ENV_JSON: JSON.stringify(input.pluginConfig?.runner?.env ?? {}),
      MAX_EVENT_ROWS: String(input.pluginConfig?.persistence?.maxEventRowsPerSession ?? 2000),
      LOG_LEVEL: input.pluginConfig?.logging?.level ?? "info"
    }
  };
}

export function createServiceManager(options: ServiceManagerOptions = {}) {
  const spawn = options.spawn ?? ((command, args, spawnOptions) => nodeSpawn(command, args, spawnOptions));
  const restartDelayMs = options.restartDelayMs ?? 1000;

  let child: ChildProcessLike | undefined;
  let stopping = false;
  let restartTimer: NodeJS.Timeout | undefined;
  let currentSpec: LaunchSpec | undefined;

  const launch = (spec: LaunchSpec) => {
    currentSpec = spec;
    child = spawn(spec.command, spec.args, {
      env: spec.env
    });

    child.once("exit", () => {
      child = undefined;
      if (stopping || !currentSpec) {
        return;
      }
      restartTimer = setTimeout(() => {
        launch(currentSpec!);
      }, restartDelayMs);
    });
  };

  return {
    start(spec: LaunchSpec) {
      stopping = false;
      launch(spec);
    },
    stop() {
      stopping = true;
      if (restartTimer) {
        clearTimeout(restartTimer);
        restartTimer = undefined;
      }
      child?.kill?.();
      child = undefined;
    }
  };
}
