import { afterEach, describe, expect, it } from "vitest";
import { access, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parse as parseYaml } from "yaml";

import { installIntoOpenClaw, installIntoQClaw, runInstallCommand } from "../src/install-qclaw";
import type { HostDefinition } from "../src/install-qclaw";

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("QClaw installer", () => {
  it("merges plugin allow-list and config without clobbering other plugin entries", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "qclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, "openclaw.json");
    const extensionsDir = join(tempRoot, "extensions");
    const packageRoot = join(tempRoot, "package");
    await mkdir(extensionsDir, { recursive: true });
    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          plugins: {
            allow: ["browser"],
            load: { paths: ["/existing/extensions"] },
            entries: {
              browser: { enabled: true }
            }
          }
        },
        null,
        2
      )
    );

    await installIntoQClaw({
      packageRoot,
      extensionsDir,
      configPath,
      gateway: "ws://gateway.example.com:28789",
      botId: "bot-123",
      secret: "sk-secret",
      preferResponsesApi: true,
      gatewayModel: "openai/gpt-5.5",
      consolePort: 4321
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
        entries: Record<string, { enabled: boolean; config?: Record<string, unknown> }>;
      };
    };

    expect(updated.plugins.allow).toContain("claw-control-center");
    expect(updated.plugins.allow).toContain("browser");
    expect(updated.plugins.load.paths).toContain(extensionsDir);
    expect(updated.plugins.entries.browser.enabled).toBe(true);
    expect(updated.plugins.entries["claw-control-center"]).toEqual({
      enabled: true,
      config: {
        gateway: {
          baseUrl: "ws://gateway.example.com:28789",
          botId: "bot-123",
          secret: "sk-secret",
          preferResponsesApi: true,
          modelOverride: "openai/gpt-5.5"
        },
        console: {
          port: 4321
        }
      }
    });
    expect((updated as any).gateway.http.endpoints.responses.enabled).toBe(true);
  });

  it("infers the local gateway settings from openclaw.json when flags are omitted", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "qclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, "openclaw.json");
    const extensionsDir = join(tempRoot, "extensions");
    const packageRoot = join(tempRoot, "package");
    await mkdir(extensionsDir, { recursive: true });
    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          gateway: {
            host: "127.0.0.1",
            port: 28789,
            auth: {
              mode: "token",
              token: "local-token"
            }
          },
          plugins: {
            entries: {
              "claw-control-center": {
                enabled: false,
                config: {
                  gateway: {
                    baseUrl: "ws://127.0.0.1:49711",
                    secret: "stale-token"
                  },
                  console: {
                    host: "127.0.0.1"
                  }
                }
              }
            }
          }
        },
        null,
        2
      )
    );

    const result = await installIntoQClaw({
      packageRoot,
      extensionsDir,
      configPath
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        entries: Record<string, { enabled: boolean; config?: Record<string, unknown> }>;
      };
    };

    expect(result.gatewayBaseUrl).toBe("ws://127.0.0.1:28789");
    expect(updated.plugins.entries["claw-control-center"]).toEqual({
      enabled: true,
      config: {
        gateway: {
          preferResponsesApi: false
        },
        console: {
          host: "127.0.0.1"
        }
      }
    });
    expect((updated as any).gateway.http?.endpoints?.responses).toBeUndefined();
  });

  it("replaces stale web assets when reinstalling over an existing plugin directory", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "qclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, "openclaw.json");
    const extensionsDir = join(tempRoot, "extensions");
    const packageRoot = join(tempRoot, "package");
    const stalePluginDir = join(extensionsDir, "claw-control-center");
    const staleAssetPath = join(stalePluginDir, "web-dist", "assets", "old-bundle.js");

    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await mkdir(join(packageRoot, "web-dist", "assets"), { recursive: true });
    await mkdir(join(stalePluginDir, "web-dist", "assets"), { recursive: true });
    await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
    await writeFile(join(packageRoot, "web-dist", "index.html"), "<html>fresh</html>\n");
    await writeFile(join(packageRoot, "web-dist", "assets", "new-bundle.js"), "console.log('fresh');\n");
    await writeFile(staleAssetPath, "console.log('stale');\n");
    await writeFile(
      configPath,
      JSON.stringify({
        gateway: {
          host: "127.0.0.1",
          port: 28789,
          auth: {
            mode: "token",
            token: "local-token"
          }
        }
      })
    );

    await installIntoQClaw({
      packageRoot,
      extensionsDir,
      configPath
    });

    await expect(access(staleAssetPath)).rejects.toThrow();
    expect(await readFile(join(stalePluginDir, "web-dist", "index.html"), "utf8")).toContain("fresh");
    expect(await readFile(join(stalePluginDir, "web-dist", "assets", "new-bundle.js"), "utf8")).toContain("fresh");
  });

  it("writes explicit 53AIHub bridge config without mixing it into the local gateway config", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "qclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, "openclaw.json");
    const extensionsDir = join(tempRoot, "extensions");
    const packageRoot = join(tempRoot, "package");
    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
    await writeFile(
      configPath,
      JSON.stringify({
        gateway: {
          host: "127.0.0.1",
          port: 28789,
          auth: {
            mode: "token",
            token: "local-token"
          }
        }
      })
    );

    const result = await installIntoQClaw({
      packageRoot,
      extensionsDir,
      configPath,
      hubWsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
      hubBotId: "hub-bot",
      hubSecret: "hub-secret"
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        entries: Record<string, { enabled: boolean; config?: Record<string, any> }>;
      };
    };

    expect(result.hub53aiConfigured).toBe(true);
    expect(updated.plugins.entries["claw-control-center"].config?.gateway).toEqual({
      preferResponsesApi: false
    });
    expect(updated.plugins.entries["claw-control-center"].config?.hub53ai).toEqual({
      enabled: true,
      botId: "hub-bot",
      secret: "hub-secret",
      wsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
      accessPolicy: "open",
      allowFrom: [],
      sendThinkingMessage: true
    });
  });

  it("migrates legacy channels.53aihub config into the new bridge config", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "qclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, "openclaw.json");
    const extensionsDir = join(tempRoot, "extensions");
    const packageRoot = join(tempRoot, "package");
    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          gateway: {
            host: "127.0.0.1",
            port: 28789,
            auth: {
              mode: "token",
              token: "local-token"
            }
          },
          channels: {
            "53aihub": {
              enabled: true,
              botId: "legacy-bot",
              secret: "legacy-secret",
              WSUrl: "wss://legacy.example.com/api/v1/openclaw/ws/connect",
              accessPolicy: "allowlist",
              allowFrom: ["user-a"],
              sendThinkingMessage: false
            }
          }
        },
        null,
        2
      )
    );

    await installIntoQClaw({
      packageRoot,
      extensionsDir,
      configPath
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      channels: Record<string, { botId: string }>;
      plugins: {
        entries: Record<string, { enabled: boolean; config?: Record<string, any> }>;
      };
    };

    expect(updated.plugins.entries["claw-control-center"].config?.hub53ai).toEqual({
      enabled: true,
      botId: "legacy-bot",
      secret: "legacy-secret",
      wsUrl: "wss://legacy.example.com/api/v1/openclaw/ws/connect",
      accessPolicy: "allowlist",
      allowFrom: ["user-a"],
      sendThinkingMessage: false
    });
    expect(updated.channels["53aihub"].botId).toBe("legacy-bot");
    expect((updated as any).gateway.http?.endpoints?.responses).toBeUndefined();
  });

  it("keeps the legacy 53ai-openclaw plugin disabled when installing into OpenClaw", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, ".openclaw", "openclaw.json");
    const extensionsDir = join(tempRoot, ".openclaw", "extensions");
    const packageRoot = join(tempRoot, "package");
    await mkdir(join(tempRoot, ".openclaw"), { recursive: true });
    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "claw-control-center" }));
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = { build: 'fresh' };\n");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          gateway: {
            host: "127.0.0.1",
            port: 18789,
            auth: {
              mode: "token",
              token: "local-token"
            }
          },
          channels: {
            "53aihub": {
              enabled: true,
              botId: "legacy-bot",
              secret: "legacy-secret",
              WSUrl: "wss://legacy.example.com/api/v1/openclaw/ws/connect"
            }
          },
          plugins: {
            allow: ["53ai-openclaw"],
            entries: {
              "53ai-openclaw": {
                enabled: false,
                config: {
                  botId: "legacy-bot",
                  secret: "legacy-secret",
                  WSUrl: "wss://legacy.example.com/api/v1/openclaw/ws/connect"
                }
              }
            }
          }
        },
        null,
        2
      )
    );

    const result = await installIntoOpenClaw({
      packageRoot,
      extensionsDir,
      configPath
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        entries: Record<string, { enabled: boolean; config?: Record<string, any> }>;
      };
    };

    expect(result.destination).toBe(join(extensionsDir, "claw-control-center"));
    expect(updated.plugins.allow).toEqual(["claw-control-center"]);
    expect(updated.plugins.entries["53ai-openclaw"].enabled).toBe(false);
    expect(updated.plugins.entries["claw-control-center"].config?.hub53ai).toMatchObject({
      enabled: true,
      botId: "legacy-bot",
      secret: "legacy-secret",
      wsUrl: "wss://legacy.example.com/api/v1/openclaw/ws/connect"
    });
    expect(await readFile(join(extensionsDir, "claw-control-center", "dist", "index.cjs"), "utf8")).toContain(
      "fresh"
    );
  });

  it("installs only into the explicit config and extension paths provided by the Claw host", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, ".openclaw", "openclaw.json");
    const extensionsDir = join(tempRoot, ".openclaw", "extensions");
    const packageRoot = join(tempRoot, "package");
    await mkdir(join(tempRoot, ".openclaw"), { recursive: true });
    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          gateway: {
            host: "127.0.0.1",
            port: 18789,
            auth: {
              mode: "password",
              password: "openclaw-password"
            }
          }
        },
        null,
        2
      )
    );

    const chunks: string[] = [];
    const originalWrite = process.stdout.write;
    process.stdout.write = ((chunk: string | Uint8Array) => {
      chunks.push(String(chunk));
      return true;
    }) as typeof process.stdout.write;

    try {
      await runInstallCommand({
        packageRoot,
        argv: [
          "install",
          "--config-path",
          configPath,
          "--extensions-dir",
          extensionsDir,
          "--hub-ws-url",
          "wss://hub.example.com/api/v1/openclaw/ws/connect",
          "--hub-bot-id",
          "hub-bot",
          "--hub-secret",
          "hub-secret"
        ]
      });
    } finally {
      process.stdout.write = originalWrite;
    }

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
        entries: Record<string, { enabled: boolean; config?: Record<string, unknown> }>;
      };
    };

    expect(updated.plugins.allow).toContain("claw-control-center");
    expect(updated.plugins.load.paths).toContain(extensionsDir);
    expect(updated.plugins.entries["claw-control-center"]).toEqual({
      enabled: true,
      config: {
        gateway: {
          preferResponsesApi: false
        },
        hub53ai: {
          enabled: true,
          botId: "hub-bot",
          secret: "hub-secret",
          wsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
          accessPolicy: "open",
          allowFrom: [],
          sendThinkingMessage: true
        }
      }
    });
    await access(join(extensionsDir, "claw-control-center", "dist", "index.cjs"));
    expect(chunks.join("")).toContain("Installed claw-control-center into Claw.");
    expect(chunks.join("")).toContain("Plugin build:");
    expect(chunks.join("")).toContain("Restart Claw to load the plugin.");
  });

  it("auto-detects a single QClaw host when explicit paths are omitted", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, ".qclaw", "openclaw.json");
    const extensionsDir = join(tempRoot, "Library/Application Support/QClaw/openclaw/config/extensions");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(join(tempRoot, ".qclaw"), { recursive: true });
    await writeGatewayConfig(configPath, 28789, "qclaw-token");

    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: ["install"],
        hostDefinitions: [
          {
            id: "qclaw",
            label: "QClaw",
            configPath,
            extensionsDir
          },
          {
            id: "openclaw",
            label: "OpenClaw",
            configPath: join(tempRoot, ".openclaw", "openclaw.json"),
            extensionsDir: join(tempRoot, ".openclaw", "extensions")
          }
        ]
      });
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };

    expect(updated.plugins.allow).toContain("claw-control-center");
    expect(updated.plugins.load.paths).toContain(extensionsDir);
    expect(chunks.join("")).toContain("Installed claw-control-center into QClaw.");
    expect(chunks.join("")).toContain("Restart QClaw to load the plugin.");
  });

  it("auto-detects a single OpenClaw host when explicit paths are omitted", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, ".openclaw", "openclaw.json");
    const extensionsDir = join(tempRoot, ".openclaw", "extensions");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(join(tempRoot, ".openclaw"), { recursive: true });
    await writeGatewayConfig(configPath, 18789, "openclaw-token");

    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: ["install"],
        hostDefinitions: [
          {
            id: "qclaw",
            label: "QClaw",
            configPath: join(tempRoot, ".qclaw", "openclaw.json"),
            extensionsDir: join(tempRoot, "Library/Application Support/QClaw/openclaw/config/extensions")
          },
          {
            id: "openclaw",
            label: "OpenClaw",
            configPath,
            extensionsDir
          }
        ]
      });
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };

    expect(updated.plugins.allow).toContain("claw-control-center");
    expect(updated.plugins.load.paths).toContain(extensionsDir);
    expect(chunks.join("")).toContain("Installed claw-control-center into OpenClaw.");
    expect(chunks.join("")).toContain("Restart OpenClaw to load the plugin.");
  });

  it("uses an interactive selection callback when multiple Claw hosts are detected", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const qclawConfigPath = join(tempRoot, ".qclaw", "openclaw.json");
    const qclawExtensionsDir = join(tempRoot, "Library/Application Support/QClaw/openclaw/config/extensions");
    const openClawConfigPath = join(tempRoot, ".openclaw", "openclaw.json");
    const openClawExtensionsDir = join(tempRoot, ".openclaw", "extensions");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(join(tempRoot, ".qclaw"), { recursive: true });
    await mkdir(join(tempRoot, ".openclaw"), { recursive: true });
    await writeGatewayConfig(qclawConfigPath, 28789, "qclaw-token");
    await writeGatewayConfig(openClawConfigPath, 18789, "openclaw-token");

    const hosts: HostDefinition[] = [
      {
        id: "qclaw",
        label: "QClaw",
        configPath: qclawConfigPath,
        extensionsDir: qclawExtensionsDir
      },
      {
        id: "openclaw",
        label: "OpenClaw",
        configPath: openClawConfigPath,
        extensionsDir: openClawExtensionsDir
      }
    ];

    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: ["install"],
        hostDefinitions: hosts,
        selectHost: async (detected) => {
          expect(detected).toEqual(hosts);
          return detected[1]!;
        }
      });
    });

    const openClawConfig = JSON.parse(await readFile(openClawConfigPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };
    const qclawConfig = JSON.parse(await readFile(qclawConfigPath, "utf8")) as {
      plugins?: {
        allow?: string[];
      };
    };

    expect(openClawConfig.plugins.allow).toContain("claw-control-center");
    expect(openClawConfig.plugins.load.paths).toContain(openClawExtensionsDir);
    expect(qclawConfig.plugins?.allow).toBeUndefined();
    expect(chunks.join("")).toContain("Installed claw-control-center into OpenClaw.");
  });

  it("installs into multiple selected Claw hosts when several compatible hosts are detected", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const qclawConfigPath = join(tempRoot, ".qclaw", "openclaw.json");
    const qclawExtensionsDir = join(tempRoot, "Library/Application Support/QClaw/openclaw/config/extensions");
    const openClawConfigPath = join(tempRoot, ".openclaw", "openclaw.json");
    const openClawExtensionsDir = join(tempRoot, ".openclaw", "extensions");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(join(tempRoot, ".qclaw"), { recursive: true });
    await mkdir(join(tempRoot, ".openclaw"), { recursive: true });
    await writeGatewayConfig(qclawConfigPath, 28789, "qclaw-token");
    await writeGatewayConfig(openClawConfigPath, 18789, "openclaw-token");

    const hosts: HostDefinition[] = [
      {
        id: "qclaw",
        label: "QClaw",
        configPath: qclawConfigPath,
        extensionsDir: qclawExtensionsDir
      },
      {
        id: "openclaw",
        label: "OpenClaw",
        configPath: openClawConfigPath,
        extensionsDir: openClawExtensionsDir
      }
    ];

    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: ["install"],
        hostDefinitions: hosts,
        selectHosts: async (detected) => {
          expect(detected).toEqual(hosts);
          return detected;
        }
      });
    });

    const openClawConfig = JSON.parse(await readFile(openClawConfigPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };
    const qclawConfig = JSON.parse(await readFile(qclawConfigPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };

    expect(openClawConfig.plugins.allow).toContain("claw-control-center");
    expect(openClawConfig.plugins.load.paths).toContain(openClawExtensionsDir);
    expect(qclawConfig.plugins.allow).toContain("claw-control-center");
    expect(qclawConfig.plugins.load.paths).toContain(qclawExtensionsDir);
    expect(chunks.join("")).toContain("Installed claw-control-center into QClaw.");
    expect(chunks.join("")).toContain("Installed claw-control-center into OpenClaw.");
  });

  it("auto-detects a single Hermes host and installs the native platform plugin", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const hermesConfigPath = join(tempRoot, ".hermes", "config.yaml");
    const hermesPluginsDir = join(tempRoot, ".hermes", "plugins", "platforms");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(join(tempRoot, ".hermes"), { recursive: true });
    await writeFile(
      hermesConfigPath,
      [
        "plugins:",
        "  enabled:",
        "    - observability/langfuse",
        "platforms:",
        "  telegram:",
        "    enabled: true",
        ""
      ].join("\n")
    );
    await writeFile(join(tempRoot, ".hermes", ".env"), "EXISTING=value\nHUB53AI_SECRET=\"old-secret\"\n");

    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: [
          "install",
          "--hub-ws-url",
          "wss://hub.example.com/api/v1/openclaw/ws/connect",
          "--hub-bot-id",
          "hub-bot",
          "--hub-secret",
          "hub-secret"
        ],
        hostDefinitions: [
          {
            id: "hermes",
            label: "Hermes",
            configPath: hermesConfigPath,
            extensionsDir: hermesPluginsDir,
            installKind: "hermes"
          }
        ]
      });
    });

    const updatedConfig = parseYaml(await readFile(hermesConfigPath, "utf8")) as any;
    const updatedEnv = await readFile(join(tempRoot, ".hermes", ".env"), "utf8");

    expect(updatedConfig.plugins.enabled).toContain("observability/langfuse");
    expect(updatedConfig.plugins.enabled).toContain("platforms/53aihub");
    expect(updatedConfig.plugins.enabled).toContain("53aihub");
    expect(updatedConfig.platforms.telegram.enabled).toBe(true);
    expect(updatedConfig.platforms["53aihub"]).toEqual({ enabled: true, extra: {} });
    expect(updatedEnv).toContain("EXISTING=value");
    expect(updatedEnv).toContain('HUB53AI_BOT_ID="hub-bot"');
    expect(updatedEnv).toContain('HUB53AI_SECRET="hub-secret"');
    expect(updatedEnv).toContain('HUB53AI_WS_URL="wss://hub.example.com/api/v1/openclaw/ws/connect"');
    await access(join(hermesPluginsDir, "53aihub", "plugin.yaml"));
    await access(join(hermesPluginsDir, "53aihub", "adapter.py"));
    expect(chunks.join("")).toContain("Installed claw-control-center into Hermes.");
    expect(chunks.join("")).not.toContain("Gateway:");
  });

  it("installs into both Hermes and OpenClaw when both are selected", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const hermesConfigPath = join(tempRoot, ".hermes", "config.yaml");
    const hermesPluginsDir = join(tempRoot, ".hermes", "plugins", "platforms");
    const openClawConfigPath = join(tempRoot, ".openclaw", "openclaw.json");
    const openClawExtensionsDir = join(tempRoot, ".openclaw", "extensions");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(join(tempRoot, ".hermes"), { recursive: true });
    await mkdir(join(tempRoot, ".openclaw"), { recursive: true });
    await writeFile(hermesConfigPath, "plugins:\n  enabled: []\n");
    await writeGatewayConfig(openClawConfigPath, 18789, "openclaw-token");

    const hosts: HostDefinition[] = [
      {
        id: "hermes",
        label: "Hermes",
        configPath: hermesConfigPath,
        extensionsDir: hermesPluginsDir,
        installKind: "hermes"
      },
      {
        id: "openclaw",
        label: "OpenClaw",
        configPath: openClawConfigPath,
        extensionsDir: openClawExtensionsDir
      }
    ];

    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: [
          "install",
          "--hub-ws-url",
          "wss://hub.example.com/api/v1/openclaw/ws/connect",
          "--hub-bot-id",
          "hub-bot",
          "--hub-secret",
          "hub-secret"
        ],
        hostDefinitions: hosts,
        selectHosts: async (detected) => {
          expect(detected).toEqual(hosts);
          return detected;
        }
      });
    });

    const hermesConfig = parseYaml(await readFile(hermesConfigPath, "utf8")) as any;
    const openClawConfig = JSON.parse(await readFile(openClawConfigPath, "utf8")) as any;

    expect(hermesConfig.plugins.enabled).toContain("platforms/53aihub");
    expect(openClawConfig.plugins.allow).toContain("claw-control-center");
    await access(join(hermesPluginsDir, "53aihub", "adapter.py"));
    await access(join(openClawExtensionsDir, "claw-control-center", "dist", "index.cjs"));
    expect(chunks.join("")).toContain("Installed claw-control-center into Hermes.");
    expect(chunks.join("")).toContain("Installed claw-control-center into OpenClaw.");
  });

  it("rejects multiple detected hosts when no interactive terminal is available", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const qclawConfigPath = join(tempRoot, ".qclaw", "openclaw.json");
    const openClawConfigPath = join(tempRoot, ".openclaw", "openclaw.json");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(join(tempRoot, ".qclaw"), { recursive: true });
    await mkdir(join(tempRoot, ".openclaw"), { recursive: true });
    await writeGatewayConfig(qclawConfigPath, 28789, "qclaw-token");
    await writeGatewayConfig(openClawConfigPath, 18789, "openclaw-token");

    await expect(
      runInstallCommand({
        packageRoot,
        argv: ["install"],
        hostDefinitions: [
          {
            id: "qclaw",
            label: "QClaw",
            configPath: qclawConfigPath,
            extensionsDir: join(tempRoot, "Library/Application Support/QClaw/openclaw/config/extensions")
          },
          {
            id: "openclaw",
            label: "OpenClaw",
            configPath: openClawConfigPath,
            extensionsDir: join(tempRoot, ".openclaw", "extensions")
          }
        ],
        ttyPath: join(tempRoot, "missing-tty")
      })
    ).rejects.toThrow("multiple Claw installations were detected, but no interactive terminal was available");
  });

  it("rejects installs when no Claw host can be auto-detected", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const packageRoot = await createPackageRoot(tempRoot);

    await expect(
      runInstallCommand({
        packageRoot,
        argv: ["install"],
        hostDefinitions: [
          {
            id: "qclaw",
            label: "QClaw",
            configPath: join(tempRoot, ".qclaw", "openclaw.json"),
            extensionsDir: join(tempRoot, "Library/Application Support/QClaw/openclaw/config/extensions")
          },
          {
            id: "openclaw",
            label: "OpenClaw",
            configPath: join(tempRoot, ".openclaw", "openclaw.json"),
            extensionsDir: join(tempRoot, ".openclaw", "extensions")
          }
        ]
      })
    ).rejects.toThrow("could not auto-detect an installed Claw host");
  });

  it("rejects the removed target option with a clear error", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-"));
    cleanupPaths.push(tempRoot);

    const packageRoot = await createPackageRoot(tempRoot);

    await expect(
      runInstallCommand({
        packageRoot,
        argv: ["install", "--target", "qclaw"]
      })
    ).rejects.toThrow("--target has been removed");
  });
});

async function createPackageRoot(tempRoot: string): Promise<string> {
  const packageRoot = join(tempRoot, "package");
  await mkdir(join(packageRoot, "dist"), { recursive: true });
  await mkdir(join(packageRoot, "hermes", "platforms", "53aihub"), { recursive: true });
  await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
  await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
  await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
  await writeFile(join(packageRoot, "hermes", "platforms", "53aihub", "plugin.yaml"), "name: 53aihub\nversion: 0.1.0\nkind: platform\n");
  await writeFile(join(packageRoot, "hermes", "platforms", "53aihub", "__init__.py"), "from .adapter import register\n");
  await writeFile(join(packageRoot, "hermes", "platforms", "53aihub", "adapter.py"), "def register(ctx):\n    pass\n");
  return packageRoot;
}

async function writeGatewayConfig(configPath: string, port: number, token: string): Promise<void> {
  await writeFile(
    configPath,
    JSON.stringify(
      {
        gateway: {
          host: "127.0.0.1",
          port,
          auth: {
            mode: "token",
            token
          }
        }
      },
      null,
      2
    )
  );
}

async function withCapturedStdout(chunks: string[], run: () => Promise<void>): Promise<void> {
  const originalWrite = process.stdout.write;
  process.stdout.write = ((chunk: string | Uint8Array) => {
    chunks.push(String(chunk));
    return true;
  }) as typeof process.stdout.write;
  try {
    await run();
  } finally {
    process.stdout.write = originalWrite;
  }
}
