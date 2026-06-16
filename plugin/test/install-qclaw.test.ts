import { afterEach, describe, expect, it, vi } from "vitest";
import { access, chmod, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { parse as parseYaml } from "yaml";

import {
  getQClawExtensionsDirCandidates,
  installIntoCodex,
  installIntoOpenClaw,
  installIntoQClaw,
  installIntoWorkBuddy,
  runInstallCommand
} from "../src/install-qclaw";
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

  it("removes runtime dependency declarations from the copied extension package", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "qclaw-install-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, "openclaw.json");
    const extensionsDir = join(tempRoot, "extensions");
    const packageRoot = join(tempRoot, "package");
    await mkdir(join(packageRoot, "dist"), { recursive: true });
    await writeFile(
      join(packageRoot, "package.json"),
      JSON.stringify({
        name: "claw-control-center",
        version: "0.1.9",
        dependencies: {
          ws: "^8.18.3",
          yaml: "^2.9.0"
        },
        optionalDependencies: {
          bufferutil: "^4.0.0"
        },
        devDependencies: {
          vitest: "^3.2.4"
        }
      })
    );
    await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
    await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
    await writeGatewayConfig(configPath, 28789, "local-token");

    await installIntoOpenClaw({
      packageRoot,
      extensionsDir,
      configPath
    });

    const copiedPackage = JSON.parse(
      await readFile(join(extensionsDir, "claw-control-center", "package.json"), "utf8")
    ) as Record<string, unknown>;

    expect(copiedPackage.dependencies).toBeUndefined();
    expect(copiedPackage.optionalDependencies).toBeUndefined();
    expect(copiedPackage.devDependencies).toEqual({ vitest: "^3.2.4" });
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
      sendThinkingMessage: true,
      detectCreatedFiles: true,
      fileWorkspaceDirs: [],
      createdFilesMaxFileBytes: 10 * 1024 * 1024,
      createdFilesMaxCount: 20,
      createdFilesExclude: []
    });
  });

  it("preserves existing plugin 53AIHub config and adds created-file detection defaults", async () => {
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
          plugins: {
            entries: {
              "claw-control-center": {
                enabled: true,
                config: {
                  hub53ai: {
                    enabled: true,
                    botId: "existing-bot",
                    secret: "existing-secret",
                    wsUrl: "wss://existing.example.com/api/v1/openclaw/ws/connect",
                    accessPolicy: "allowlist",
                    allowFrom: ["user-a"],
                    sendThinkingMessage: false
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
        entries: Record<string, { enabled: boolean; config?: Record<string, any> }>;
      };
    };

    expect(result.hub53aiConfigured).toBe(true);
    expect(updated.plugins.entries["claw-control-center"].config?.hub53ai).toEqual({
      enabled: true,
      botId: "existing-bot",
      secret: "existing-secret",
      wsUrl: "wss://existing.example.com/api/v1/openclaw/ws/connect",
      accessPolicy: "allowlist",
      allowFrom: ["user-a"],
      sendThinkingMessage: false,
      detectCreatedFiles: true,
      fileWorkspaceDirs: [],
      createdFilesMaxFileBytes: 10 * 1024 * 1024,
      createdFilesMaxCount: 20,
      createdFilesExclude: []
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
      sendThinkingMessage: false,
      detectCreatedFiles: true,
      fileWorkspaceDirs: [],
      createdFilesMaxFileBytes: 10 * 1024 * 1024,
      createdFilesMaxCount: 20,
      createdFilesExclude: []
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
          sendThinkingMessage: true,
          detectCreatedFiles: true,
          fileWorkspaceDirs: [],
          createdFilesMaxFileBytes: 10 * 1024 * 1024,
          createdFilesMaxCount: 20,
          createdFilesExclude: []
        }
      }
    });
    await access(join(extensionsDir, "claw-control-center", "dist", "index.cjs"));
    expect(chunks.join("")).toContain("Installed claw-control-center into OpenClaw.");
    expect(chunks.join("")).toContain("Plugin build:");
    expect(chunks.join("")).toContain("Restart OpenClaw to load the plugin.");
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
    let promptCalled = false;
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
        ],
        promptSelectHost: async (detected, incompatible) => {
          promptCalled = true;
          expect(detected).toHaveLength(1);
          expect(detected[0]?.id).toBe("qclaw");
          expect(incompatible).toEqual([]);
          return detected[0]!;
        }
      });
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };

    expect(promptCalled).toBe(true);
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
    let promptCalled = false;
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
        ],
        promptSelectHost: async (detected, incompatible) => {
          promptCalled = true;
          expect(detected).toHaveLength(1);
          expect(detected[0]?.id).toBe("openclaw");
          expect(incompatible).toEqual([]);
          return detected[0]!;
        }
      });
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };

    expect(promptCalled).toBe(true);
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

  it("uses --host-kind to select QClaw without prompting when multiple hosts exist", async () => {
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

    let promptCalled = false;
    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: ["install", "--host-kind", "qclaw"],
        hostDefinitions: [
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
        ],
        promptSelectHost: async (detected) => {
          promptCalled = true;
          return detected[0]!;
        }
      });
    });

    const qclawConfig = JSON.parse(await readFile(qclawConfigPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };
    const openClawConfig = JSON.parse(await readFile(openClawConfigPath, "utf8")) as {
      plugins?: {
        allow?: string[];
      };
    };

    expect(promptCalled).toBe(false);
    expect(qclawConfig.plugins.allow).toContain("claw-control-center");
    expect(qclawConfig.plugins.load.paths).toContain(qclawExtensionsDir);
    expect(openClawConfig.plugins?.allow).toBeUndefined();
    expect(chunks.join("")).toContain("Installed claw-control-center into QClaw.");
  });

  it("uses platform-specific QClaw extension directory candidates", () => {
    const home = join(tmpdir(), "qclaw-home");

    expect(getQClawExtensionsDirCandidates({ platform: "darwin", homeDir: home })[0]).toBe(
      join(home, "Library", "Application Support", "QClaw", "openclaw", "config", "extensions")
    );
    const windowsCandidate = getQClawExtensionsDirCandidates({
      platform: "win32",
      homeDir: home,
      env: {
        APPDATA: "C:\\Users\\tester\\AppData\\Roaming",
        LOCALAPPDATA: "C:\\Users\\tester\\AppData\\Local"
      }
    })[0]!.replace(/\\/g, "/");
    expect(windowsCandidate).toContain("AppData/Roaming/QClaw/openclaw/config/extensions");
    expect(
      getQClawExtensionsDirCandidates({
        platform: "linux",
        homeDir: home,
        env: {
          XDG_CONFIG_HOME: join(home, ".config")
        }
      })[0]
    ).toBe(join(home, ".config", "QClaw", "openclaw", "config", "extensions"));
  });

  it("uses the cross-platform keyboard prompt to select one Claw host when multiple hosts are detected", async () => {
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
        promptSelectHost: async (detected: HostDefinition[], incompatible: HostDefinition[]) => {
          expect(detected).toEqual(hosts);
          expect(incompatible).toEqual([]);
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

  it("rejects multiple selected compatible agents when several hosts are detected", async () => {
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

    await expect(
      runInstallCommand({
        packageRoot,
        argv: ["install"],
        hostDefinitions: hosts,
        selectHosts: async (detected) => {
          expect(detected).toEqual(hosts);
          return detected;
        }
      })
    ).rejects.toThrow("select exactly one compatible agent");
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
        "display:",
        "  platforms:",
        "    telegram:",
        "      show_reasoning: false",
        ""
      ].join("\n")
    );
    await writeFile(join(tempRoot, ".hermes", ".env"), "EXISTING=value\nHUB53AI_SECRET=\"old-secret\"\n");

    const chunks: string[] = [];
    let promptCalled = false;
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
        ],
        promptSelectHost: async (detected, incompatible) => {
          promptCalled = true;
          expect(detected).toHaveLength(1);
          expect(detected[0]?.id).toBe("hermes");
          expect(incompatible).toEqual([]);
          return detected[0]!;
        }
      });
    });

    const updatedConfig = parseYaml(await readFile(hermesConfigPath, "utf8")) as any;
    const updatedEnv = await readFile(join(tempRoot, ".hermes", ".env"), "utf8");

    expect(promptCalled).toBe(true);
    expect(updatedConfig.plugins.enabled).toContain("observability/langfuse");
    expect(updatedConfig.plugins.enabled).toContain("platforms/53aihub");
    expect(updatedConfig.plugins.enabled).toContain("53aihub");
    expect(updatedConfig.platforms.telegram.enabled).toBe(true);
    expect(updatedConfig.platforms["53aihub"]).toEqual({ enabled: true, extra: {} });
    expect(updatedConfig.display.platforms.telegram.show_reasoning).toBe(false);
    expect(updatedConfig.display.platforms["53aihub"].show_reasoning).toBe(true);
    expect(updatedEnv).toContain("EXISTING=value");
    expect(updatedEnv).toContain('HUB53AI_BOT_ID="hub-bot"');
    expect(updatedEnv).toContain('HUB53AI_SECRET="hub-secret"');
    expect(updatedEnv).toContain('HUB53AI_WS_URL="wss://hub.example.com/api/v1/openclaw/ws/connect"');
    expect(updatedEnv).not.toContain("GATEWAY_ALLOW_ALL_USERS");
    expect(updatedEnv).not.toContain("HUB53AI_HOME_CHANNEL");
    await access(join(hermesPluginsDir, "53aihub", "plugin.yaml"));
    await access(join(hermesPluginsDir, "53aihub", "adapter.py"));
    expect(chunks.join("")).toContain("Installed claw-control-center into Hermes.");
    expect(chunks.join("")).not.toContain("Gateway:");
  });

  it("installs only into the selected host when Hermes and OpenClaw are both detected", async () => {
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
        promptSelectHost: async (detected) => {
          expect(detected).toEqual(hosts);
          return detected[0]!;
        }
      });
    });

    const hermesConfig = parseYaml(await readFile(hermesConfigPath, "utf8")) as any;

    expect(hermesConfig.plugins.enabled).toContain("platforms/53aihub");
    expect(hermesConfig.display.platforms["53aihub"].show_reasoning).toBe(true);
    await access(join(hermesPluginsDir, "53aihub", "adapter.py"));
    expect(chunks.join("")).toContain("Installed claw-control-center into Hermes.");
    expect(chunks.join("")).not.toContain("Installed claw-control-center into OpenClaw.");
    await expect(access(join(openClawExtensionsDir, "claw-control-center", "dist", "index.cjs"))).rejects.toThrow();
  });

  it("installs the 53AIHub channel plugin into WorkBuddy local marketplace", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-install-"));
    cleanupPaths.push(tempRoot);

    const packageRoot = await createPackageRoot(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    let cleanedTargets: string[] | undefined;

    const result = await installIntoWorkBuddy({
      packageRoot,
      workbuddyHome,
      hubWsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
      hubBotId: "hub-bot",
      hubSecret: "hub-secret",
      cleanupWorkBuddyChannelProcesses: async (targets) => {
        cleanedTargets = targets;
      }
    });

    const destination = join(
      workbuddyHome,
      "plugins",
      "marketplaces",
      "my-experts",
      "plugins",
      "53aihub-workbuddy"
    );
    const mcpConfig = JSON.parse(await readFile(join(destination, ".mcp.json"), "utf8")) as any;
    const marketplace = JSON.parse(
      await readFile(join(workbuddyHome, "plugins", "marketplaces", "my-experts", ".codebuddy-plugin", "marketplace.json"), "utf8")
    ) as any;
    const knownMarketplaces = JSON.parse(
      await readFile(join(workbuddyHome, "plugins", "known_marketplaces.json"), "utf8")
    ) as any;
    const settings = JSON.parse(await readFile(join(workbuddyHome, "settings.json"), "utf8")) as any;

    expect(result.destination).toBe(destination);
    expect(result.marketplacePath).toBe(
      join(workbuddyHome, "plugins", "marketplaces", "my-experts", ".codebuddy-plugin", "marketplace.json")
    );
    expect(mcpConfig.mcpServers["53aihub-channel"].env).toMatchObject({
      HUB53AI_WS_URL: "wss://hub.example.com/api/v1/openclaw/ws/connect",
      HUB53AI_BOT_ID: "hub-bot",
      HUB53AI_SECRET: "hub-secret",
      HUB53AI_ACCESS_POLICY: "open",
      HUB53AI_SEND_THINKING_MESSAGE: "true",
      HUB53AI_CHANNEL_ENTRY_PATH: join(destination, "dist", "codebuddy-channel.cjs"),
      HUB53AI_WORKBUDDY_HOME: workbuddyHome,
      HUB53AI_WORKBUDDY_WORKSPACE: join(workbuddyHome, "channels", "53aihub-workspace"),
      HUB53AI_WORKBUDDY_HISTORY_SCOPE: "all",
      HUB53AI_WORKBUDDY_SESSION_ID: "53aihub-workbuddy-shared"
    });
    expect(mcpConfig.mcpServers["53aihub-channel"].args).toEqual([
      join(destination, "dist", "workbuddy-supervisor.cjs")
    ]);
    expect(cleanedTargets).toEqual(
      expect.arrayContaining([
        join(destination, "dist", "codebuddy-channel.cjs"),
        join(destination, "dist", "workbuddy-supervisor.cjs"),
        "--session-id 53aihub-workbuddy-shared"
      ])
    );
    expect(marketplace.plugins).toContainEqual(
      expect.objectContaining({
        name: "53aihub-workbuddy",
        source: "./plugins/53aihub-workbuddy"
      })
    );
    expect(knownMarketplaces["my-experts"]).toMatchObject({
      type: "directory",
      source: {
        source: "directory",
        path: join(workbuddyHome, "plugins", "marketplaces", "my-experts")
      },
      installLocation: join(workbuddyHome, "plugins", "marketplaces", "my-experts"),
      isBuiltIn: false,
      autoUpdate: false
    });
    expect(settings.enabledPlugins).toMatchObject({
      "53aihub-workbuddy@my-experts": true
    });
    expect(settings.channelsEnabled).toBe(true);
    await access(join(destination, ".codebuddy-plugin", "plugin.json"));
    await access(join(destination, "dist", "codebuddy-channel.cjs"));
    await access(join(destination, "dist", "workbuddy-supervisor.cjs"));
    await access(join(workbuddyHome, "channels", "53aihub-workspace"));
  });

  it("supports install-workbuddy as an explicit installer subcommand", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-command-"));
    cleanupPaths.push(tempRoot);

    const packageRoot = await createPackageRoot(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: [
          "install-workbuddy",
          "--workbuddy-home",
          workbuddyHome,
          "--hub-ws-url",
          "wss://hub.example.com/api/v1/openclaw/ws/connect",
          "--hub-bot-id",
          "hub-bot",
          "--hub-secret",
          "hub-secret"
        ]
      });
    });

    expect(chunks.join("")).toContain("Installed 53aihub-workbuddy into WorkBuddy local marketplace.");
    await access(
      join(
        workbuddyHome,
        "plugins",
        "marketplaces",
        "my-experts",
        "plugins",
        "53aihub-workbuddy",
        ".mcp.json"
      )
    );
    const settings = JSON.parse(await readFile(join(workbuddyHome, "settings.json"), "utf8")) as any;
    expect(settings.enabledPlugins?.["53aihub-workbuddy@my-experts"]).toBe(true);
    expect(settings.channelsEnabled).toBe(true);
    const mcpConfig = JSON.parse(
      await readFile(
        join(
          workbuddyHome,
          "plugins",
          "marketplaces",
          "my-experts",
          "plugins",
          "53aihub-workbuddy",
          ".mcp.json"
        ),
        "utf8"
      )
    ) as any;
    expect(mcpConfig.mcpServers["53aihub-channel"].args).toEqual([
      join(
        workbuddyHome,
        "plugins",
        "marketplaces",
        "my-experts",
        "plugins",
        "53aihub-workbuddy",
        "dist",
        "workbuddy-supervisor.cjs"
      )
    ]);
    expect(mcpConfig.mcpServers["53aihub-channel"].env.HUB53AI_CHANNEL_ENTRY_PATH).toContain(
      "codebuddy-channel.cjs"
    );
  });

  it("installs a Codex App Server channel with auto-detected Codex and hidden workspace root", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-install-"));
    cleanupPaths.push(tempRoot);

    const packageRoot = await createPackageRoot(tempRoot);
    const fakeCodex = join(tempRoot, "bin", "codex");
    const fakeNode = join(tempRoot, "bin", "node");
    await mkdir(join(tempRoot, "bin"), { recursive: true });
    await writeFile(fakeCodex, "#!/bin/sh\necho codex-cli 0.134.0\n");
    await writeFile(fakeNode, "#!/bin/sh\nexit 0\n");
    await chmod(fakeCodex, 0o755);
    await chmod(fakeNode, 0o755);

    const launchctlCalls: string[][] = [];
    const launchAgent = {
      launchAgentsDir: join(tempRoot, "Library", "LaunchAgents"),
      logDir: join(tempRoot, ".53ai", "codex-channel", "logs"),
      uid: 501,
      platform: "darwin" as const,
      runLaunchctl: vi.fn(async (args: string[]) => {
        launchctlCalls.push(args);
      })
    };

    const result = await installIntoCodex({
      packageRoot,
      installRoot: join(tempRoot, ".53ai", "codex-channel"),
      workspaceRoot: join(tempRoot, ".53ai", "codex-workspaces"),
      codexBinPath: fakeCodex,
      nodeBinPath: fakeNode,
      hubWsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
      hubBotId: "hub-bot",
      hubSecret: "hub-secret",
      launchAgent
    });

    const config = JSON.parse(await readFile(result.configPath, "utf8")) as any;
    const startScript = await readFile(result.startScriptPath, "utf8");
    const plist = await readFile(result.launchAgent.plistPath, "utf8");

    expect(result.codexBinPath).toBe(fakeCodex);
    expect(result.codexVersion).toBe("codex-cli 0.134.0");
    expect(result.hubBotId).toBe("hub-bot");
    expect(result.workspaceRoot).toBe(join(tempRoot, ".53ai", "codex-workspaces"));
    expect(config).toMatchObject({
      wsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
      botId: "hub-bot",
      secret: "hub-secret",
      codexBinPath: fakeCodex,
      codexVersion: "codex-cli 0.134.0",
      workspaceRoot: join(tempRoot, ".53ai", "codex-workspaces"),
      runnerCommand: "codex-app-server",
      hostKind: "codex"
    });
    expect(startScript).toContain("HUB53AI_CODEX_CHANNEL_CONFIG=");
    expect(startScript).toContain(`exec '${fakeNode}'`);
    expect(startScript).not.toContain("exec node ");
    expect(startScript).toContain("codex-channel.cjs");
    expect(result.launchAgent).toMatchObject({
      enabled: true,
      loaded: true,
      label: "com.53ai.codex-channel",
      serviceTarget: "gui/501/com.53ai.codex-channel"
    });
    expect(plist).toContain("<key>RunAtLoad</key>");
    expect(plist).toContain("<key>KeepAlive</key>");
    expect(plist).toContain(fakeNode);
    expect(plist).toContain(result.channelEntryPath);
    expect(plist).toContain("HUB53AI_CODEX_CHANNEL_CONFIG");
    expect(plist).toContain(result.configPath);
    expect(plist).toContain(result.startScriptPath);
    expect(plist).toContain(result.launchAgent.stdoutPath);
    expect(launchctlCalls).toEqual([
      ["bootout", "gui/501", result.launchAgent.plistPath],
      ["bootstrap", "gui/501", result.launchAgent.plistPath],
      ["enable", "gui/501/com.53ai.codex-channel"],
      ["kickstart", "-k", "gui/501/com.53ai.codex-channel"],
      ["print", "gui/501/com.53ai.codex-channel"]
    ]);
    await access(join(result.destination, "dist", "codex-channel.cjs"));
    await access(join(tempRoot, ".53ai", "codex-workspaces"));

    launchctlCalls.length = 0;
    await installIntoCodex({
      packageRoot,
      installRoot: join(tempRoot, ".53ai", "codex-channel"),
      workspaceRoot: join(tempRoot, ".53ai", "codex-workspaces"),
      codexBinPath: fakeCodex,
      nodeBinPath: fakeNode,
      hubWsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
      hubBotId: "hub-bot",
      hubSecret: "hub-secret",
      launchAgent
    });
    expect(launchctlCalls.map((args) => args[0])).toEqual(["bootout", "bootstrap", "enable", "kickstart", "print"]);
  });

  it("offers WorkBuddy through the normal install command", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-auto-command-"));
    cleanupPaths.push(tempRoot);

    const packageRoot = await createPackageRoot(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const workbuddySettings = join(workbuddyHome, "settings.json");
    await mkdir(workbuddyHome, { recursive: true });
    await writeFile(workbuddySettings, JSON.stringify({ claw: { channels: {} } }, null, 2));

    const qclawConfigPath = join(tempRoot, ".qclaw", "openclaw.json");
    const qclawExtensionsDir = join(tempRoot, "Library/Application Support/QClaw/openclaw/config/extensions");
    await mkdir(join(tempRoot, ".qclaw"), { recursive: true });
    await writeGatewayConfig(qclawConfigPath, 28789, "qclaw-token");

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
            id: "qclaw",
            label: "QClaw",
            configPath: qclawConfigPath,
            extensionsDir: qclawExtensionsDir
          },
          {
            id: "workbuddy",
            label: "WorkBuddy",
            configPath: workbuddySettings,
            extensionsDir: join(workbuddyHome, "plugins", "marketplaces", "my-experts", "plugins"),
            installKind: "workbuddy",
            workbuddyHome
          }
        ],
        promptSelectHost: async (detected, incompatible) => {
          expect(incompatible).toEqual([]);
          expect(detected.map((host) => host.label)).toEqual(["QClaw", "WorkBuddy"]);
          return detected[1]!;
        }
      });
    });

    expect(chunks.join("")).toContain("Installed 53aihub-workbuddy into WorkBuddy local marketplace.");
    await access(
      join(
        workbuddyHome,
        "plugins",
        "marketplaces",
        "my-experts",
        "plugins",
        "53aihub-workbuddy",
        ".mcp.json"
      )
    );
    await expect(access(join(qclawExtensionsDir, "claw-control-center", "dist", "index.cjs"))).rejects.toThrow();
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
    ).rejects.toThrow("multiple compatible agents were detected, but no interactive terminal was available");
  });

  it("rejects installs when no compatible agent can be auto-detected", async () => {
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
    ).rejects.toThrow("could not auto-detect an installed compatible agent");
  });

  it("uses bounded fast search as a fallback for --host-kind qclaw", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-install-search-"));
    cleanupPaths.push(tempRoot);

    const configPath = join(tempRoot, "nested", ".qclaw", "openclaw.json");
    const packageRoot = await createPackageRoot(tempRoot);
    await mkdir(dirname(configPath), { recursive: true });
    await writeGatewayConfig(configPath, 28789, "qclaw-token");

    const searchedCommands: string[] = [];
    const chunks: string[] = [];
    await withCapturedStdout(chunks, async () => {
      await runInstallCommand({
        packageRoot,
        argv: ["install", "--host-kind", "qclaw"],
        hostDefinitions: [],
        platform: "linux",
        homeDir: tempRoot,
        fastSearchExec: async (file) => {
          searchedCommands.push(file);
          return { stdout: `${configPath}\n` };
        }
      });
    });

    const updated = JSON.parse(await readFile(configPath, "utf8")) as {
      plugins: {
        allow: string[];
        load: { paths: string[] };
      };
    };

    expect(searchedCommands).toContain("fd");
    expect(updated.plugins.allow).toContain("claw-control-center");
    expect(updated.plugins.load.paths[0]).toContain("QClaw");
    expect(chunks.join("")).toContain("Installed claw-control-center into QClaw.");
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
  await mkdir(join(packageRoot, ".codebuddy-plugin"), { recursive: true });
  await mkdir(join(packageRoot, "hermes", "platforms", "53aihub"), { recursive: true });
  await writeFile(join(packageRoot, "package.json"), JSON.stringify({ name: "@claw-plugin/claw-control-center" }));
  await writeFile(join(packageRoot, "openclaw.plugin.json"), JSON.stringify({ id: "claw-control-center" }));
  await writeFile(join(packageRoot, ".codebuddy-plugin", "plugin.json"), JSON.stringify({ name: "53aihub-workbuddy" }));
  await writeFile(
    join(packageRoot, ".mcp.json"),
    JSON.stringify({
      mcpServers: {
        "53aihub-channel": {
          command: "node",
          args: ["dist/workbuddy-supervisor.cjs"],
          env: {}
        }
      }
    })
  );
  await writeFile(join(packageRoot, "dist", "index.cjs"), "module.exports = {};\n");
  await writeFile(join(packageRoot, "dist", "codebuddy-channel.cjs"), "module.exports = {};\n");
  await writeFile(join(packageRoot, "dist", "codex-channel.cjs"), "module.exports = {};\n");
  await writeFile(join(packageRoot, "dist", "workbuddy-supervisor.cjs"), "module.exports = {};\n");
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
