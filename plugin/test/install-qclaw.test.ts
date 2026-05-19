import { afterEach, describe, expect, it } from "vitest";
import { access, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { installIntoQClaw, runInstallCommand } from "../src/install-qclaw";

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
          secret: "sk-secret"
        },
        console: {
          port: 4321
        }
      }
    });
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
          baseUrl: "ws://127.0.0.1:28789",
          secret: "local-token"
        },
        console: {
          host: "127.0.0.1"
        }
      }
    });
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

  it("installs into OpenClaw paths when the openclaw target is selected", async () => {
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
          "--target",
          "openclaw",
          "--config-path",
          configPath,
          "--extensions-dir",
          extensionsDir
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
          baseUrl: "ws://127.0.0.1:18789",
          secret: "openclaw-password"
        }
      }
    });
    await access(join(extensionsDir, "claw-control-center", "dist", "index.cjs"));
    expect(chunks.join("")).toContain("Installed claw-control-center into OpenClaw.");
    expect(chunks.join("")).toContain("Restart OpenClaw to load the plugin.");
  });
});
