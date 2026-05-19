import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { detectHostKind, resolvePluginConfigWithHostDefaults, sanitizePluginConfig } from "../src/host";

describe("host helpers", () => {
  it("detects qclaw state directories", () => {
    expect(detectHostKind("/Users/demo/.qclaw")).toBe("qclaw");
    expect(detectHostKind("/Users/demo/.qclow/plugins")).toBe("qclaw");
  });

  it("sanitizes sensitive gateway values before exposing config", () => {
    const config = sanitizePluginConfig({
      gateway: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-demo-secret"
      },
      console: {
        host: "127.0.0.1",
        port: 4318
      }
    });

    expect(config.gateway?.secret).toBe("[redacted]");
    expect(config.gateway?.botId).toBe("bot-123");
    expect(config.console?.host).toBe("127.0.0.1");
  });

  it("reads nested gateway host defaults from openclaw.json", () => {
    const directory = mkdtempSync(join(tmpdir(), "claw-plugin-host-"));
    const configPath = join(directory, "openclaw.json");

    writeFileSync(
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

    const config = resolvePluginConfigWithHostDefaults(configPath, {});
    expect(config.gateway.baseUrl).toBe("ws://127.0.0.1:28789");
    expect(config.gateway.secret).toBe("local-token");
  });
});
