import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { detectHostKind, resolvePluginConfigWithHostDefaults, sanitizePluginConfig } from "../src/host";

describe("host helpers", () => {
  it("detects the compatible host brand from runtime paths", () => {
    expect(detectHostKind("/Users/demo/.qclaw")).toBe("qclaw");
    expect(detectHostKind("/Users/demo/.openclaw")).toBe("openclaw");
    expect(detectHostKind("/Users/demo/.hermes/config.yaml")).toBe("hermes");
    expect(detectHostKind("/Users/demo/.workbuddy/plugins")).toBe("workbuddy");
  });

  it("falls back to OpenClaw for unsupported paths", () => {
    expect(detectHostKind("/Users/demo/.qclow/plugins")).toBe("openclaw");
    expect(detectHostKind("/Users/demo/custom-claw")).toBe("openclaw");
  });

  it("sanitizes sensitive gateway values before exposing config", () => {
    const config = sanitizePluginConfig({
      gateway: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-demo-secret"
      },
      hub53ai: {
        enabled: true,
        botId: "hub-bot",
        secret: "hub-secret",
        wsUrl: "wss://hub.example.com/ws"
      },
      console: {
        host: "127.0.0.1",
        port: 4318,
        showRawThinking: true
      }
    });

    expect(config.gateway?.secret).toBe("[redacted]");
    expect(config.gateway?.botId).toBe("bot-123");
    expect(config.hub53ai?.secret).toBe("[redacted]");
    expect(config.hub53ai?.botId).toBe("hub-bot");
    expect(config.console?.host).toBe("127.0.0.1");
    expect(config.console?.showRawThinking).toBe(true);
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
    expect(config.gateway.preferResponsesApi).toBe(false);
  });

  it("prefers the host gateway when plugin config contains a stale loopback gateway", () => {
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

    const config = resolvePluginConfigWithHostDefaults(configPath, {
      gateway: {
        baseUrl: "ws://127.0.0.1:49711",
        secret: "stale-token"
      }
    });

    expect(config.gateway.baseUrl).toBe("ws://127.0.0.1:28789");
    expect(config.gateway.secret).toBe("local-token");
  });

  it("keeps an explicit non-loopback gateway override", () => {
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

    const config = resolvePluginConfigWithHostDefaults(configPath, {
      gateway: {
        baseUrl: "wss://gateway.example.com/v1",
        secret: "remote-token"
      }
    });

    expect(config.gateway.baseUrl).toBe("wss://gateway.example.com/v1");
    expect(config.gateway.secret).toBe("remote-token");
  });

  it("reads legacy 53AIHub channel defaults from openclaw.json", () => {
    const directory = mkdtempSync(join(tmpdir(), "claw-plugin-host-"));
    const configPath = join(directory, "openclaw.json");

    writeFileSync(
      configPath,
      JSON.stringify({
        channels: {
          "53aihub": {
            botId: "legacy-bot",
            secret: "legacy-secret",
            WSUrl: "wss://legacy.example.com/api/v1/openclaw/ws/connect"
          }
        }
      })
    );

    const config = resolvePluginConfigWithHostDefaults(configPath, {
      hub53ai: {
        enabled: true
      }
    });
    expect(config.hub53ai.botId).toBe("legacy-bot");
    expect(config.hub53ai.secret).toBe("legacy-secret");
    expect(config.hub53ai.wsUrl).toBe("wss://legacy.example.com/api/v1/openclaw/ws/connect");
  });
});
