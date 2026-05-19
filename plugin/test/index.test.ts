import { describe, expect, it, vi } from "vitest";

import plugin from "../src/index";

describe("plugin entry", () => {
  it("fails fast when gateway config is incomplete", async () => {
    vi.stubEnv("HOME", "/tmp/claw-plugin-empty-home");
    let serviceStart: ((ctx: { stateDir: string }) => Promise<void>) | undefined;

    plugin.register({
      pluginConfig: {
        gateway: {
          baseUrl: "https://gateway.example.com"
        },
        console: {
          port: 0
        }
      },
      registerService(definition) {
        serviceStart = definition.start;
      },
      logger: {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn()
      },
      version: "1.0.0",
      rootDir: "/plugin-root"
    } as never);

    await expect(serviceStart?.({ stateDir: "/tmp/.qclaw" })).rejects.toThrow(
      /gateway\.secret/i
    );
    vi.unstubAllEnvs();
  });
});
