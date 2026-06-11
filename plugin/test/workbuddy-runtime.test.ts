import { createServer, type Server } from "node:http";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { loadWorkBuddyRuntime, pokeWorkBuddySessionRefresh } from "../src/workbuddy-runtime";

const cleanupPaths: string[] = [];
const cleanupServers: Server[] = [];

afterEach(async () => {
  await Promise.all(cleanupServers.splice(0).map((server) => closeServer(server)));
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("WorkBuddy runtime adapter", () => {
  it("aggregates info, workers, plugins, skills, and scheduled tasks from WorkBuddy API", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-runtime-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    await mkdir(join(workbuddyHome, "sessions"), { recursive: true });

    const api = await createFakeWorkBuddyApi();
    cleanupServers.push(api.server);
    await writeFile(
      join(workbuddyHome, "sessions", "12345.json"),
      JSON.stringify({
        pid: 12345,
        sessionId: "53aihub-workbuddy-shared",
        endpoint: api.url,
        updatedAt: Date.now()
      })
    );

    const runtime = await loadWorkBuddyRuntime({
      workbuddyHome,
      sessionId: "53aihub-workbuddy-shared",
      timeoutMs: 500
    });

    expect(runtime.info).toMatchObject({ version: "5.0.3", gatewayMode: "local" });
    expect(runtime.workers).toEqual([
      expect.objectContaining({
        sessionId: "53aihub-workbuddy-shared",
        endpoint: api.url,
        healthy: true
      })
    ]);
    expect(runtime.plugins).toEqual([
      expect.objectContaining({
        id: "weixinpay@workbuddy-builtin",
        name: "weixinpay",
        enabled: true
      })
    ]);
    expect(runtime.skills).toEqual([
      expect.objectContaining({
        id: "weixinpay@workbuddy-builtin:weixinpay-intro",
        name: "weixinpay-intro",
        pluginName: "weixinpay"
      })
    ]);
    expect(runtime.cronTasks).toEqual([
      expect.objectContaining({
        id: "task-1",
        name: "daily-check"
      })
    ]);

    await expect(pokeWorkBuddySessionRefresh({ workbuddyHome, timeoutMs: 500 })).resolves.toMatchObject({
      attempted: 2,
      ok: 2,
      endpoints: [api.url]
    });
  });

  it("falls back to local enabled plugin manifests when API endpoints are unavailable", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "workbuddy-runtime-local-"));
    cleanupPaths.push(tempRoot);
    const workbuddyHome = join(tempRoot, ".workbuddy");
    const pluginRoot = join(workbuddyHome, "plugins", "marketplaces", "my-experts", "plugins", "demo-plugin");
    await mkdir(join(pluginRoot, ".codebuddy-plugin"), { recursive: true });
    await mkdir(join(pluginRoot, "skills", "demo-skill"), { recursive: true });
    await writeFile(
      join(workbuddyHome, "settings.json"),
      JSON.stringify({ enabledPlugins: { "demo-plugin@my-experts": true } })
    );
    await writeFile(
      join(pluginRoot, ".codebuddy-plugin", "plugin.json"),
      JSON.stringify({
        name: "demo-plugin",
        description: "Demo plugin",
        version: "1.0.0"
      })
    );
    await writeFile(join(pluginRoot, "skills", "demo-skill", "SKILL.md"), "# Demo Skill\n\nUse for tests.\n");

    const runtime = await loadWorkBuddyRuntime({ workbuddyHome, timeoutMs: 10 });

    expect(runtime.plugins).toEqual([
      expect.objectContaining({
        id: "demo-plugin@my-experts",
        name: "demo-plugin",
        enabled: true
      })
    ]);
    expect(runtime.skills).toEqual([
      expect.objectContaining({
        id: "demo-plugin:demo-skill",
        name: "demo-skill",
        description: "Demo Skill"
      })
    ]);
  });
});

async function createFakeWorkBuddyApi(): Promise<{ server: Server; url: string }> {
  const server = createServer((request, response) => {
    response.setHeader("content-type", "application/json");
    if (request.url === "/api/v1/info") {
      response.end(JSON.stringify({ data: { version: "5.0.3", gatewayMode: "local" } }));
      return;
    }
    if (request.url === "/api/v1/workers") {
      response.end(JSON.stringify({
        data: [
          {
            pid: 12345,
            sessionId: "53aihub-workbuddy-shared",
            endpoint: `http://${request.headers.host}`,
            updatedAt: Date.now()
          }
        ]
      }));
      return;
    }
    if (request.url === "/api/v1/plugins") {
      response.end(JSON.stringify({
        data: [
          {
            name: "weixinpay",
            version: "1.1.102",
            marketplace: "workbuddy-builtin",
            status: "enabled",
            skills: [{ name: "weixinpay-intro", description: "Intro" }]
          }
        ]
      }));
      return;
    }
    if (request.url?.startsWith("/api/v1/scheduled-tasks")) {
      response.end(JSON.stringify({ data: { tasks: [{ id: "task-1", name: "daily-check", enabled: true }] } }));
      return;
    }
    if (request.url === "/api/v1/sessions") {
      response.end(JSON.stringify({ data: [] }));
      return;
    }
    response.statusCode = 404;
    response.end(JSON.stringify({ error: "not found" }));
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("failed to bind fake WorkBuddy API");
  }
  return {
    server,
    url: `http://127.0.0.1:${address.port}`
  };
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}
