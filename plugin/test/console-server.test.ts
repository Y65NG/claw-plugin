import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { GatewayEvent, GatewaySession } from "../src/gateway-client";
import { createConsoleServer } from "../src/console-server";

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("console server", () => {
  it("keeps the current REST contract while persisting gateway-backed sessions", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-control-center-"));
    cleanupPaths.push(stateDir);

    const gateway = new FakeGateway();
    const server = createConsoleServer({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      hostKind: "qclaw",
      pluginVersion: "1.0.0",
      token: "local-token",
      gatewayConfig: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-secret"
      },
      consoleConfig: {
        host: "127.0.0.1",
        port: 0
      },
      persistence: {
        maxSessions: 20
      },
      gateway
    });

    await server.start();
    try {
      const bootstrap = await fetchJson<{ token: string; config: { gateway: { secret: string } } }>(
        `${server.baseUrl}/api/bootstrap`
      );
      expect(bootstrap.token).toBe("local-token");
      expect(bootstrap.config.gateway.secret).toBe("[redacted]");

      const created = await fetchJson<GatewaySession>(`${server.baseUrl}/api/sessions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Plugin-Token": "local-token"
        },
        body: JSON.stringify({ title: "Gateway session" })
      });
      expect(created.id).toBe("session-1");

      await fetchJson(`${server.baseUrl}/api/sessions/session-1/messages`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Plugin-Token": "local-token"
        },
        body: JSON.stringify({ content: "Say hello" })
      });

      await waitFor(async () => {
        const detail = await fetchJson<{
          messages: Array<{ role: string; content: string }>;
        }>(`${server.baseUrl}/api/sessions/session-1`);
        expect(detail.messages.at(-1)?.content).toBe("Hello from the gateway");
      });

      const persisted = JSON.parse(await readFile(join(stateDir, "claw-control-center-state.json"), "utf8")) as {
        sessions: Record<string, { messages: Array<{ role: string }> }>;
      };
      expect(persisted.sessions["session-1"]?.messages.length).toBeGreaterThanOrEqual(2);
    } finally {
      await server.stop();
    }
  });

  it("hydrates remote session detail on demand after startup sync", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-control-center-"));
    cleanupPaths.push(stateDir);

    const gateway = new FakeGateway();
    gateway.seedSession({
      session: {
        id: "session-remote",
        title: "Remote hydrated session",
        status: "completed",
        hostKind: "qclaw",
        runnerCommand: "gateway",
        createdAt: "2026-05-19T04:00:00.000Z",
        updatedAt: "2026-05-19T04:00:01.000Z",
        lastEventSeq: 2
      },
      messages: [
        {
          id: "user-1",
          sessionId: "session-remote",
          role: "user",
          content: "Hydrate me",
          createdAt: "2026-05-19T04:00:00.100Z"
        },
        {
          id: "assistant-2",
          sessionId: "session-remote",
          role: "assistant",
          content: "Hydrated from gateway",
          createdAt: "2026-05-19T04:00:01.000Z"
        }
      ],
      events: [
        {
          id: "evt-1",
          sessionId: "session-remote",
          seq: 1,
          kind: "run.started",
          payload: { ok: true },
          createdAt: "2026-05-19T04:00:00.200Z"
        },
        {
          id: "evt-2",
          sessionId: "session-remote",
          seq: 2,
          kind: "run.completed",
          payload: { ok: true },
          createdAt: "2026-05-19T04:00:01.000Z"
        }
      ]
    });

    const server = createConsoleServer({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      hostKind: "qclaw",
      pluginVersion: "1.0.0",
      token: "local-token",
      gatewayConfig: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-secret"
      },
      consoleConfig: {
        host: "127.0.0.1",
        port: 0
      },
      persistence: {
        maxSessions: 20
      },
      gateway
    });

    await server.start();
    try {
      const detail = await fetchJson<{
        session: { id: string; title: string };
        messages: Array<{ role: string; content: string }>;
      }>(`${server.baseUrl}/api/sessions/session-remote`);
      expect(detail.session.title).toBe("Remote hydrated session");
      expect(detail.messages.map((message) => message.content)).toEqual(["Hydrate me", "Hydrated from gateway"]);

      const events = await fetchJson<{ events: Array<{ seq: number; kind: string }> }>(
        `${server.baseUrl}/api/sessions/session-remote/events`
      );
      expect(events.events.map((event) => event.kind)).toEqual(["run.started", "run.completed"]);
    } finally {
      await server.stop();
    }
  });

  it("derives a final assistant message from cumulative delta output when the gateway never emits session.message", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-control-center-"));
    cleanupPaths.push(stateDir);

    const gateway = new FakeGateway();
    gateway.seedSession({
      session: {
        id: "session-long-run",
        title: "Long running summary",
        status: "completed",
        hostKind: "qclaw",
        runnerCommand: "gateway",
        createdAt: "2026-05-19T05:21:40.000Z",
        updatedAt: "2026-05-19T05:22:28.000Z",
        lastEventSeq: 4
      },
      messages: [
        {
          id: "user-1",
          sessionId: "session-long-run",
          role: "user",
          content: "从网上搜索10本书并总结",
          createdAt: "2026-05-19T05:21:40.000Z"
        },
        {
          id: "assistant-1",
          sessionId: "session-long-run",
          role: "assistant",
          content: "我将帮您从网上搜索10本书并进行总结。首先让我查看一下可用的搜索技能。",
          createdAt: "2026-05-19T05:21:41.000Z"
        },
        {
          id: "assistant-2",
          sessionId: "session-long-run",
          role: "assistant",
          content: "现在我将使用联网搜索工具来搜索10本推荐书籍。让我先搜索值得阅读的书籍推荐。",
          createdAt: "2026-05-19T05:21:49.000Z"
        }
      ],
      events: [
        {
          id: "evt-1",
          sessionId: "session-long-run",
          seq: 3,
          kind: "assistant.delta",
          payload: {
            content:
              "我将帮您从网上搜索10本书并进行总结。首先让我查看一下可用的搜索技能。现在我将使用联网搜索工具来搜索10本推荐书籍。让我先搜索值得阅读的书籍推荐。\n\n## 10本值得阅读的书籍总结\n\n1. 《人类简史》\n2. 《1984》"
          },
          createdAt: "2026-05-19T05:22:20.000Z"
        },
        {
          id: "evt-2",
          sessionId: "session-long-run",
          seq: 4,
          kind: "run.completed",
          payload: { ok: true },
          createdAt: "2026-05-19T05:22:28.000Z"
        }
      ]
    });

    const server = createConsoleServer({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      hostKind: "qclaw",
      pluginVersion: "1.0.0",
      token: "local-token",
      gatewayConfig: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-secret"
      },
      consoleConfig: {
        host: "127.0.0.1",
        port: 0
      },
      persistence: {
        maxSessions: 20
      },
      gateway
    });

    await server.start();
    try {
      const detail = await fetchJson<{
        messages: Array<{ role: string; content: string }>;
      }>(`${server.baseUrl}/api/sessions/session-long-run`);
      expect(detail.messages.map((message) => message.content)).toEqual([
        "从网上搜索10本书并总结",
        "我将帮您从网上搜索10本书并进行总结。首先让我查看一下可用的搜索技能。",
        "现在我将使用联网搜索工具来搜索10本推荐书籍。让我先搜索值得阅读的书籍推荐。",
        "## 10本值得阅读的书籍总结\n\n1. 《人类简史》\n2. 《1984》"
      ]);
    } finally {
      await server.stop();
    }
  });

  it("does not derive a duplicate tail chunk when split deltas already have a final assistant message", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-control-center-"));
    cleanupPaths.push(stateDir);

    const gateway = new FakeGateway();
    gateway.seedSession({
      session: {
        id: "session-split-delta",
        title: "Split delta session",
        status: "completed",
        hostKind: "qclaw",
        runnerCommand: "gateway",
        createdAt: "2026-05-20T03:49:20.000Z",
        updatedAt: "2026-05-20T03:49:46.000Z",
        lastEventSeq: 5
      },
      messages: [
        {
          id: "user-1",
          sessionId: "session-split-delta",
          role: "user",
          content: "解释流式输出",
          createdAt: "2026-05-20T03:49:20.000Z"
        },
        {
          id: "assistant-5",
          sessionId: "session-split-delta",
          role: "assistant",
          content: "第一部分，第二部分。",
          createdAt: "2026-05-20T03:49:46.000Z"
        }
      ],
      events: [
        {
          id: "evt-1",
          sessionId: "session-split-delta",
          seq: 2,
          kind: "assistant.delta",
          payload: { content: "第一部分，", mode: "append" },
          createdAt: "2026-05-20T03:49:45.000Z"
        },
        {
          id: "evt-2",
          sessionId: "session-split-delta",
          seq: 3,
          kind: "assistant.delta",
          payload: { content: "第二部分。", mode: "append" },
          createdAt: "2026-05-20T03:49:45.100Z"
        },
        {
          id: "evt-3",
          sessionId: "session-split-delta",
          seq: 4,
          kind: "assistant.message",
          payload: { content: "第一部分，第二部分。" },
          createdAt: "2026-05-20T03:49:46.000Z"
        },
        {
          id: "evt-4",
          sessionId: "session-split-delta",
          seq: 5,
          kind: "run.completed",
          payload: { ok: true },
          createdAt: "2026-05-20T03:49:46.000Z"
        }
      ]
    });

    const server = createConsoleServer({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      hostKind: "qclaw",
      pluginVersion: "1.0.0",
      token: "local-token",
      gatewayConfig: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-secret"
      },
      consoleConfig: {
        host: "127.0.0.1",
        port: 0
      },
      persistence: {
        maxSessions: 20
      },
      gateway
    });

    await server.start();
    try {
      const detail = await fetchJson<{
        messages: Array<{ role: string; content: string }>;
      }>(`${server.baseUrl}/api/sessions/session-split-delta`);
      expect(detail.messages.map((message) => message.content)).toEqual([
        "解释流式输出",
        "第一部分，第二部分。"
      ]);
    } finally {
      await server.stop();
    }
  });

  it("surfaces host skill and model metadata in bootstrap and status responses", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-control-center-"));
    cleanupPaths.push(stateDir);

    const configPath = join(stateDir, "openclaw.json");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          agents: {
            defaults: {
              model: {
                primary: "qclaw/modelroute"
              }
            }
          },
          skills: {
            entries: {
              "online-search": { enabled: true },
              weather: { enabled: false },
              browser: { enabled: true }
            }
          }
        },
        null,
        2
      )
    );

    const gateway = new FakeGateway();
    const server = createConsoleServer({
      stateDir,
      configPath,
      hostKind: "qclaw",
      pluginVersion: "1.0.0",
      token: "local-token",
      gatewayConfig: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-secret"
      },
      consoleConfig: {
        host: "127.0.0.1",
        port: 0
      },
      persistence: {
        maxSessions: 20
      },
      gateway
    });

    await server.start();
    try {
      const bootstrap = await fetchJson<{
        status: {
          modelPrimary?: string;
          enabledSkills?: string[];
        };
      }>(`${server.baseUrl}/api/bootstrap`);
      expect(bootstrap.status.modelPrimary).toBe("qclaw/modelroute");
      expect(bootstrap.status.enabledSkills).toEqual(["browser", "online-search"]);

      const status = await fetchJson<{
        modelPrimary?: string;
        enabledSkills?: string[];
      }>(`${server.baseUrl}/api/status`);
      expect(status.modelPrimary).toBe("qclaw/modelroute");
      expect(status.enabledSkills).toEqual(["browser", "online-search"]);
    } finally {
      await server.stop();
    }
  });

  it("prefers runtime skill metadata from the gateway and falls back safely when unavailable", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-control-center-"));
    cleanupPaths.push(stateDir);

    const configPath = join(stateDir, "openclaw.json");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          agents: {
            defaults: {
              model: {
                primary: "config/model"
              }
            }
          },
          skills: {
            entries: {}
          }
        },
        null,
        2
      )
    );

    const gateway = new FakeGateway();
    gateway.runtimeInfo = {
      modelPrimary: "runtime/model",
      enabledSkills: ["weather", "web_search"],
      cronScheduler: {
        enabled: true,
        jobCount: 1,
        nextWakeAt: "2026-05-21T08:00:00.000Z"
      },
      cronTasks: [
        {
          id: "cron-1",
          name: "Morning brief",
          enabled: true,
          schedule: "cron 0 8 * * *",
          nextRunAt: "2026-05-21T08:00:00.000Z"
        }
      ]
    };
    const server = createConsoleServer({
      stateDir,
      configPath,
      hostKind: "openclaw",
      pluginVersion: "1.0.0",
      token: "local-token",
      gatewayConfig: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-secret"
      },
      consoleConfig: {
        host: "127.0.0.1",
        port: 0
      },
      persistence: {
        maxSessions: 20
      },
      gateway
    });

    await server.start();
    try {
      const bootstrap = await fetchJson<{
        status: {
          modelPrimary?: string;
          enabledSkills?: string[];
          cronScheduler?: { enabled?: boolean; jobCount?: number; nextWakeAt?: string };
          cronTasks?: Array<{ id: string; name: string; enabled: boolean; schedule?: string }>;
        };
      }>(`${server.baseUrl}/api/bootstrap`);
      expect(bootstrap.status.modelPrimary).toBe("runtime/model");
      expect(bootstrap.status.enabledSkills).toEqual(["weather", "web_search"]);
      expect(bootstrap.status.cronScheduler).toMatchObject({
        enabled: true,
        jobCount: 1,
        nextWakeAt: "2026-05-21T08:00:00.000Z"
      });
      expect(bootstrap.status.cronTasks).toEqual([
        {
          id: "cron-1",
          name: "Morning brief",
          enabled: true,
          schedule: "cron 0 8 * * *",
          nextRunAt: "2026-05-21T08:00:00.000Z"
        }
      ]);

      gateway.runtimeInfoError = new Error("skills.status unsupported");
      const status = await fetchJson<{
        modelPrimary?: string;
        enabledSkills?: string[];
        cronScheduler?: { enabled?: boolean; jobCount?: number; nextWakeAt?: string };
        cronTasks?: Array<{ id: string; name: string; enabled: boolean; schedule?: string }>;
      }>(`${server.baseUrl}/api/status`);
      expect(status.modelPrimary).toBe("runtime/model");
      expect(status.enabledSkills).toEqual(["weather", "web_search"]);
      expect(status.cronScheduler?.jobCount).toBe(1);
      expect(status.cronTasks?.[0]?.name).toBe("Morning brief");
    } finally {
      await server.stop();
    }
  });

  it("surfaces redacted 53AIHub bridge config and status", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-control-center-"));
    cleanupPaths.push(stateDir);

    const gateway = new FakeGateway();
    const server = createConsoleServer({
      stateDir,
      configPath: join(stateDir, "openclaw.json"),
      hostKind: "qclaw",
      pluginVersion: "1.0.0",
      token: "local-token",
      gatewayConfig: {
        baseUrl: "https://gateway.example.com",
        botId: "bot-123",
        secret: "sk-secret"
      },
      hub53aiConfig: {
        enabled: false,
        botId: "hub-bot",
        secret: "hub-secret",
        wsUrl: "wss://hub.example.com/api/v1/openclaw/ws/connect",
        accessPolicy: "open",
        allowFrom: [],
        sendThinkingMessage: true,
        reconnectBaseMs: 2000,
        maxReconnectAttempts: 10
      },
      consoleConfig: {
        host: "127.0.0.1",
        port: 0
      },
      persistence: {
        maxSessions: 20
      },
      gateway
    });

    await server.start();
    try {
      const bootstrap = await fetchJson<{
        status: {
          hub53ai?: { enabled: boolean; connectionStatus: string; botId?: string };
        };
        config: {
          hub53ai?: { secret?: string; botId?: string };
        };
      }>(`${server.baseUrl}/api/bootstrap`);

      expect(bootstrap.status.hub53ai).toMatchObject({
        enabled: false,
        connectionStatus: "disabled",
        botId: "hu***ot"
      });
      expect(bootstrap.config.hub53ai).toMatchObject({
        botId: "hub-bot",
        secret: "[redacted]"
      });
    } finally {
      await server.stop();
    }
  });
});

class FakeGateway {
  private sessions = new Map<string, GatewaySession>();
  private messageHistory = new Map<string, Array<{ id: string; sessionId: string; role: string; content: string; createdAt: string }>>();
  private eventHistory = new Map<string, GatewayEvent[]>();
  private listeners = new Map<string, Set<(event: GatewayEvent) => void>>();
  runtimeInfo?: {
    modelPrimary?: string;
    enabledSkills: string[];
    cronScheduler?: { enabled?: boolean; jobCount?: number; nextWakeAt?: string };
    cronTasks?: Array<{ id: string; name: string; enabled: boolean; schedule?: string; nextRunAt?: string }>;
  };
  runtimeInfoError?: Error;

  async listSessions(): Promise<GatewaySession[]> {
    return [...this.sessions.values()];
  }

  async getRuntimeInfo(): Promise<NonNullable<FakeGateway["runtimeInfo"]>> {
    if (this.runtimeInfoError) {
      throw this.runtimeInfoError;
    }
    return this.runtimeInfo ?? { enabledSkills: [] };
  }

  async createSession(title: string): Promise<GatewaySession> {
    const now = new Date().toISOString();
    const session: GatewaySession = {
      id: "session-1",
      title,
      status: "idle",
      hostKind: "qclaw",
      runnerCommand: "gateway",
      createdAt: now,
      updatedAt: now,
      lastEventSeq: 0
    };
    this.sessions.set(session.id, session);
    return session;
  }

  async getSessionMessages(sessionId: string) {
    return this.messageHistory.get(sessionId) ?? [];
  }

  async sendMessage(sessionId: string, content: string): Promise<void> {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error("unknown session");
    }
    session.status = "running";
    session.updatedAt = new Date().toISOString();
    setTimeout(() => {
      this.emit(sessionId, {
        id: "evt-1",
        sessionId,
        seq: 1,
        kind: "assistant.message",
        payload: {
          content: content === "Say hello" ? "Hello from the gateway" : "Unknown"
        },
        createdAt: new Date().toISOString()
      });
      this.emit(sessionId, {
        id: "evt-2",
        sessionId,
        seq: 2,
        kind: "run.completed",
        payload: { ok: true },
        createdAt: new Date().toISOString()
      });
    }, 10);
  }

  async getSession(sessionId: string) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error("unknown session");
    }
    return session;
  }

  async listEvents(_sessionId: string, _afterSeq = 0): Promise<GatewayEvent[]> {
    return (this.eventHistory.get(_sessionId) ?? []).filter((event) => event.seq > _afterSeq);
  }

  async stop() {
    return;
  }

  subscribe(
    sessionId: string,
    _afterSeq: number,
    handlers: {
      onEvent: (event: GatewayEvent) => void;
      onDisconnect: (error?: Error) => void;
    }
  ): () => void {
    const listeners = this.listeners.get(sessionId) ?? new Set<(event: GatewayEvent) => void>();
    listeners.add(handlers.onEvent);
    this.listeners.set(sessionId, listeners);
    return () => {
      listeners.delete(handlers.onEvent);
      if (listeners.size === 0) {
        this.listeners.delete(sessionId);
      }
    };
  }

  private emit(sessionId: string, event: GatewayEvent) {
    this.sessions.get(sessionId)!.lastEventSeq = event.seq;
    this.sessions.get(sessionId)!.status = event.kind === "run.completed" ? "completed" : "running";
    const history = this.eventHistory.get(sessionId) ?? [];
    history.push(event);
    this.eventHistory.set(sessionId, history);
    for (const listener of this.listeners.get(sessionId) ?? []) {
      listener(event);
    }
  }

  seedSession(input: {
    session: GatewaySession;
    messages: Array<{ id: string; sessionId: string; role: string; content: string; createdAt: string }>;
    events: GatewayEvent[];
  }) {
    this.sessions.set(input.session.id, input.session);
    this.messageHistory.set(input.session.id, input.messages);
    this.eventHistory.set(input.session.id, input.events);
  }
}

async function fetchJson<T>(input: string, init?: RequestInit): Promise<T> {
  const response = await fetch(input, init);
  if (!response.ok) {
    throw new Error(`request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

async function waitFor(assertion: () => Promise<void>, timeoutMs = 1500): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw lastError instanceof Error ? lastError : new Error("condition not met");
}
