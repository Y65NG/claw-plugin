import { afterEach, describe, expect, it } from "vitest";
import { createServer } from "node:http";

import { WebSocketServer } from "ws";

import { createGatewayClient } from "../src/gateway-client";

const servers = new Set<ReturnType<typeof createServer>>();

afterEach(async () => {
  await Promise.all(
    Array.from(servers, (server) => new Promise<void>((resolve) => server.close(() => resolve())))
  );
  servers.clear();
});

describe("gateway client", () => {
  it("speaks the OpenClaw websocket RPC handshake and carries the shared token", async () => {
    let capturedToken = "";
    let capturedMethod = "";
    let capturedMinProtocol = 0;
    let capturedMaxProtocol = 0;

    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
            params?: {
              auth?: {
                token?: string;
              };
            };
          };

          if (frame.method === "connect") {
            capturedToken = String(frame.params?.auth?.token ?? "");
            capturedMinProtocol = Number((frame.params as Record<string, unknown>)?.minProtocol ?? 0);
            capturedMaxProtocol = Number((frame.params as Record<string, unknown>)?.maxProtocol ?? 0);
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  type: "hello-ok",
                  protocol: 4
                }
              })
            );
            return;
          }

          capturedMethod = frame.method;
          client.send(
            JSON.stringify({
              type: "res",
              id: frame.id,
              ok: true,
              payload: {
                ok: true,
                key: "agent:main:test-session",
                sessionId: "session-1",
                label: "Gateway session"
              }
            })
          );
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const session = await client.createSession("Gateway session");

    expect(capturedMethod).toBe("sessions.create");
    expect(capturedToken).toBe("shared-token");
    expect(capturedMinProtocol).toBe(4);
    expect(capturedMaxProtocol).toBe(4);
    expect(session.id).toBe("agent:main:test-session");
    expect(session.title).toBe("Gateway session");
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("sends OpenClaw messages through chat.send without external delivery", async () => {
    let capturedMethod = "";
    let capturedParams: Record<string, unknown> = {};

    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
            params?: Record<string, unknown>;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          capturedMethod = frame.method;
          capturedParams = frame.params ?? {};
          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    await client.sendMessage("agent:main:telegram:direct:123", "hello");

    expect(capturedMethod).toBe("chat.send");
    expect(capturedParams).toMatchObject({
      sessionKey: "agent:main:telegram:direct:123",
      message: "hello",
      deliver: false
    });
    expect(capturedParams).toHaveProperty("idempotencyKey");
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("prefers Gateway HTTP responses SSE when enabled and emits text plus reasoning deltas", async () => {
    const received: Array<{ kind: string; content: string; transport?: unknown }> = [];
    const wsMethods: string[] = [];
    let capturedAuthorization = "";
    let capturedSessionKey = "";
    let capturedModelOverride = "";
    let capturedBody = "";

    const httpServer = createServer((request, response) => {
      if (request.method !== "POST" || request.url !== "/v1/responses") {
        response.writeHead(404);
        response.end("not found");
        return;
      }

      capturedAuthorization = String(request.headers.authorization ?? "");
      capturedSessionKey = String(request.headers["x-openclaw-session-key"] ?? "");
      capturedModelOverride = String(request.headers["x-openclaw-model"] ?? "");
      request.on("data", (chunk) => {
        capturedBody += chunk.toString();
      });
      request.on("end", () => {
        response.writeHead(200, {
          "Content-Type": "text/event-stream"
        });
        response.write('event: response.created\ndata: {"type":"response.created","response":{"id":"resp_1"}}\n\n');
        response.write(
          'event: response.reasoning_text.delta\ndata: {"type":"response.reasoning_text.delta","delta":"thinking"}\n\n'
        );
        response.write('event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"Hel"}\n\n');
        response.write('event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"lo"}\n\n');
        response.write('event: response.output_text.done\ndata: {"type":"response.output_text.done","text":"Hello"}\n\n');
        response.write('event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_1"}}\n\n');
        response.end("data: [DONE]\n\n");
      });
    });
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };
          wsMethods.push(frame.method);
          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true, protocol: 4 } }));
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token",
      preferResponsesApi: true,
      modelOverride: "openai/gpt-5.5"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          content: String((event.payload as { content?: string }).content ?? ""),
          transport: (event.payload as { transport?: unknown }).transport
        }),
      onDisconnect: () => {}
    });

    await client.sendMessage("agent:main:test-session", "hello");

    await eventually(() => {
      expect(capturedAuthorization).toBe("Bearer shared-token");
      expect(capturedSessionKey).toBe("agent:main:test-session");
      expect(capturedModelOverride).toBe("openai/gpt-5.5");
      expect(JSON.parse(capturedBody)).toMatchObject({
        model: "openclaw",
        stream: true,
        input: "hello",
        user: "agent:main:test-session"
      });
      expect(wsMethods).not.toContain("chat.send");
      expect(received).toEqual([
        { kind: "run.started", content: "", transport: "responses-http" },
        { kind: "assistant.thinking", content: "thinking", transport: "responses-http" },
        { kind: "assistant.delta", content: "Hel", transport: "responses-http" },
        { kind: "assistant.delta", content: "lo", transport: "responses-http" },
        { kind: "assistant.message", content: "Hello", transport: "responses-http" },
        { kind: "run.completed", content: "", transport: "responses-http" }
      ]);
    });

    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("falls back to websocket chat.send when Gateway HTTP responses is unavailable", async () => {
    let capturedMethod = "";
    const httpServer = createServer((request, response) => {
      if (request.url === "/v1/responses") {
        response.writeHead(404);
        response.end("not found");
        return;
      }
      response.writeHead(404);
      response.end("not found");
    });
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };
          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }
          capturedMethod = frame.method;
          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token",
      preferResponsesApi: true
    });

    await client.sendMessage("agent:main:test-session", "hello");

    expect(capturedMethod).toBe("chat.send");
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("reads runtime model and enabled skills from Gateway runtime metadata", async () => {
    const capturedMethods: string[] = [];
    const capturedCronListParams: Array<Record<string, unknown> | undefined> = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
            params?: Record<string, unknown>;
          };
          capturedMethods.push(frame.method);
          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }
          if (frame.method === "skills.status") {
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  modelPrimary: "openai/gpt-5.5",
                  skills: [
                    "browser",
                    { id: "weather", enabled: true },
                    { id: "disabled-skill", enabled: false },
                    { name: "web_search", status: "ready" },
                    { key: "not-ready", status: "disabled" }
                  ]
                }
              })
            );
            return;
          }
          if (frame.method === "cron.status") {
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  enabled: true,
                  storePath: "/Users/me/.openclaw/cron/jobs.json",
                  jobs: 3,
                  nextWakeAtMs: 1770000000000
                }
              })
            );
            return;
          }
          if (frame.method === "cron.list") {
            capturedCronListParams.push(frame.params);
            const offset = Number(frame.params?.offset ?? 0);
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  total: 3,
                  offset,
                  limit: 2,
                  hasMore: offset === 0,
                  nextOffset: offset === 0 ? 2 : null,
                  jobs:
                    offset === 0
                      ? [
                          {
                            id: "job-1",
                            name: "Daily digest",
                            enabled: true,
                            agentId: "main",
                            schedule: {
                              kind: "cron",
                              expr: "0 8 * * *",
                              timezone: "America/New_York"
                            },
                            nextRunAtMs: 1770000000000,
                            lastRunAtMs: 1769900000000
                          },
                          {
                            id: "job-2",
                            name: "Disabled cleanup",
                            enabled: false,
                            schedule: {
                              kind: "every",
                              everyMs: 3600000
                            }
                          }
                        ]
                      : [
                          {
                            id: "job-3",
                            name: "Weekly planning",
                            enabled: true,
                            schedule: {
                              kind: "cron",
                              expr: "0 9 * * 1"
                            }
                          }
                        ]
                }
              })
            );
            return;
          }
          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    await expect(client.getRuntimeInfo()).resolves.toEqual({
      modelPrimary: "openai/gpt-5.5",
      enabledSkills: ["browser", "weather", "web_search"],
      cronScheduler: {
        enabled: true,
        storePath: "/Users/me/.openclaw/cron/jobs.json",
        jobCount: 3,
        nextWakeAt: "2026-02-02T02:40:00.000Z"
      },
      cronTasks: [
        {
          id: "job-1",
          name: "Daily digest",
          enabled: true,
          agentId: "main",
          schedule: "cron 0 8 * * * · America/New_York",
          nextRunAt: "2026-02-02T02:40:00.000Z",
          lastRunAt: "2026-01-31T22:53:20.000Z"
        },
        {
          id: "job-2",
          name: "Disabled cleanup",
          enabled: false,
          schedule: "every 1h"
        },
        {
          id: "job-3",
          name: "Weekly planning",
          enabled: true,
          schedule: "cron 0 9 * * 1"
        }
      ]
    });
    expect(capturedMethods).toContain("skills.status");
    expect(capturedMethods).toContain("cron.status");
    expect(capturedMethods).toContain("cron.list");
    expect(capturedCronListParams).toMatchObject([
      { includeDisabled: true, limit: 50, offset: 0 },
      { includeDisabled: true, limit: 50, offset: 2 }
    ]);
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("returns empty runtime metadata when Gateway does not support skills.status", async () => {
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };
          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }
          client.send(
            JSON.stringify({
              type: "res",
              id: frame.id,
              ok: false,
              error: {
                message: "method not found"
              }
            })
          );
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    await expect(client.getRuntimeInfo()).resolves.toEqual({
      enabledSkills: []
    });
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("reads official Gateway health summary", async () => {
    const capturedMethods: string[] = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };
          capturedMethods.push(frame.method);
          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }
          if (frame.method === "health") {
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  ok: false,
                  status: "degraded",
                  ts: 1779335113816,
                  durationMs: 984
                }
              })
            );
            return;
          }
          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    await expect(client.getHealth()).resolves.toEqual({
      ok: false,
      status: "degraded",
      checkedAt: "2026-05-21T03:45:13.816Z",
      durationMs: 984
    });
    expect(capturedMethods).toContain("health");
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("drops replayed stream events at or below the subscribed sequence", async () => {
    const received: string[] = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
            params?: Record<string, unknown>;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            const now = Date.now();
            client.send(
              JSON.stringify({
                type: "event",
                event: "chat",
                payload: {
                  sessionKey: "agent:main:test-session",
                  seq: 4,
                  message: { role: "assistant", content: [{ type: "text", text: "old" }], timestamp: now - 60_000 }
                }
              })
            );
            client.send(
              JSON.stringify({
                type: "event",
                event: "chat",
                payload: {
                  sessionKey: "agent:main:test-session",
                  seq: 6,
                  message: { role: "assistant", content: [{ type: "text", text: "new" }], timestamp: now }
                }
              })
            );
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 5, {
      onEvent: (event) => received.push(String((event.payload as { content?: string }).content ?? "")),
      onDisconnect: () => {}
    });

    await eventually(() => expect(received).toEqual(["new"]));
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("uses chat deltaText as append chunks instead of replacing with cumulative message content", async () => {
    const received: Array<{ kind: string; content: string; mode?: unknown }> = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
            params?: Record<string, unknown>;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            const now = Date.now();
            for (const payload of [
              {
                state: "delta",
                deltaText: "Hel",
                message: { role: "assistant", content: [{ type: "text", text: "Hello" }], timestamp: now }
              },
              {
                state: "delta",
                deltaText: "lo",
                message: { role: "assistant", content: [{ type: "text", text: "Hello" }], timestamp: now }
              },
              {
                state: "final",
                message: { role: "assistant", content: [{ type: "text", text: "Hello" }], timestamp: now }
              }
            ]) {
              client.send(
                JSON.stringify({
                  type: "event",
                  event: "chat",
                  payload: {
                    sessionKey: "agent:main:test-session",
                    runId: "run-1",
                    seq: received.length + 1,
                    ...payload
                  }
                })
              );
            }
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          content: String((event.payload as { content?: string }).content ?? ""),
          mode: (event.payload as { mode?: unknown }).mode
        }),
      onDisconnect: () => {}
    });

    await eventually(() =>
      expect(received).toEqual([
        { kind: "assistant.delta", content: "Hel", mode: "append" },
        { kind: "assistant.delta", content: "lo", mode: "append" },
        { kind: "assistant.message", content: "Hello", mode: "replace" }
      ])
    );
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("normalizes cumulative chat deltas as replace snapshots instead of appending duplicates", async () => {
    const received: Array<{ kind: string; content: string; mode?: unknown; synthetic?: unknown }> = [];
    const longSnapshot =
      "开头" + Array.from({ length: 18 }, (_, index) => `第 ${index + 1} 句用于验证累计快照归一化。`).join("");
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            const now = Date.now();
            for (const payload of [
              {
                seq: 1,
                deltaText: "开头"
              },
              {
                seq: 2,
                deltaText: longSnapshot
              }
            ]) {
              client.send(
                JSON.stringify({
                  type: "event",
                  event: "chat",
                  payload: {
                    sessionKey: "agent:main:test-session",
                    runId: "run-1",
                    state: "delta",
                    message: {
                      role: "assistant",
                      content: [{ type: "text", text: payload.deltaText }],
                      timestamp: now
                    },
                    ...payload
                  }
                })
              );
            }
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          content: String((event.payload as { content?: string }).content ?? ""),
          mode: (event.payload as { mode?: unknown }).mode,
          synthetic: (event.payload as { syntheticChunk?: unknown }).syntheticChunk
        }),
      onDisconnect: () => {}
    });

    await eventually(() =>
      expect(received).toEqual([
        { kind: "assistant.delta", content: "开头", mode: "append", synthetic: undefined },
        { kind: "assistant.delta", content: longSnapshot, mode: "replace", synthetic: undefined }
      ])
    );
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("strips reasoning-prefixed final snapshots when streamed answer text is already known", async () => {
    const received: Array<{ kind: string; content: string; mode?: unknown }> = [];
    const finalAnswer = "Final visible answer.";
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            const now = Date.now();
            for (const payload of [
              {
                seq: 1,
                state: "delta",
                deltaText: finalAnswer
              },
              {
                seq: 2,
                state: "final",
                deltaText: `Reasoning prefix that should not be rendered. ${finalAnswer}`
              }
            ]) {
              client.send(
                JSON.stringify({
                  type: "event",
                  event: "chat",
                  payload: {
                    sessionKey: "agent:main:test-session",
                    runId: "run-1",
                    message: {
                      role: "assistant",
                      content: [{ type: "text", text: payload.deltaText }],
                      timestamp: now
                    },
                    ...payload
                  }
                })
              );
            }
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          content: String((event.payload as { content?: string }).content ?? ""),
          mode: (event.payload as { mode?: unknown }).mode
        }),
      onDisconnect: () => {}
    });

    await eventually(() =>
      expect(received).toEqual([
        { kind: "assistant.delta", content: finalAnswer, mode: "append" },
        { kind: "assistant.message", content: finalAnswer, mode: "replace" }
      ])
    );
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("maps nested session.tool result phases to tool.result events", async () => {
    const received: Array<{ kind: string; name?: unknown; result?: unknown }> = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            client.send(
              JSON.stringify({
                type: "event",
                event: "session.tool",
                payload: {
                  sessionKey: "agent:main:test-session",
                  seq: 9,
                  data: {
                    phase: "result",
                    name: "web_search",
                    result: {
                      details: {
                        query: "books",
                        count: 3
                      }
                    }
                  }
                }
              })
            );
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          name: (event.payload as { data?: { name?: unknown } }).data?.name,
          result: (event.payload as { data?: { result?: unknown } }).data?.result
        }),
      onDisconnect: () => {}
    });

    await eventually(() =>
      expect(received).toEqual([
        {
          kind: "tool.result",
          name: "web_search",
          result: {
            details: {
              query: "books",
              count: 3
            }
          }
        }
      ])
    );
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("collapses duplicated leading words in exposed reasoning text", async () => {
    const received: Array<{ kind: string; content: string }> = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            client.send(
              JSON.stringify({
                type: "event",
                event: "session.message",
                payload: {
                  sessionKey: "agent:main:test-session",
                  messageSeq: 7,
                  message: {
                    role: "assistant",
                    content: [
                      { type: "thinking", thinking: "\nLet meLet me search the web." },
                      { type: "text", text: "Visible answer" }
                    ],
                    timestamp: Date.now()
                  }
                }
              })
            );
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          content: String((event.payload as { content?: string }).content ?? "")
        }),
      onDisconnect: () => {}
    });

    await eventually(() =>
      expect(received).toEqual([
        {
          kind: "assistant.thinking",
          content: "\nLet me search the web."
        },
        {
          kind: "assistant.message",
          content: "Visible answer"
        }
      ])
    );
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("sanitizes thinking signals when raw thinking is disabled", async () => {
    const received: Array<{
      kind: string;
      content: string;
      privateContentOmitted?: unknown;
      textLength?: unknown;
    }> = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            client.send(
              JSON.stringify({
                type: "event",
                event: "session.message",
                payload: {
                  sessionKey: "agent:main:test-session",
                  messageSeq: 7,
                  message: {
                    role: "assistant",
                    content: [
                      { type: "thinking", thinking: "private reasoning that should never be rendered" },
                      { type: "text", text: "Visible answer" }
                    ],
                    timestamp: Date.now()
                  }
                }
              })
            );
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token",
      exposeRawThinking: false
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          content: String((event.payload as { content?: string }).content ?? ""),
          privateContentOmitted: (event.payload as { privateContentOmitted?: unknown }).privateContentOmitted,
          textLength: (event.payload as { textLength?: unknown }).textLength
        }),
      onDisconnect: () => {}
    });

    await eventually(() => {
      expect(received).toEqual([
        {
          kind: "assistant.thinking",
          content: "Claw emitted a private thinking update. Raw reasoning is hidden.",
          privateContentOmitted: true,
          textLength: "private reasoning that should never be rendered".length
        },
        {
          kind: "assistant.message",
          content: "Visible answer",
          privateContentOmitted: undefined,
          textLength: undefined
        }
      ]);
      expect(JSON.stringify(received)).not.toContain("private reasoning");
    });
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("exposes raw model thinking by default", async () => {
    const received: Array<{
      kind: string;
      content: string;
      privateContentOmitted?: unknown;
      rawThinkingVisible?: unknown;
      textLength?: unknown;
    }> = [];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            client.send(
              JSON.stringify({
                type: "event",
                event: "session.message",
                payload: {
                  sessionKey: "agent:main:test-session",
                  messageSeq: 7,
                  message: {
                    role: "assistant",
                    content: [
                      { type: "thinking", thinking: "visible model reasoning from OpenClaw" },
                      { type: "text", text: "Visible answer" }
                    ],
                    timestamp: Date.now()
                  }
                }
              })
            );
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) =>
        received.push({
          kind: event.kind,
          content: String((event.payload as { content?: string }).content ?? ""),
          privateContentOmitted: (event.payload as { privateContentOmitted?: unknown }).privateContentOmitted,
          rawThinkingVisible: (event.payload as { rawThinkingVisible?: unknown }).rawThinkingVisible,
          textLength: (event.payload as { textLength?: unknown }).textLength
        }),
      onDisconnect: () => {}
    });

    await eventually(() => {
      expect(received).toEqual([
        {
          kind: "assistant.thinking",
          content: "visible model reasoning from OpenClaw",
          privateContentOmitted: false,
          rawThinkingVisible: true,
          textLength: "visible model reasoning from OpenClaw".length
        },
        {
          kind: "assistant.message",
          content: "Visible answer",
          privateContentOmitted: undefined,
          rawThinkingVisible: undefined,
          textLength: undefined
        }
      ]);
    });
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("splits a single large gateway delta into smaller local streaming chunks", async () => {
    const received: string[] = [];
    const longText = Array.from({ length: 20 }, (_, index) => `第 ${index + 1} 句用于验证流式输出。`).join("");
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(
          JSON.stringify({
            type: "event",
            event: "connect.challenge",
            payload: { nonce: "nonce-1" }
          })
        );

        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as {
            id: string;
            method: string;
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));

          if (frame.method === "sessions.messages.subscribe") {
            client.send(
              JSON.stringify({
                type: "event",
                event: "chat",
                payload: {
                  sessionKey: "agent:main:test-session",
                  runId: "run-1",
                  state: "delta",
                  seq: 1,
                  deltaText: longText,
                  message: {
                    role: "assistant",
                    content: [{ type: "text", text: longText }],
                    timestamp: Date.now()
                  }
                }
              })
            );
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token"
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) => received.push(String((event.payload as { content?: string }).content ?? "")),
      onDisconnect: () => {}
    });

    await eventually(() => {
      expect(received.length).toBeGreaterThan(1);
      expect(received.join("")).toBe(longText);
    });
    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });
});

function listen(server: ReturnType<typeof createServer>): Promise<string> {
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("expected TCP address");
      }
      resolve(`http://${address.address}:${address.port}`);
    });
  });
}

async function eventually(assertion: () => void) {
  const deadline = Date.now() + 1_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  throw lastError;
}
