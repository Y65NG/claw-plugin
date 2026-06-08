import { afterEach, describe, expect, it } from "vitest";
import { createServer } from "node:http";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { WebSocketServer } from "ws";

import { createGatewayClient } from "../src/gateway-client";

const servers = new Set<ReturnType<typeof createServer>>();
const tempDirs = new Set<string>();

afterEach(async () => {
  await Promise.all(
    Array.from(servers, (server) => new Promise<void>((resolve) => server.close(() => resolve())))
  );
  servers.clear();
  for (const dir of tempDirs) {
    rmSync(dir, { recursive: true, force: true });
  }
  tempDirs.clear();
  delete (globalThis as Record<string, unknown>).__officialGatewayClientRequests;
  delete (globalThis as Record<string, unknown>).__officialGatewayClientOptions;
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
    expect(capturedMinProtocol).toBe(3);
    expect(capturedMaxProtocol).toBe(4);
    expect(session.id).toBe("agent:main:test-session");
    expect(session.title).toBe("Gateway session");
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("retries the websocket handshake with the expected Gateway protocol when the peer requires protocol 3", async () => {
    const capturedProtocols: Array<{ minProtocol: number; maxProtocol: number }> = [];
    let capturedMethod = "";

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
            capturedProtocols.push({
              minProtocol: Number(frame.params?.minProtocol ?? 0),
              maxProtocol: Number(frame.params?.maxProtocol ?? 0)
            });
            if (capturedProtocols.length === 1) {
              client.send(
                JSON.stringify({
                  type: "res",
                  id: frame.id,
                  ok: false,
                  error: {
                    message: 'network error: protocol mismatch: {"expectedProtocol":3}'
                  }
                })
              );
              return;
            }
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 3 } }));
            return;
          }

          capturedMethod = frame.method;
          client.send(
            JSON.stringify({
              type: "res",
              id: frame.id,
              ok: true,
              payload: {
                sessions: [],
                total: 0,
                hasMore: false
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

    await expect(client.listSessions()).resolves.toEqual([]);

    expect(capturedProtocols).toEqual([
      { minProtocol: 3, maxProtocol: 4 },
      { minProtocol: 3, maxProtocol: 3 }
    ]);
    expect(capturedMethod).toBe("sessions.list");
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("rejects an unsupported expected Gateway protocol without retrying outside the supported range", async () => {
    let handshakeAttempts = 0;

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
            handshakeAttempts += 1;
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: false,
                error: {
                  message: "protocol mismatch",
                  details: {
                    expectedProtocol: 5
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
      requestTimeoutMs: 200
    });

    await expect(client.listSessions()).rejects.toThrow("Gateway protocol 5 is not supported");
    expect(handshakeAttempts).toBe(1);
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("keeps using the official GatewayClient transport for QClaw's default 28789 gateway when available", async () => {
    const runtimeRoot = mkdtempSync(join(tmpdir(), "claw-official-gateway-"));
    tempDirs.add(runtimeRoot);
    const distDir = join(runtimeRoot, "node_modules", "openclaw", "dist");
    mkdirSync(distDir, { recursive: true });
    writeFileSync(
      join(distDir, "client-mock.js"),
      `
        export class GatewayClient {
          constructor(options) {
            this.options = options;
            globalThis.__officialGatewayClientOptions = options;
          }
          start() {
            this.options.onHelloOk();
          }
          async request(method, params) {
            globalThis.__officialGatewayClientRequests = [
              ...(globalThis.__officialGatewayClientRequests || []),
              { method, params }
            ];
            return { ok: true, key: "official-session", sessionId: "official-session", label: "Official session" };
          }
          stop() {}
        }
      `
    );

    const client = createGatewayClient({
      baseUrl: "ws://127.0.0.1:28789",
      secret: "shared-token",
      runtimeRoot
    });

    const session = await client.createSession("Official session");

    expect(session.id).toBe("official-session");
    expect((globalThis as Record<string, unknown>).__officialGatewayClientOptions).toMatchObject({
      url: "ws://127.0.0.1:28789",
      token: "shared-token"
    });
    expect((globalThis as Record<string, unknown>).__officialGatewayClientRequests).toMatchObject([
      {
        method: "sessions.create",
        params: {
          label: "Official session",
          agentId: "main"
        }
      }
    ]);
    await client.stop();
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

  it("treats chat.abort ACK timeout as a submitted OpenClaw stop", async () => {
    let abortRequested = false;
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
          };

          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }

          if (frame.method === "sessions.messages.subscribe") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));
            return;
          }

          if (frame.method === "chat.abort") {
            abortRequested = true;
            return;
          }

          client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token",
      requestTimeoutMs: 20
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) => received.push(event.kind),
      onDisconnect: () => {}
    });

    await client.controlSession("agent:main:test-session", "stop");

    expect(abortRequested).toBe(true);
    expect(received).toContain("run.interrupted");

    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("rejects OpenClaw stop when chat.abort fails because the gateway disconnects", async () => {
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

          if (frame.method === "chat.abort") {
            client.close();
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token",
      requestTimeoutMs: 200
    });

    await expect(client.controlSession("agent:main:test-session", "stop")).rejects.toThrow();

    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("treats OpenClaw success end events as run completion", async () => {
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
                event: "sessions.changed",
                payload: {
                  sessionKey: "agent:main:test-session",
                  phase: "end",
                  status: "success",
                  runId: "run-1",
                  endedAt: 1779871345389
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
      onEvent: (event) => received.push(event.kind),
      onDisconnect: () => {}
    });

    await eventually(() => {
      expect(received).toContain("run.completed");
    });

    close();
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("maps OpenClaw failed and interrupted end events to terminal run events", async () => {
    const cases = [
      { status: "failed", expected: "run.failed" },
      { status: "timeout", expected: "run.failed" },
      { status: "interrupted", expected: "run.interrupted" }
    ];

    for (const testCase of cases) {
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
                  event: "sessions.changed",
                  payload: {
                    sessionKey: `agent:main:test-${testCase.status}`,
                    phase: "end",
                    status: testCase.status,
                    runId: `run-${testCase.status}`,
                    endedAt: 1779871345389
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

      const close = client.subscribe(`agent:main:test-${testCase.status}`, 0, {
        onEvent: (event) => received.push(event.kind),
        onDisconnect: () => {}
      });

      await eventually(() => {
        expect(received).toContain(testCase.expected);
      });

      close();
      await client.stop();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    }
  });

  it("waits for the OpenClaw terminal end event before resolving stop", async () => {
    const received: string[] = [];
    let abortAcked = false;
    let stopResolved = false;

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

          if (frame.method === "chat.abort") {
            abortAcked = true;
            setTimeout(() => {
              client.send(
                JSON.stringify({
                  type: "event",
                  event: "sessions.changed",
                  payload: {
                    sessionKey: "agent:main:test-session",
                    phase: "end",
                    status: "timeout",
                    runId: "run-timeout",
                    endedAt: Date.now()
                  }
                })
              );
            }, 30);
          }
        });
      });
    });

    const address = await listen(httpServer);
    const client = createGatewayClient({
      baseUrl: address.replace("http://", "ws://"),
      secret: "shared-token",
      requestTimeoutMs: 100
    });

    const close = client.subscribe("agent:main:test-session", 0, {
      onEvent: (event) => received.push(event.kind),
      onDisconnect: () => {}
    });

    const stopPromise = client.controlSession("agent:main:test-session", "stop").then(() => {
      stopResolved = true;
    });

    await eventually(() => {
      expect(abortAcked).toBe(true);
    });
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(stopResolved).toBe(false);

    await stopPromise;
    expect(received).toContain("run.failed");

    close();
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
    let capturedParams: Record<string, unknown> = {};
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
          capturedParams = frame.params ?? {};
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
    expect(capturedParams).toMatchObject({
      sessionKey: "agent:main:test-session",
      message: "hello",
      deliver: false
    });
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

  it("paginates Gateway session lists and can find sessions beyond the first page", async () => {
    const capturedListParams: Array<Record<string, unknown>> = [];
    const sessions = Array.from({ length: 120 }, (_, index) => ({
      key: `session-${index}`,
      label: `Session ${index}`,
      status: "idle",
      startedAt: 1779335000000 + index,
      updatedAt: 1779335000000 + index,
      messageSeq: index
    }));
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
          if (frame.method === "sessions.list") {
            capturedListParams.push(frame.params ?? {});
            const offset = Number(frame.params?.offset ?? 0);
            const limit = Number(frame.params?.limit ?? 50);
            const page = sessions.slice(offset, offset + limit);
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  sessions: page,
                  total: sessions.length,
                  offset,
                  hasMore: offset + page.length < sessions.length,
                  nextOffset: offset + page.length
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

    const allSessions = await client.listSessions(120);
    expect(allSessions).toHaveLength(120);
    expect(allSessions.at(-1)?.id).toBe("session-119");
    const session = await client.getSession("session-119");
    expect(session.title).toBe("Session 119");
    expect(capturedListParams).toMatchObject([
      { limit: 50 },
      { limit: 100 },
      { limit: 120 },
      { limit: 50 },
      { limit: 100 },
      { limit: 150 }
    ]);
    expect(capturedListParams.every((params) => params.offset === undefined)).toBe(true);
    await client.stop();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  });

  it("paginates chat history for messages, event replay, and retry lookup", async () => {
    const capturedHistoryParams: Array<Record<string, unknown>> = [];
    const sentMessages: string[] = [];
    const history = Array.from({ length: 210 }, (_, index) => ({
      role: index === 205 ? "user" : "assistant",
      content: index === 205 ? "retry this old prompt" : `assistant ${index}`,
      timestamp: 1779335000000 + index,
      __openclaw: {
        seq: index + 1
      }
    }));
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
          if (frame.method === "chat.history") {
            capturedHistoryParams.push(frame.params ?? {});
            const limit = Number(frame.params?.limit ?? 200);
            const offset = Math.max(0, history.length - limit);
            const page = history.slice(offset);
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  messages: page,
                  total: history.length,
                  offset,
                  hasMore: limit < history.length,
                  nextOffset: offset + page.length
                }
              })
            );
            return;
          }
          if (frame.method === "chat.send") {
            sentMessages.push(String(frame.params?.message ?? ""));
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { ok: true } }));
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

    await expect(client.getSessionMessages("session-1")).resolves.toHaveLength(210);
    const events = await client.listEvents("session-1", 205);
    expect(events.map((event) => event.seq)).toEqual([207, 208, 209, 210]);
    await client.controlSession("session-1", "retry");
    expect(sentMessages).toEqual(["retry this old prompt"]);
    expect(capturedHistoryParams).toContainEqual(expect.objectContaining({ sessionKey: "session-1", limit: 200 }));
    expect(capturedHistoryParams).toContainEqual(expect.objectContaining({ sessionKey: "session-1", limit: 400 }));
    expect(capturedHistoryParams.every((params) => params.offset === undefined)).toBe(true);
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

  it("synthesizes ordered thinking, tool call, tool result, and final answer events from chat history", async () => {
    const now = Date.now();
    const history = [
      {
        role: "user",
        content: [{ type: "text", text: "find movies" }],
        timestamp: now,
        __openclaw: { seq: 1 }
      },
      {
        role: "assistant",
        content: [
          { type: "thinking", thinking: "Need to search first." },
          {
            type: "toolCall",
            id: "call-1",
            name: "web_search",
            arguments: { query: "movies", count: 5 }
          }
        ],
        timestamp: now + 1,
        __openclaw: { seq: 2 }
      },
      {
        role: "toolResult",
        toolCallId: "call-1",
        toolName: "web_search",
        content: [{ type: "text", text: '{"status":"error","tool":"web_search","error":"fetch failed"}' }],
        timestamp: now + 2,
        __openclaw: { seq: 3 }
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "Final answer" }],
        timestamp: now + 3,
        __openclaw: { seq: 4 }
      }
    ];
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

          if (frame.method === "chat.history") {
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  messages: history,
                  total: history.length,
                  hasMore: false
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

    try {
      const events = await client.listEvents("session-1", 0);

      expect(events.map((event) => event.kind)).toEqual([
        "assistant.thinking",
        "tool.call",
        "tool.result",
        "assistant.message"
      ]);
      expect(events.map((event) => event.seq)).toEqual([20, 21, 30, 40]);
      expect(events.map((event) => event.payload)).toEqual([
        expect.objectContaining({
          content: "Need to search first.",
          mode: "replace",
          replace: true,
          rawSeq: 2
        }),
        expect.objectContaining({
          data: expect.objectContaining({
            phase: "call",
            name: "web_search",
            toolCallId: "call-1",
            args: { query: "movies", count: 5 },
            meta: 'for "movies" (top 5)'
          })
        }),
        expect.objectContaining({
          data: expect.objectContaining({
            phase: "result",
            name: "web_search",
            toolCallId: "call-1",
            isError: true,
            result: expect.objectContaining({
              details: {
                status: "error",
                tool: "web_search",
                error: "fetch failed"
              }
            })
          })
        }),
        {
          content: "Final answer"
        }
      ]);
    } finally {
      await client.stop();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    }
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

  it("synthesizes output_files process steps for QClaw write results in chat history", async () => {
    const previousHome = process.env.HOME;
    const tempHome = mkdtempSync(join(tmpdir(), "qclaw-history-home-"));
    tempDirs.add(tempHome);
    process.env.HOME = tempHome;
    const workspace = join(tempHome, ".qclaw", "workspace");
    mkdirSync(workspace, { recursive: true });
    const outputPath = join(workspace, "chinese_classics.txt");
    writeFileSync(outputPath, "history output");

    const now = Date.now();
    const history = [
      {
        role: "user",
        content: [{ type: "text", text: "write a file" }],
        timestamp: now,
        __openclaw: { seq: 1 }
      },
      {
        role: "assistant",
        content: [
          {
            type: "toolCall",
            id: "call-write",
            name: "write",
            arguments: { path: outputPath, content: "history output" }
          }
        ],
        timestamp: now + 1,
        __openclaw: { seq: 2 }
      },
      {
        role: "toolResult",
        toolCallId: "call-write",
        toolName: "write",
        content: [{ type: "text", text: `Successfully wrote 14 bytes to ${outputPath}` }],
        timestamp: now + 2,
        __openclaw: { seq: 3 }
      },
      {
        role: "assistant",
        content: [{ type: "text", text: `Saved to ${outputPath}` }],
        timestamp: now + 3,
        __openclaw: { seq: 4 }
      }
    ];
    const httpServer = createServer();
    servers.add(httpServer);
    const wss = new WebSocketServer({ noServer: true });

    httpServer.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (client) => {
        client.send(JSON.stringify({ type: "event", event: "connect.challenge", payload: { nonce: "nonce-1" } }));
        client.on("message", (data) => {
          const frame = JSON.parse(data.toString()) as { id: string; method: string };
          if (frame.method === "connect") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { protocol: 4 } }));
            return;
          }
          if (frame.method === "chat.history") {
            client.send(JSON.stringify({ type: "res", id: frame.id, ok: true, payload: { messages: history, total: history.length, hasMore: false } }));
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

    try {
      const events = await client.listEvents("session-1", 0);
      const outputStep = events.find((event) => event.kind === "process.step");
      expect(events.map((event) => event.kind)).toEqual([
        "tool.call",
        "tool.result",
        "process.step",
        "assistant.message"
      ]);
      expect(outputStep?.payload.process_step).toMatchObject({
        step_code: "output_files",
        status: "completed",
        data: {
          files: [
            {
              file_name: "chinese_classics.txt",
              mime_type: "text/plain",
              size: 14,
              base64: Buffer.from("history output").toString("base64")
            }
          ]
        }
      });
    } finally {
      if (previousHome === undefined) {
        delete process.env.HOME;
      } else {
        process.env.HOME = previousHome;
      }
      await client.stop();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    }
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
