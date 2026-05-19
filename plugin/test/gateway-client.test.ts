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
            client.send(
              JSON.stringify({
                type: "res",
                id: frame.id,
                ok: true,
                payload: {
                  type: "hello-ok",
                  protocol: 3
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
    expect(session.id).toBe("agent:main:test-session");
    expect(session.title).toBe("Gateway session");
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
