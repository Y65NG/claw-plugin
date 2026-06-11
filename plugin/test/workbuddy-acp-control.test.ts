import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import { afterEach, describe, expect, it } from "vitest";

import { submitWorkBuddyInteraction } from "../src/workbuddy-acp-control";

const cleanupServers: Server[] = [];

afterEach(async () => {
  await Promise.all(cleanupServers.splice(0).map(closeServer));
});

describe("WorkBuddy ACP control adapter", () => {
  it("resolves permission interruptions through WorkBuddy extMethod", async () => {
    const api = await createFakeAcpServer();
    cleanupServers.push(api.server);

    const result = await submitWorkBuddyInteraction({
      sessionId: "session-1",
      apiBaseUrls: [api.url],
      payload: {
        action: "respond_interruption",
        session_id: "session-1",
        method: "session/request_permission",
        tool_call_id: "tool-1",
        decision: "allow",
        answers: { permission: "allow" }
      },
      timeoutMs: 500
    });

    expect(result).toMatchObject({
      ok: true,
      submitted: true,
      action: "respond_interruption",
      session_id: "session-1",
      endpoint: api.url
    });
    expect(api.calls.map((call) => call.method)).toEqual([
      "initialize",
      "session/load",
      "_codebuddy.ai/resolveInterruption"
    ]);
    expect(api.calls.at(-1)?.params).toMatchObject({
      sessionId: "session-1",
      toolCallId: "tool-1",
      decision: "allow",
      answers: { permission: "allow" }
    });
  });

  it("submits WorkBuddy question answers as JSON-RPC results when request id is available", async () => {
    const api = await createFakeAcpServer();
    cleanupServers.push(api.server);

    const result = await submitWorkBuddyInteraction({
      sessionId: "session-1",
      apiBaseUrls: [api.url],
      payload: {
        action: "submit_answer",
        session_id: "session-1",
        method: "_codebuddy.ai/question",
        interaction_id: "42",
        question_id: "choice",
        answer: "blue",
        answers: { choice: "blue" }
      },
      timeoutMs: 500
    });

    expect(result).toMatchObject({
      ok: true,
      submitted: true,
      action: "submit_answer",
      session_id: "session-1"
    });
    expect(api.results).toEqual([
      {
        jsonrpc: "2.0",
        id: 42,
        result: {
          outcome: "submitted",
          answers: { choice: "blue" }
        }
      }
    ]);
  });
});

async function createFakeAcpServer() {
  const calls: Array<{ method: string; params: Record<string, unknown> }> = [];
  const results: Array<Record<string, unknown>> = [];
  const server = createServer(async (req, res) => {
    if (req.method === "POST" && req.url === "/api/v1/acp/connect") {
      writeJson(res, {
        connectionId: "connection-1",
        sessionToken: "session-token-1"
      });
      return;
    }

    if (req.method === "POST" && req.url === "/api/v1/acp") {
      const body = await readJson(req);
      if (body.method) {
        calls.push({
          method: String(body.method),
          params: toRecord(body.params)
        });
        writeSse(res, {
          jsonrpc: "2.0",
          id: body.id,
          result: {}
        });
        return;
      }
      if (body.result) {
        results.push(body);
        writeJson(res, {});
        return;
      }
    }

    res.writeHead(404);
    res.end();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("server did not bind to a TCP port");
  }
  return {
    server,
    url: `http://127.0.0.1:${address.port}`,
    calls,
    results
  };
}

function writeJson(res: ServerResponse, value: unknown) {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(value));
}

function writeSse(res: ServerResponse, value: unknown) {
  res.writeHead(200, { "Content-Type": "text/event-stream" });
  res.end(`data: ${JSON.stringify(value)}\n\n`);
}

function readJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      raw += chunk;
    });
    req.on("end", () => {
      try {
        resolve(toRecord(JSON.parse(raw || "{}")));
      } catch (error) {
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}
