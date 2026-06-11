import { request } from "node:http";

export type WorkBuddyInteractionControlPayload = Record<string, unknown> & {
  action?: string;
  session_id?: string;
  sessionId?: string;
  conversation_id?: string;
  interaction_id?: string;
  request_id?: string;
  tool_call_id?: string;
  toolCallId?: string;
  question_id?: string;
  method?: string;
  type?: string;
  decision?: string;
  answer?: unknown;
  answers?: Record<string, unknown>;
};

export type WorkBuddyInteractionControlAttempt = {
  endpoint: string;
  method: string;
  ok: boolean;
  error?: string;
};

export type WorkBuddyInteractionControlResult = {
  ok: boolean;
  action: string;
  session_id: string;
  conversation_id: string;
  submitted: boolean;
  endpoint?: string;
  attempts: WorkBuddyInteractionControlAttempt[];
};

export type SubmitWorkBuddyInteractionInput = {
  sessionId: string;
  payload: WorkBuddyInteractionControlPayload;
  apiBaseUrls: string[];
  timeoutMs?: number;
};

const DEFAULT_ACP_TIMEOUT_MS = 5_000;

export async function submitWorkBuddyInteraction(
  input: SubmitWorkBuddyInteractionInput
): Promise<WorkBuddyInteractionControlResult> {
  const sessionId = readString(input.payload.session_id) ||
    readString(input.payload.sessionId) ||
    input.sessionId;
  const action = readString(input.payload.action) || "respond_interruption";
  const endpoints = uniqueStrings(input.apiBaseUrls.map(normalizeEndpoint).filter(Boolean));
  const attempts: WorkBuddyInteractionControlAttempt[] = [];

  if (!sessionId) {
    throw new Error("WorkBuddy session_id is required");
  }
  if (!endpoints.length) {
    throw new Error("No WorkBuddy ACP endpoint is available");
  }

  for (const endpoint of endpoints) {
    try {
      const connection = await acpConnect(endpoint, input.timeoutMs);
      await acpRequest(endpoint, connection, "initialize", {
        protocolVersion: 1,
        clientInfo: {
          name: "53aihub-workbuddy-channel",
          version: "0.1.19"
        },
        clientCapabilities: {
          _meta: {
            "codebuddy.ai": {
              question: true,
              promptSuggestion: true
            }
          }
        }
      }, input.timeoutMs);

      await acpRequest(endpoint, connection, "session/load", {
        sessionId,
        cwd: ".",
        mcpServers: []
      }, input.timeoutMs).catch((error) => {
        attempts.push({
          endpoint,
          method: "session/load",
          ok: false,
          error: normalizeErrorMessage(error)
        });
      });

      const isPermission = isPermissionPayload(input.payload);
      if (!isPermission) {
        const submitted = await trySubmitQuestionAnswer(endpoint, connection, input.payload, attempts, input.timeoutMs);
        const resolved = await tryResolveInterruption(endpoint, connection, sessionId, input.payload, attempts, input.timeoutMs);
        if (submitted || resolved) {
          return buildResult(action, sessionId, endpoint, attempts);
        }
      }

      const resolved = await tryResolveInterruption(endpoint, connection, sessionId, input.payload, attempts, input.timeoutMs);
      if (resolved) {
        return buildResult(action, sessionId, endpoint, attempts);
      }

      if (isPermission) {
        const submitted = await trySubmitQuestionAnswer(endpoint, connection, input.payload, attempts, input.timeoutMs);
        if (submitted) {
          return buildResult(action, sessionId, endpoint, attempts);
        }
      }
    } catch (error) {
      attempts.push({
        endpoint,
        method: "acp/connect",
        ok: false,
        error: normalizeErrorMessage(error)
      });
    }
  }

  const lastError = [...attempts].reverse().find((attempt) => !attempt.ok)?.error;
  throw new Error(lastError || "WorkBuddy interaction submit failed");
}

async function tryResolveInterruption(
  endpoint: string,
  connection: WorkBuddyAcpConnection,
  sessionId: string,
  payload: WorkBuddyInteractionControlPayload,
  attempts: WorkBuddyInteractionControlAttempt[],
  timeoutMs?: number
) {
  const toolCallId = readString(payload.tool_call_id) || readString(payload.toolCallId);
  if (!toolCallId) {
    return false;
  }

  const decision = normalizeDecision(readString(payload.decision) || readString(payload.answer));
  const answers = normalizeAnswers(payload);
  try {
    await acpRequest(endpoint, connection, "_codebuddy.ai/resolveInterruption", {
      sessionId,
      toolCallId,
      decision,
      ...(Object.keys(answers).length ? { answers } : {})
    }, timeoutMs);
    attempts.push({ endpoint, method: "_codebuddy.ai/resolveInterruption", ok: true });
    return true;
  } catch (error) {
    attempts.push({
      endpoint,
      method: "_codebuddy.ai/resolveInterruption",
      ok: false,
      error: normalizeErrorMessage(error)
    });
    return false;
  }
}

async function trySubmitQuestionAnswer(
  endpoint: string,
  connection: WorkBuddyAcpConnection,
  payload: WorkBuddyInteractionControlPayload,
  attempts: WorkBuddyInteractionControlAttempt[],
  timeoutMs?: number
) {
  const requestId = readString(payload.request_id) || readString(payload.interaction_id);
  if (!requestId) {
    return false;
  }

  const result = isPermissionPayload(payload)
    ? {
        outcome: {
          outcome: "selected",
          optionId: mapDecisionToOptionId(readString(payload.decision) || readString(payload.answer))
        }
      }
    : {
        outcome: "submitted",
        answers: normalizeAnswers(payload)
      };

  try {
    await acpJsonRpcResult(endpoint, connection, requestId, result, timeoutMs);
    attempts.push({ endpoint, method: "jsonrpc/result", ok: true });
    return true;
  } catch (error) {
    attempts.push({
      endpoint,
      method: "jsonrpc/result",
      ok: false,
      error: normalizeErrorMessage(error)
    });
    return false;
  }
}

type WorkBuddyAcpConnection = {
  connectionId: string;
  sessionToken?: string;
  nextId: number;
};

async function acpConnect(endpoint: string, timeoutMs?: number): Promise<WorkBuddyAcpConnection> {
  const parsed = await httpJson(endpoint, "/api/v1/acp/connect", undefined, {
    Accept: "application/json"
  }, timeoutMs);
  const connectionId = readString(parsed.connectionId);
  if (!connectionId) {
    throw new Error("ACP connect did not return connectionId");
  }
  return {
    connectionId,
    sessionToken: readString(parsed.sessionToken) || undefined,
    nextId: 1
  };
}

async function acpRequest(
  endpoint: string,
  connection: WorkBuddyAcpConnection,
  method: string,
  params: Record<string, unknown>,
  timeoutMs?: number
): Promise<Record<string, unknown>> {
  const id = connection.nextId++;
  const raw = await httpPost(endpoint, "/api/v1/acp", {
    jsonrpc: "2.0",
    id,
    method,
    params
  }, acpHeaders(connection), timeoutMs);
  const response = parseAcpResponse(raw, id);
  if (response.error) {
    throw new Error(readString(toRecord(response.error).message) || JSON.stringify(response.error));
  }
  return toRecord(response.result);
}

async function acpJsonRpcResult(
  endpoint: string,
  connection: WorkBuddyAcpConnection,
  requestId: string,
  result: Record<string, unknown>,
  timeoutMs?: number
): Promise<void> {
  await httpPost(endpoint, "/api/v1/acp", {
    jsonrpc: "2.0",
    id: coerceJsonRpcId(requestId),
    result
  }, acpHeaders(connection), timeoutMs);
}

function acpHeaders(connection: WorkBuddyAcpConnection): Record<string, string> {
  return {
    Accept: "application/json, text/event-stream",
    "Content-Type": "application/json",
    "acp-connection-id": connection.connectionId,
    ...(connection.sessionToken ? { "acp-session-token": connection.sessionToken } : {})
  };
}

function parseAcpResponse(raw: string, id: string | number): Record<string, unknown> {
  const sseMessages = parseSseMessages(raw);
  const sseResponse = sseMessages.find((message) => message.id === id || String(message.id) === String(id));
  if (sseResponse) {
    return sseResponse;
  }
  const json = parseJsonObject(raw);
  if (Object.keys(json).length) {
    return json;
  }
  throw new Error(`ACP response not found for request ${id}`);
}

async function httpJson(
  endpoint: string,
  path: string,
  body: unknown,
  headers: Record<string, string>,
  timeoutMs?: number
): Promise<Record<string, unknown>> {
  const raw = await httpPost(endpoint, path, body, headers, timeoutMs);
  const parsed = parseJsonObject(raw);
  if (!Object.keys(parsed).length) {
    throw new Error(`invalid JSON response: ${raw.slice(0, 200)}`);
  }
  return parsed;
}

function httpPost(
  endpoint: string,
  path: string,
  body: unknown,
  headers: Record<string, string>,
  timeoutMs = DEFAULT_ACP_TIMEOUT_MS
): Promise<string> {
  const baseUrl = new URL(endpoint);
  const payload = body === undefined ? "" : JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = request(
      {
        hostname: baseUrl.hostname,
        port: Number(baseUrl.port),
        path,
        method: "POST",
        headers: {
          "x-codebuddy-request": "1",
          ...(payload ? { "Content-Length": String(Buffer.byteLength(payload)) } : {}),
          ...headers
        },
        timeout: timeoutMs
      },
      (res) => {
        let raw = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          raw += chunk;
        });
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve(raw);
            return;
          }
          reject(new Error(`ACP HTTP ${res.statusCode}: ${raw.slice(0, 200)}`));
        });
      }
    );
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy(new Error(`ACP request timed out: ${path}`));
    });
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

function parseSseMessages(raw: string): Array<Record<string, unknown>> {
  const messages: Array<Record<string, unknown>> = [];
  for (const line of raw.split(/\r?\n/)) {
    if (!line.startsWith("data:")) {
      continue;
    }
    const data = line.slice(5).trim();
    if (!data) {
      continue;
    }
    const parsed = parseJsonObject(data);
    if (Object.keys(parsed).length) {
      messages.push(parsed);
    }
  }
  return messages;
}

function parseJsonObject(raw: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(raw || "{}");
    return toRecord(parsed);
  } catch {
    return {};
  }
}

function normalizeAnswers(payload: WorkBuddyInteractionControlPayload): Record<string, unknown> {
  if (payload.answers && typeof payload.answers === "object" && !Array.isArray(payload.answers)) {
    return payload.answers;
  }
  const questionId = readString(payload.question_id) || readString(payload.interaction_id) || "answer";
  const answer = payload.answer ?? payload.decision ?? payload.option_id ?? "";
  return { [questionId]: answer };
}

function normalizeDecision(value: string): string {
  const normalized = value.trim();
  switch (normalized) {
    case "allow_always":
    case "allowAll":
      return "allowAll";
    case "allow_once":
    case "allow":
      return "allow";
    case "reject_and_exit_plan":
    case "rejectAndExitPlan":
      return "rejectAndExitPlan";
    case "reject":
    case "deny":
    default:
      return normalized || "deny";
  }
}

function mapDecisionToOptionId(value: string): string {
  const decision = normalizeDecision(value);
  switch (decision) {
    case "allowAll":
      return "allow_always";
    case "allow":
      return "allow";
    case "rejectAndExitPlan":
      return "reject_and_exit_plan";
    default:
      return "reject";
  }
}

function isPermissionPayload(payload: WorkBuddyInteractionControlPayload): boolean {
  const method = readString(payload.method).toLowerCase();
  const type = readString(payload.type).toLowerCase();
  return method === "session/request_permission" || type.includes("permission");
}

function buildResult(
  action: string,
  sessionId: string,
  endpoint: string,
  attempts: WorkBuddyInteractionControlAttempt[]
): WorkBuddyInteractionControlResult {
  return {
    ok: true,
    action,
    session_id: sessionId,
    conversation_id: sessionId,
    submitted: true,
    endpoint,
    attempts
  };
}

function coerceJsonRpcId(value: string): string | number {
  const numeric = Number(value);
  return Number.isInteger(numeric) && String(numeric) === value ? numeric : value;
}

function normalizeEndpoint(value: string): string {
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "http:") {
      return "";
    }
    if (!["127.0.0.1", "localhost", "[::1]"].includes(parsed.hostname)) {
      return "";
    }
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return "";
  }
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

function readString(value: unknown): string {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return "";
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function normalizeErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
