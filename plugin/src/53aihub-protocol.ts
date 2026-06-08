import { randomUUID } from "node:crypto";

export type Hub53AIBaseConfig = {
  botId: string;
  secret: string;
  wsUrl: string;
  accessPolicy: "open" | "allowlist";
  allowFrom: string[];
  sendThinkingMessage: boolean;
};

export type Hub53AIIncomingMessage = {
  type: string;
  msgId: string;
  reqId: string;
  chatId: string;
  userId: string;
  userName?: string;
  text: string;
  imageUrls?: string[];
  fileUrls?: string[];
  quoteContent?: string;
  conversationTitle?: string;
};

export type Hub53AIOutgoingChunk = {
  req_id: string;
  action: "chat";
  status: "streaming" | "thinking" | "done" | "error";
  data: {
    id: string;
    object: "chat.completion.chunk";
    created: number;
    model: "openclaw-agent";
    status?: "streaming" | "thinking" | "done" | "error";
    mode?: string;
    replace?: boolean;
    event_kind?: string;
    payload?: Record<string, unknown>;
    session_id?: string;
    conversation_id?: string;
    choices: Array<{
      index: number;
      delta: {
        content: string;
        role: "assistant";
      };
      finish_reason: "stop" | "error" | null;
    }>;
    error?: {
      code: string;
      message: string;
      details?: string;
    };
  };
};

export type Hub53AIRPCRequest = {
  reqId: string;
  action: string;
  data: unknown;
};

export type Hub53AIRPCFrame = {
  req_id: string;
  action: string;
  status: "done" | "error";
  data: unknown;
};

export const DEFAULT_HUB53AI_THINKING_MESSAGE = "正在处理您的请求...";

export function parseIncomingMessage(rawJson: string): Hub53AIIncomingMessage | null {
  try {
    const wsMsg = JSON.parse(rawJson) as Record<string, any>;
    if (wsMsg.action === "ping" || wsMsg.action === "pong") {
      return null;
    }

    if (wsMsg.action === "chat") {
      const openAIReq = toRecord(wsMsg.data);
      const metadata = toRecord(openAIReq.metadata);
      const messages = Array.isArray(openAIReq.messages) ? openAIReq.messages : [];
      const lastUserMsg = [...messages].reverse().find((message) => toRecord(message).role === "user");
      if (!lastUserMsg) {
        return null;
      }
      const userMessage = toRecord(lastUserMsg);
      const content = userMessage.content;
      const userObject = toRecord(openAIReq.user);
      const userId = stringOr(
        openAIReq.user,
        userObject.id,
        userObject.userId,
        userMessage.userId,
        userMessage.name,
        `user-${String(wsMsg.req_id ?? randomUUID())}`
      );
      const chatId = stringOr(openAIReq.conversation_id, userId);
      return {
        type: "message",
        msgId: String(wsMsg.req_id ?? randomUUID()),
        reqId: String(wsMsg.req_id ?? randomUUID()),
        chatId,
        userId,
        userName: extractUserName(openAIReq, metadata, userObject, userMessage),
        conversationTitle: extractConversationTitle(openAIReq, metadata),
        text: extractTextFromContent(content),
        imageUrls: extractImagesFromContent(content),
        fileUrls: extractFilesFromContent(content)
      };
    }

    if (typeof wsMsg.status === "string" && wsMsg.status !== "request") {
      return null;
    }

    const data = toRecord(wsMsg.data);
    const userObject = toRecord(data.user);
    const chatId = stringOr(data.chatId, data.userId, "default-chat");
    const userId = stringOr(data.userId, userObject.id, userObject.userId, data.chatId, "default-user");
    return {
      type: stringOr(data.type, "message"),
      msgId: stringOr(data.msgId, data.id, `msg-${Date.now()}`),
      reqId: String(wsMsg.req_id ?? data.msgId ?? data.id ?? `msg-${Date.now()}`),
      chatId,
      userId,
      userName: extractUserName(data, userObject),
      conversationTitle: extractConversationTitle(data),
      text: stringOr(data.text, data.content, ""),
      imageUrls: normalizeUrlList(data.imageUrls, data.images),
      fileUrls: normalizeUrlList(data.fileUrls, data.files),
      quoteContent: typeof data.quoteContent === "string" ? data.quoteContent : undefined
    };
  } catch {
    return null;
  }
}

export function buildHub53AIPrompt(message: Hub53AIIncomingMessage): string {
  const parts = [message.text.trim()].filter(Boolean);
  if (message.imageUrls?.length) {
    parts.push(`Images:\n${message.imageUrls.join("\n")}`);
  }
  if (message.fileUrls?.length) {
    parts.push(`Files:\n${message.fileUrls.join("\n")}`);
  }
  return parts.join("\n\n");
}

export function buildHub53AIOutgoingChunk(
  reqId: string,
  text: string,
  status: Hub53AIOutgoingChunk["status"],
  error?: Hub53AIOutgoingChunk["data"]["error"],
  sessionId?: string,
  metadata?: {
    mode?: string;
    replace?: boolean;
    eventKind?: string;
    payload?: Record<string, unknown>;
  }
): Hub53AIOutgoingChunk {
  return {
    req_id: reqId,
    action: "chat",
    status,
    data: {
      id: reqId,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: "openclaw-agent",
      status,
      ...(metadata?.mode ? { mode: metadata.mode } : {}),
      ...(typeof metadata?.replace === "boolean" ? { replace: metadata.replace } : {}),
      ...(metadata?.eventKind ? { event_kind: metadata.eventKind } : {}),
      ...(metadata?.payload ? { payload: metadata.payload } : {}),
      ...(sessionId ? { session_id: sessionId, conversation_id: sessionId } : {}),
      choices: [
        {
          index: 0,
          delta: {
            content: text,
            role: "assistant"
          },
          finish_reason: status === "done" ? "stop" : status === "error" ? "error" : null
        }
      ],
      ...(error ? { error } : {})
    }
  };
}

export function createHub53AIAuthHeaders(config: Pick<Hub53AIBaseConfig, "botId" | "secret">): Record<string, string> {
  const authBase64 = Buffer.from(`${config.botId}:${config.secret}`).toString("base64");
  return {
    Authorization: `Bearer ${config.secret}`,
    "Proxy-Authorization": `Basic ${authBase64}`,
    "X-Bot-Id": config.botId,
    "X-Api-Key": config.secret
  };
}

export function checkHub53AIAccessPolicy(
  config: Pick<Hub53AIBaseConfig, "accessPolicy" | "allowFrom">,
  message: Hub53AIIncomingMessage
): { allowed: boolean; reason: string } {
  if (config.accessPolicy === "open") {
    return { allowed: true, reason: "" };
  }
  const allowed = new Set(config.allowFrom);
  const candidates = [
    message.userId,
    `user:${message.userId}`,
    `53aihub:${message.userId}`,
    message.chatId,
    `chat:${message.chatId}`,
    `53aihub:${message.chatId}`
  ];
  if (candidates.some((candidate) => allowed.has(candidate))) {
    return { allowed: true, reason: "" };
  }
  return { allowed: false, reason: "user is not in allowlist" };
}

export function parseHub53AIHeartbeat(rawPayload: string): "ping" | "pong" | null {
  try {
    const parsed = JSON.parse(rawPayload) as { action?: unknown };
    if (parsed.action === "ping" || parsed.action === "pong") {
      return parsed.action;
    }
  } catch {
    return null;
  }
  return null;
}

export function parseHub53AIRPCRequest(rawPayload: string): Hub53AIRPCRequest | null {
  try {
    const parsed = JSON.parse(rawPayload) as Record<string, unknown>;
    if (parsed.status !== "request") {
      return null;
    }
    if (typeof parsed.req_id !== "string" || !parsed.req_id.trim()) {
      return null;
    }
    if (typeof parsed.action !== "string" || !parsed.action.trim()) {
      return null;
    }
    return {
      reqId: parsed.req_id.trim(),
      action: parsed.action.trim(),
      data: parsed.data
    };
  } catch {
    return null;
  }
}

export function buildHub53AIRPCFrame(
  request: Hub53AIRPCRequest,
  status: Hub53AIRPCFrame["status"],
  data: unknown
): Hub53AIRPCFrame {
  return {
    req_id: request.reqId,
    action: request.action,
    status,
    data
  };
}

export function validateHub53AIConfig(config: Pick<Hub53AIBaseConfig, "botId" | "secret" | "wsUrl">) {
  if (!config.botId) {
    throw new Error("hub53ai.botId is required");
  }
  if (!config.secret) {
    throw new Error("hub53ai.secret is required");
  }
  if (!config.wsUrl) {
    throw new Error("hub53ai.wsUrl is required");
  }
  if (!config.wsUrl.startsWith("ws://") && !config.wsUrl.startsWith("wss://")) {
    throw new Error("hub53ai.wsUrl must start with ws:// or wss://");
  }
}

export function inferHub53AIErrorCode(errorText: string): string {
  const lower = errorText.toLowerCase();
  if (lower.includes("timeout")) {
    return "TIMEOUT";
  }
  if (lower.includes("rate limit")) {
    return "RATE_LIMITED";
  }
  if (lower.includes("quota")) {
    return "INSUFFICIENT_QUOTA";
  }
  if (lower.includes("unauthorized") || lower.includes("access denied")) {
    return "ACCESS_DENIED";
  }
  if (lower.includes("websocket")) {
    return "WEBSOCKET_ERROR";
  }
  return "INTERNAL_ERROR";
}

export function sanitizeHub53AIWsUrl(wsUrl: string): string | undefined {
  if (!wsUrl) {
    return undefined;
  }
  try {
    const url = new URL(wsUrl);
    url.username = "";
    url.password = "";
    url.search = "";
    return url.toString();
  } catch {
    return wsUrl;
  }
}

export function maskHub53AIBotId(botId: string): string | undefined {
  if (!botId) {
    return undefined;
  }
  if (botId.length <= 4) {
    return `${botId.slice(0, 1)}***`;
  }
  return `${botId.slice(0, 2)}***${botId.slice(-2)}`;
}

function extractUserName(...sources: unknown[]): string | undefined {
  const nameKeys = [
    "userName",
    "username",
    "nickName",
    "nickname",
    "displayName",
    "senderName",
    "fromUserName",
    "name"
  ];

  for (const source of sources) {
    const record = toRecord(source);
    const direct = stringFromKeys(record, nameKeys);
    if (direct) {
      return direct;
    }

    const nestedUser = toRecord(record.user);
    const nestedName = stringFromKeys(nestedUser, nameKeys);
    if (nestedName) {
      return nestedName;
    }
  }

  return undefined;
}

function extractConversationTitle(...sources: unknown[]): string | undefined {
  const titleKeys = [
    "openclaw_conversation_title",
    "openclawConversationTitle",
    "conversation_title",
    "conversationTitle",
    "title"
  ];
  for (const source of sources) {
    const record = toRecord(source);
    const title = stringFromKeys(record, titleKeys);
    if (title) {
      return title;
    }
  }
  return undefined;
}

function stringFromKeys(record: Record<string, any>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function extractTextFromContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((item) => {
      const record = toRecord(item);
      if (record.type === "text" && typeof record.text === "string") {
        return record.text;
      }
      return "";
    })
    .filter(Boolean)
    .join("\n");
}

function extractImagesFromContent(content: unknown): string[] {
  if (!Array.isArray(content)) {
    return [];
  }
  return normalizeUrlList(
    undefined,
    content
      .map((item) => {
        const record = toRecord(item);
        if (record.type === "image_url") {
          return toRecord(record.image_url).url;
        }
        if (record.type === "image") {
          return record.url ?? toRecord(record.image).url;
        }
        return undefined;
      })
      .filter(Boolean)
  );
}

function extractFilesFromContent(content: unknown): string[] {
  if (!Array.isArray(content)) {
    return [];
  }
  return normalizeUrlList(
    undefined,
    content
      .map((item) => {
        const record = toRecord(item);
        if (record.type === "file") {
          return record.url ?? toRecord(record.file).url;
        }
        return undefined;
      })
      .filter(Boolean)
  );
}

function normalizeUrlList(primary: unknown, fallback: unknown): string[] {
  const source = Array.isArray(primary) ? primary : Array.isArray(fallback) ? fallback : [];
  return source
    .map((entry) => {
      if (typeof entry === "string") {
        return entry;
      }
      const record = toRecord(entry);
      return typeof record.url === "string" ? record.url : "";
    })
    .filter(Boolean);
}

function stringOr(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}
