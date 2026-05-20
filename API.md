# Claw Control Center API

本文档按前端功能组织当前插件的 API。阅读顺序是：

```text
Frontend feature
  -> Local console REST / WebSocket API
  -> Plugin adapter implementation
  -> OpenClaw / QClaw Gateway RPC or event
```

当前插件的前端不是直接访问 OpenClaw Gateway，而是访问插件内置的本地控制台服务。插件服务再通过 OpenClaw/QClaw Gateway WebSocket RPC 与本机 Claw 通信。

官方文档主入口：

- [OpenClaw Gateway Protocol](https://docs.openclaw.ai/gateway/protocol)
- [OpenClaw Gateway Protocol: Transport](https://docs.openclaw.ai/gateway/protocol#transport)
- [OpenClaw Gateway Protocol: Handshake (connect)](https://docs.openclaw.ai/gateway/protocol#handshake-connect)
- [OpenClaw Gateway Protocol: Framing](https://docs.openclaw.ai/gateway/protocol#framing)
- [OpenClaw Gateway Protocol: Session control](https://docs.openclaw.ai/gateway/protocol#session-control)
- [OpenClaw Gateway Protocol: Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families)
- [OpenClaw Gateway Protocol: Models and usage](https://docs.openclaw.ai/gateway/protocol#models-and-usage)
- [OpenClaw Gateway Protocol: Operator helper methods](https://docs.openclaw.ai/gateway/protocol#operator-helper-methods)
- [OpenClaw OpenResponses API](https://docs.openclaw.ai/gateway/openresponses-http-api)
- [OpenClaw Sessions CLI](https://docs.openclaw.ai/cli/sessions)

## 1. Architecture

```text
Browser UI
  -> Local console server
     - REST: /api/*
     - WebSocket: /ws/*
     - Static web assets: web-dist
  -> Gateway adapter
     - plugin/src/gateway-client.ts
  -> OpenClaw/QClaw Gateway
     - WebSocket JSON-RPC
  -> Local Claw runtime
```

关键实现文件：

| Layer | File |
| --- | --- |
| Plugin entry / runtime creation | [plugin/src/index.ts](./plugin/src/index.ts) |
| Local console REST + WebSocket server | [plugin/src/console-server.ts](./plugin/src/console-server.ts) |
| OpenClaw Gateway adapter | [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) |
| Local JSON persistence | [plugin/src/file-store.ts](./plugin/src/file-store.ts) |
| Host config/runtime parsing | [plugin/src/host.ts](./plugin/src/host.ts) |
| 53AIHub bridge | [plugin/src/53aihub-client.ts](./plugin/src/53aihub-client.ts) |
| Frontend API wrapper | [web/src/api.ts](./web/src/api.ts) |
| Frontend UI | [web/src/App.tsx](./web/src/App.tsx) |
| Shared frontend types | [web/src/types.ts](./web/src/types.ts) |
| Shared plugin types | [plugin/src/models.ts](./plugin/src/models.ts) |

## 2. Local Console Auth

写操作需要本地随机 token：

```http
X-Plugin-Token: <token>
```

token 由 `GET /api/bootstrap` 返回。若 token 为空，当前实现允许写操作继续执行，主要用于测试或特殊配置环境。

实现位置：

- Token 生成：[plugin/src/index.ts](./plugin/src/index.ts)
- Token 校验：[plugin/src/console-server.ts](./plugin/src/console-server.ts)
- 前端 header 封装：[web/src/api.ts](./web/src/api.ts)

## 3. Frontend Feature: App Bootstrap

这一组接口用于页面第一次打开时初始化控制台。

### 3.1 `GET /api/bootstrap`

用途：

- 获取本地写操作 token。
- 获取当前插件状态快照。
- 获取脱敏后的插件配置。

Frontend:

- `fetchBootstrap()` in [web/src/api.ts](./web/src/api.ts)
- App initial load in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- `GET /api/bootstrap` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)

OpenClaw upstream:

- 无直接 RPC。此接口组合本地 runtime 状态、插件配置、宿主配置读取结果。

Response example:

```json
{
  "token": "local-plugin-token",
  "status": {
    "hostKind": "openclaw",
    "stateDir": "/Users/me/.openclaw",
    "configPath": "/Users/me/.openclaw/openclaw.json",
    "serviceVersion": "0.1.1",
    "pluginVersion": "0.1.1",
    "port": 4318,
    "pid": 12345,
    "runnerCommand": "gateway",
    "activeSessionCount": 4,
    "runningSessionCount": 1,
    "healthy": true,
    "modelPrimary": "openai/gpt-5.1",
    "enabledSkills": ["browser", "weather"],
    "hub53ai": {
      "enabled": true,
      "configured": true,
      "connectionStatus": "connected",
      "botId": "eW***l8",
      "wsUrl": "ws://kmapirc.53ai.com/api/v1/openclaw/ws/connect",
      "receivedMessageCount": 2,
      "sentMessageCount": 5,
      "pendingOutboundCount": 0
    }
  },
  "config": {
    "gateway": {
      "baseUrl": "ws://127.0.0.1:18789",
      "secret": "[redacted]",
      "requestTimeoutMs": 15000,
      "streamReconnectMs": 2000
    },
    "config": {
      "console": {
        "enabled": true,
        "host": "127.0.0.1",
        "port": 4318
      }
    }
  }
}
```

### 3.2 `GET /api/config`

用途：

- 右侧 Status / Config 面板展示插件配置。
- 所有敏感字段会脱敏，例如 `secret`、`token`、`password`、`key`。

Frontend:

- Bootstrap payload already contains config.
- Debug usage may call `/api/config` directly.

Backend:

- `GET /api/config` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Redaction in [plugin/src/host.ts](./plugin/src/host.ts)

OpenClaw upstream:

- 无直接 RPC。当前实现读取插件启动时已解析的本地配置。
- 如后续改为直接读取 Gateway config，可参考官方 [`config.get`](https://docs.openclaw.ai/gateway/protocol#secrets-config-update-and-wizard)。

## 4. Frontend Feature: Session List

左侧 session list 用于展示所有会话、创建新会话、切换当前会话。

### 4.1 `GET /api/sessions`

用途：

- 拉取本地缓存与远端 Gateway 同步后的会话摘要。
- 前端按 `updatedAt` 默认倒序显示，不再将 running session 强行置顶。

Frontend:

- `fetchSessions()` in [web/src/api.ts](./web/src/api.ts)
- Session list rendering in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- `GET /api/sessions` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Calls `syncRemoteSessions()`

OpenClaw upstream:

| OpenClaw API | Purpose | Official doc |
| --- | --- | --- |
| `sessions.list` | Return current session index. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |

Gateway adapter call:

```ts
transport.request("sessions.list", {
  limit,
  includeGlobal: true,
  includeUnknown: true,
  includeDerivedTitles: true,
  includeLastMessage: true
});
```

Response example:

```json
{
  "sessions": [
    {
      "id": "agent:main:dashboard:759b3a18-b507-4a44-99a0-6a69b90875a6",
      "title": "Research books",
      "status": "completed",
      "hostKind": "openclaw",
      "runnerCommand": "openclaw-gateway",
      "createdAt": "2026-05-20T10:00:00.000Z",
      "updatedAt": "2026-05-20T10:04:00.000Z",
      "lastEventSeq": 57
    }
  ]
}
```

### 4.2 `POST /api/sessions`

用途：

- 创建一个新的 Claw 会话。
- 可选 `initialPrompt`，如果存在会在创建后立即发送第一条消息。

Frontend:

- `createSession(token, title, initialPrompt)` in [web/src/api.ts](./web/src/api.ts)
- New button handler in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- `POST /api/sessions` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)

Auth:

```http
Content-Type: application/json
X-Plugin-Token: <token>
```

Request:

```json
{
  "title": "New research session",
  "initialPrompt": "Optional first prompt"
}
```

OpenClaw upstream:

| OpenClaw API | Purpose | Official doc |
| --- | --- | --- |
| `sessions.create` | Create a new session entry. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.messages.subscribe` | Subscribe to one session's transcript/event stream before sending an initial prompt. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `chat.send` | Execute chat work when `initialPrompt` is provided. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |

Gateway adapter calls:

```ts
transport.request("sessions.create", {
  label: title,
  agentId: "main"
});

transport.request("chat.send", {
  sessionKey: sessionId,
  message: initialPrompt,
  deliver: false,
  idempotencyKey: randomUUID()
});
```

Response:

```json
{
  "id": "agent:main:dashboard:...",
  "title": "New research session",
  "status": "idle",
  "hostKind": "openclaw",
  "runnerCommand": "openclaw-gateway",
  "createdAt": "2026-05-20T10:00:00.000Z",
  "updatedAt": "2026-05-20T10:00:00.000Z",
  "lastEventSeq": 0
}
```

## 5. Frontend Feature: Current Conversation

中间 current conversation 显示当前会话消息、内嵌活动卡片，并提供输入框发送消息。

### 5.1 `GET /api/sessions/:id`

用途：

- 拉取当前会话详情。
- 返回会话元数据与消息列表。
- 若本地 store 尚未 hydration，后端会从 Gateway 拉取历史消息和事件。

Frontend:

- `fetchSessionDetail(sessionId)` in [web/src/api.ts](./web/src/api.ts)
- Active session effect in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- `GET /api/sessions/:id` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Hydration path: `hydrateSession(sessionId)`

OpenClaw upstream:

| OpenClaw API | Purpose | Official doc |
| --- | --- | --- |
| `sessions.list` | Used by `getSession()` to find a matching session row. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `chat.history` | Load display-normalized message history. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |

Response:

```json
{
  "session": {
    "id": "agent:main:dashboard:...",
    "title": "Research books",
    "status": "completed",
    "hostKind": "openclaw",
    "runnerCommand": "openclaw-gateway",
    "createdAt": "2026-05-20T10:00:00.000Z",
    "updatedAt": "2026-05-20T10:04:00.000Z",
    "lastEventSeq": 57
  },
  "messages": [
    {
      "id": "user-...",
      "sessionId": "agent:main:dashboard:...",
      "role": "user",
      "content": "帮我从网上查询5本书并总结",
      "createdAt": "2026-05-20T10:00:01.000Z"
    },
    {
      "id": "assistant-...",
      "sessionId": "agent:main:dashboard:...",
      "role": "assistant",
      "content": "我查到 Literary Hub / Book Marks 的榜单...",
      "createdAt": "2026-05-20T10:04:00.000Z"
    }
  ]
}
```

### 5.2 `POST /api/sessions/:id/messages`

用途：

- 向当前会话追加用户消息。
- 将会话置为 `running`。
- 默认通过 Gateway WebSocket RPC `chat.send` 执行任务。
- 如果显式开启 `gateway.preferResponsesApi`，则先尝试 OpenClaw Gateway HTTP `POST /v1/responses` SSE；若该 endpoint 未启用或返回 `404` / `405` / `501`，自动回退到 `chat.send`。
- 后续 assistant 输出通过 `WS /ws/sessions/:id` 推送。

Frontend:

- `sendMessage(token, sessionId, content)` in [web/src/api.ts](./web/src/api.ts)
- Composer form handler in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- `POST /api/sessions/:id/messages` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)

Auth:

```http
Content-Type: application/json
X-Plugin-Token: <token>
```

Request:

```json
{
  "content": "帮我从网上查询5本书并总结"
}
```

OpenClaw upstream:

| OpenClaw API | Purpose | Official doc |
| --- | --- | --- |
| `sessions.messages.subscribe` | Ensure live events are subscribed before sending. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `chat.send` | Default execution path. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `POST /v1/responses` | Optional execution path when `gateway.preferResponsesApi` is explicitly enabled; returns SSE `response.output_text.delta` events. | [OpenResponses API](https://docs.openclaw.ai/gateway/openresponses-http-api) |

Optional Gateway HTTP call:

```http
POST /v1/responses
Authorization: Bearer <gateway-secret>
Accept: text/event-stream
Content-Type: application/json
x-openclaw-agent-id: main
x-openclaw-session-key: <sessionId>
x-openclaw-model: <optional configured modelOverride>
```

```json
{
  "model": "openclaw",
  "stream": true,
  "input": "帮我从网上查询5本书并总结",
  "user": "<sessionId>",
  "metadata": {
    "source": "claw-control-center",
    "sessionId": "<sessionId>"
  }
}
```

Fallback Gateway RPC call:

```ts
transport.request("chat.send", {
  sessionKey: sessionId,
  message: content,
  deliver: false,
  idempotencyKey: randomUUID()
});
```

Response:

```json
{
  "ok": true
}
```

Error example:

```json
{
  "error": "responses api failed: HTTP 500 ..."
}
```

### 5.3 `WS /ws/sessions/:id`

用途：

- 推送当前会话新增事件。
- 驱动聊天区中的 assistant 增量文本。
- 驱动 event list 和内嵌活动卡片。
- 驱动 session status 更新。

Frontend:

- Session WebSocket effect in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- WebSocket upgrade and `handleSocket()` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Broadcast path: `broadcastSessionEvent(sessionId, event)`

OpenClaw upstream:

| OpenClaw event/API | Purpose | Official doc |
| --- | --- | --- |
| `sessions.subscribe` | Subscribe current WS client to session change events. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.messages.subscribe` | Subscribe current WS client to one session transcript/event stream. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `chat` event | UI chat update event; mapped to `assistant.delta` or `assistant.message`. | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| `session.message` event | Transcript/message update for a subscribed session. | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| `session.tool` event | Tool event update for a subscribed session. | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| `sessions.changed` event | Session index/metadata/run status changed. | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |

Local event shape:

```json
{
  "id": "agent:main:dashboard:...:chat:55",
  "sessionId": "agent:main:dashboard:...",
  "seq": 55,
  "kind": "assistant.delta",
  "payload": {
    "content": "partial assistant text",
    "state": "delta",
    "runId": "run-id",
    "rawSeq": 12
  },
  "createdAt": "2026-05-20T10:03:20.000Z"
}
```

Supported local event kinds:

| Local event kind | Meaning | Primary frontend usage |
| --- | --- | --- |
| `user.message` | User message observed from Gateway. | Message list |
| `assistant.delta` | Streaming or cumulative assistant text. | Streaming assistant bubble |
| `assistant.message` | Final assistant message. | Final assistant bubble |
| `tool.call` | Claw invoked a tool or inspected a skill. | Activity card + event list |
| `tool.result` | Tool returned a result. | Activity card + event list |
| `status.update` | Non-terminal runtime status. | Status label + event list |
| `stderr.line` | Runner stderr line if available. | Event list |
| `run.started` | A run started. | Status label |
| `run.completed` | A run finished successfully. | Status label + detail refresh |
| `run.failed` | A run failed. | Status label + error display |
| `run.interrupted` | A run was aborted/interrupted. | Status label |

### 5.4 `POST /api/sessions/:id/control`

用途：

- 停止、重试、重命名或归档会话。

Frontend:

- 当前前端主要使用 session switching 和 message sending；control API 可供后续按钮或调试工具使用。

Backend:

- `POST /api/sessions/:id/control` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)

Request examples:

```json
{ "action": "stop" }
```

```json
{ "action": "retry" }
```

```json
{ "action": "rename", "title": "New title" }
```

```json
{ "action": "archive" }
```

OpenClaw upstream:

| Action | OpenClaw API | Purpose | Official doc |
| --- | --- | --- | --- |
| `stop` | `chat.abort` | Abort active chat work. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `retry` | `chat.history` + `chat.send` | Find last user message and send it again. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `rename` | `sessions.patch` | Update session label/metadata. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `archive` | none | Local archive only in current implementation. | N/A |

Response:

```json
{
  "ok": true
}
```

## 6. Frontend Feature: Event List

独立 event list 栏用于查看当前会话的可见执行过程，例如工具调用、skill 查看、运行状态和输出事件。

### 6.1 `GET /api/sessions/:id/events?afterSeq=0`

用途：

- 拉取当前会话事件历史。
- 支持 `afterSeq` 做增量补拉。
- 页面刷新、WebSocket 重连、切换 session 后都可以用它补齐事件。

Frontend:

- `fetchSessionEvents(sessionId, afterSeq)` in [web/src/api.ts](./web/src/api.ts)
- Event list rendering in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- `GET /api/sessions/:id/events` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- If cache is missing, calls `hydrateSession(sessionId)`

OpenClaw upstream:

| OpenClaw API | Purpose | Official doc |
| --- | --- | --- |
| `chat.history` | Used to synthesize assistant history events when replaying a session. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.messages.subscribe` | Live events are captured from active subscriptions. | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `session.message` / `session.tool` / `sessions.changed` | Source events for live event list. | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |

Request:

```http
GET /api/sessions/agent%3Amain%3Adashboard%3A.../events?afterSeq=12
```

Response:

```json
{
  "events": [
    {
      "id": "agent:main:dashboard:...:tool:13",
      "sessionId": "agent:main:dashboard:...",
      "seq": 13,
      "kind": "tool.call",
      "payload": {
        "phase": "start",
        "name": "read",
        "args": {
          "path": "/Users/me/.openclaw/skills/weather/SKILL.md"
        }
      },
      "createdAt": "2026-05-20T10:01:12.000Z"
    },
    {
      "id": "agent:main:dashboard:...:status:57",
      "sessionId": "agent:main:dashboard:...",
      "seq": 57,
      "kind": "run.completed",
      "payload": {
        "status": "done"
      },
      "createdAt": "2026-05-20T10:04:00.000Z"
    }
  ]
}
```

### 6.2 Event list vs conversation activity cards

当前前端同时有两种事件展示：

| Surface | Purpose | Detail level |
| --- | --- | --- |
| Conversation activity cards | 将重要活动嵌入对话流，默认折叠。 | 只显示用户需要看到的信息，例如 inspected skill、used tool、completed run。 |
| Event list column | 展示当前会话完整事件列表。 | 显示 event kind、seq、summary、payload JSON。 |

Activity summarization implementation:

- `buildConversationItems()` in [web/src/App.tsx](./web/src/App.tsx)
- `summarizeActivity()` in [web/src/App.tsx](./web/src/App.tsx)
- `buildEventListItems()` in [web/src/App.tsx](./web/src/App.tsx)

## 7. Frontend Feature: Claw Status

右侧 status 面板展示当前 Claw、插件、本地服务、模型、skill、53AIHub bridge 状态。

### 7.1 `GET /api/status`

用途：

- 获取全局状态快照。
- 页面初始化、调试工具、健康检查均可使用。

Frontend:

- Bootstrap initializes status.
- `WS /ws/status` keeps status live.
- Status panel rendering in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- `GET /api/status` in [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- `buildStatusSnapshot()`

OpenClaw upstream:

- 当前实现不调用 Gateway `status` / `health` RPC；`healthy` 由本地 Gateway adapter 最近错误状态推断。
- 官方可选 RPC:

| OpenClaw API | Purpose | Official doc |
| --- | --- | --- |
| `status` | Return `/status`-style Gateway summary. | [System and identity](https://docs.openclaw.ai/gateway/protocol#system-and-identity) |
| `health` | Return cached or freshly probed Gateway health snapshot. | [System and identity](https://docs.openclaw.ai/gateway/protocol#system-and-identity) |

Response:

```json
{
  "hostKind": "openclaw",
  "stateDir": "/Users/me/.openclaw",
  "configPath": "/Users/me/.openclaw/openclaw.json",
  "serviceVersion": "0.1.1",
  "pluginVersion": "0.1.1",
  "port": 4318,
  "pid": 12345,
  "runnerCommand": "gateway",
  "activeSessionCount": 4,
  "runningSessionCount": 1,
  "healthy": true,
  "modelPrimary": "openai/gpt-5.1",
  "enabledSkills": ["browser", "weather"],
  "hub53ai": {
    "enabled": true,
    "configured": true,
    "connectionStatus": "connected",
    "botId": "eW***l8",
    "wsUrl": "ws://kmapirc.53ai.com/api/v1/openclaw/ws/connect",
    "lastHeartbeatAt": "2026-05-20T10:00:00.000Z",
    "lastConnectedAt": "2026-05-20T09:55:00.000Z",
    "receivedMessageCount": 2,
    "sentMessageCount": 5,
    "pendingOutboundCount": 0
  }
}
```

### 7.2 `WS /ws/status`

用途：

- 推送全局状态变化。
- 当前实现每次 status socket 收到快照后，前端会刷新 session list。

Frontend:

- Status WebSocket effect in [web/src/App.tsx](./web/src/App.tsx)

Backend:

- WebSocket upgrade and status socket registry in [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Broadcast path: `broadcastStatus()`

OpenClaw upstream:

- `skills.status`。插件会先尝试读取运行时 skill/model 摘要，再推送本地 `buildStatusSnapshot()` 的结果；如果该 RPC 不可用，则回退到宿主 `openclaw.json`。

Message example:

```json
{
  "hostKind": "qclaw",
  "activeSessionCount": 12,
  "runningSessionCount": 2,
  "healthy": true,
  "modelPrimary": "qclaw/modelroute",
  "enabledSkills": ["browser", "online-search"]
}
```

### 7.3 Model and enabled skills

当前实现：

- `modelPrimary` 和 `enabledSkills` 优先由 [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) 调用 `skills.status` 读取运行时信息。
- 如果 Gateway 不支持该 RPC 或请求失败，则由 [plugin/src/host.ts](./plugin/src/host.ts) 读取宿主 `openclaw.json` 作为回退。
- 配置回退读取路径：
  - `agents.defaults.model.primary`
  - `skills.entries.*.enabled === true`

OpenClaw 官方可选 RPC：

| OpenClaw API | Purpose | Official doc |
| --- | --- | --- |
| `models.list` | Return runtime-allowed model catalog. | [Models and usage](https://docs.openclaw.ai/gateway/protocol#models-and-usage) |
| `skills.status` | Return visible skill inventory for an agent. | [Operator helper methods](https://docs.openclaw.ai/gateway/protocol#operator-helper-methods) |

重要说明：

- 当前插件已调用 `skills.status`，并对结果做 5 秒缓存，避免状态页高频刷新反复访问 Gateway。
- `models.list` 仍未作为独立模型目录来源使用；如果之后需要展示完整模型目录、skill eligibility、缺失依赖、安装状态，可以继续扩展上述 Gateway RPC。

## 8. Frontend Feature: 53AIHub Bridge Status

53AIHub bridge 让公司服务器通过 WebSocket 与本地 Claw 会话互通。前端只展示状态；实际桥接逻辑运行在插件进程内。

### 8.1 Status fields

来源：

- `GET /api/bootstrap`
- `GET /api/status`
- `WS /ws/status`

字段：

| Field | Meaning |
| --- | --- |
| `hub53ai.enabled` | 是否启用 53AIHub bridge。 |
| `hub53ai.configured` | `botId`、`secret`、`wsUrl` 是否齐全。 |
| `hub53ai.connectionStatus` | `disabled` / `connecting` / `connected` / `disconnected` / `error`。 |
| `hub53ai.botId` | 脱敏 bot id。 |
| `hub53ai.wsUrl` | 脱敏后的 WebSocket URL。 |
| `hub53ai.lastHeartbeatAt` | 最近业务层 ping/pong 时间。 |
| `hub53ai.lastConnectedAt` | 最近成功连接时间。 |
| `hub53ai.lastError` | 最近错误，不包含 secret。 |
| `hub53ai.receivedMessageCount` | 收到公司服务器消息数。 |
| `hub53ai.sentMessageCount` | 发送到公司服务器消息数。 |
| `hub53ai.pendingOutboundCount` | 断线待重放 outbox 数。 |

Implementation:

- [plugin/src/53aihub-client.ts](./plugin/src/53aihub-client.ts)
- `getStatus()`

### 8.2 Company server WebSocket protocol

这是公司侧协议，不属于 OpenClaw 官方 Gateway 文档。

Connection headers:

| Header | Value |
| --- | --- |
| `Authorization` | `Bearer <secret>` |
| `Proxy-Authorization` | `Basic base64(botId:secret)` |
| `X-Bot-Id` | `<botId>` |
| `X-Api-Key` | `<secret>` |

Business heartbeat:

```json
{
  "action": "ping",
  "data": {
    "botId": "bot_example"
  }
}
```

Incoming remote chat:

```json
{
  "req_id": "req-1",
  "action": "chat",
  "data": {
    "messages": [
      {
        "role": "user",
        "content": "帮我总结今天的任务"
      }
    ],
    "user": "user-1",
    "conversation_id": "chat-1"
  }
}
```

Outgoing chunk:

```json
{
  "req_id": "req-1",
  "action": "chat",
  "status": "streaming",
  "data": {
    "id": "req-1",
    "object": "chat.completion.chunk",
    "created": 1779170000,
    "model": "openclaw-agent",
    "choices": [
      {
        "index": 0,
        "delta": {
          "content": "partial assistant text",
          "role": "assistant"
        },
        "finish_reason": null
      }
    ]
  }
}
```

Bridge to OpenClaw upstream:

| Bridge step | OpenClaw API/event | Official doc |
| --- | --- | --- |
| Create mapped local session | `sessions.create` | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| Send remote user message to Claw | `chat.send` | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| Subscribe to local run events | `sessions.messages.subscribe` | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| Convert visible assistant output | `chat` / `session.message` events | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| Convert visible tool/status activity | `session.tool` / `sessions.changed` events | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |

## 9. OpenClaw Gateway RPC Mapping

此表是当前插件实际使用或明确映射的 OpenClaw Gateway API。

| OpenClaw Gateway API | Used by plugin feature | Local implementation | Official doc position |
| --- | --- | --- | --- |
| `connect` | Gateway handshake | `RpcSocketClient.sendConnect()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Handshake (connect)](https://docs.openclaw.ai/gateway/protocol#handshake-connect) |
| Request frame `{type:"req", id, method, params}` | All RPC calls | `RpcSocketClient.request()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Framing](https://docs.openclaw.ai/gateway/protocol#framing) |
| Response frame `{type:"res", id, ok, payload\|error}` | All RPC responses | `RpcSocketClient.handleFrame()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Framing](https://docs.openclaw.ai/gateway/protocol#framing) |
| Event frame `{type:"event", event, payload}` | Session/status streaming | `transport.onEvent(...)` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Framing](https://docs.openclaw.ai/gateway/protocol#framing) |
| `sessions.list` | Session list, detail fallback | `listSessions()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.create` | New session, 53AIHub mapped session | `createSession()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.patch` | Rename session | `controlSession("rename")` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.subscribe` | Session index/status change stream | `ensureSubscribed()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.messages.subscribe` | Current conversation event stream | `ensureSubscribed()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `sessions.messages.unsubscribe` | Cleanup unused subscription | `subscribe()` cleanup in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `chat.history` | Load message history, synthesize history events, retry lookup | `getSessionMessages()` / `listEvents()` / retry path in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `chat.send` | Default user-message execution path | `sendMessage()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `POST /v1/responses` | Optional local message execution path when `gateway.preferResponsesApi` is explicitly enabled and the host endpoint is available | `startResponsesApiRun()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [OpenResponses API](https://docs.openclaw.ai/gateway/openresponses-http-api) |
| `response.output_text.delta` | Optional SSE assistant text stream event | `consumeResponsesApiStream()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Streaming (SSE)](https://docs.openclaw.ai/gateway/openresponses-http-api#streaming-sse) |
| `response.output_text.done` / `response.completed` | Optional SSE final text and completion events | `consumeResponsesApiStream()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Streaming (SSE)](https://docs.openclaw.ai/gateway/openresponses-http-api#streaming-sse) |
| `chat.abort` | Stop active run | `controlSession("stop")` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Session control](https://docs.openclaw.ai/gateway/protocol#session-control) |
| `chat` event | Assistant delta/final text | `mapGatewayFrameToEvents()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| `session.message` event | User/assistant transcript rows | `mapGatewayFrameToEvents()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| `session.tool` event | Tool call/result activity | `mapGatewayFrameToEvents()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| `sessions.changed` event | Run started/completed/status update | `mapGatewayFrameToEvents()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families) |
| `skills.status` | Runtime skill/model summary for status panel | `getRuntimeInfo()` in [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts) | [Operator helper methods](https://docs.openclaw.ai/gateway/protocol#operator-helper-methods) |
| `models.list` | Not currently called as a standalone model catalog source | N/A | [Models and usage](https://docs.openclaw.ai/gateway/protocol#models-and-usage) |
| `tools.effective` | Not currently called; recommended future source for session-scoped tool inventory | N/A | [Operator helper methods](https://docs.openclaw.ai/gateway/protocol#operator-helper-methods) |
| `status` | Not currently called; optional future Gateway status source | N/A | [System and identity](https://docs.openclaw.ai/gateway/protocol#system-and-identity) |
| `health` | Not currently called; optional future Gateway health source | N/A | [System and identity](https://docs.openclaw.ai/gateway/protocol#system-and-identity) |

## 10. Data Types

### 10.1 `SessionSummary`

Source:

- [plugin/src/models.ts](./plugin/src/models.ts)
- [web/src/types.ts](./web/src/types.ts)

```ts
type SessionSummary = {
  id: string;
  title: string;
  status: "idle" | "running" | "completed" | "failed" | "interrupted" | "archived";
  hostKind: string;
  runnerCommand: string;
  createdAt: string;
  updatedAt: string;
  lastEventSeq: number;
};
```

### 10.2 `SessionMessage`

```ts
type SessionMessage = {
  id: string;
  sessionId: string;
  role: string;
  content: string;
  createdAt: string;
};
```

### 10.3 `TimelineEvent`

```ts
type TimelineEvent = {
  id: string;
  sessionId: string;
  seq: number;
  kind: string;
  payload: Record<string, unknown>;
  createdAt: string;
};
```

### 10.4 `PluginStatusSnapshot`

```ts
type PluginStatusSnapshot = {
  hostKind: string;
  stateDir?: string;
  configPath?: string;
  serviceVersion?: string;
  pluginVersion?: string;
  port?: number;
  pid?: number;
  runnerCommand?: string;
  activeSessionCount: number;
  runningSessionCount: number;
  healthy: boolean;
  modelPrimary?: string;
  enabledSkills?: string[];
  hub53ai?: Hub53AIStatusSnapshot;
};
```

## 11. Typical Frontend Flows

### 11.1 Page open

```text
GET /api/bootstrap
GET /api/sessions
GET /api/sessions/:id
GET /api/sessions/:id/events
WS  /ws/status
WS  /ws/sessions/:id
```

OpenClaw upstream:

```text
connect
sessions.list
chat.history
sessions.subscribe
sessions.messages.subscribe
```

Official docs:

- [Handshake (connect)](https://docs.openclaw.ai/gateway/protocol#handshake-connect)
- [Session control](https://docs.openclaw.ai/gateway/protocol#session-control)

### 11.2 Create a session

```text
POST /api/sessions
```

OpenClaw upstream:

```text
sessions.create
```

Official docs:

- [Session control](https://docs.openclaw.ai/gateway/protocol#session-control)

### 11.3 Send a message and receive output

```text
POST /api/sessions/:id/messages
WS   /ws/sessions/:id
```

OpenClaw upstream:

```text
sessions.messages.subscribe
chat.send
chat event
session.message event
session.tool event
sessions.changed event
```

Official docs:

- [Session control](https://docs.openclaw.ai/gateway/protocol#session-control)
- [Common event families](https://docs.openclaw.ai/gateway/protocol#common-event-families)

### 11.4 Refresh or reconnect a conversation

```text
GET /api/sessions/:id
GET /api/sessions/:id/events?afterSeq=<last-seen-seq>
WS  /ws/sessions/:id
```

OpenClaw upstream:

```text
chat.history
sessions.messages.subscribe
```

Official docs:

- [Session control](https://docs.openclaw.ai/gateway/protocol#session-control)

### 11.5 Stop a run

```text
POST /api/sessions/:id/control
{ "action": "stop" }
```

OpenClaw upstream:

```text
chat.abort
```

Official docs:

- [Session control](https://docs.openclaw.ai/gateway/protocol#session-control)

## 12. Manual Debug Commands

Use `BASE` because the configured console port can differ by machine.

```fish
set BASE http://127.0.0.1:4318
```

If the current plugin is running on another port, check:

```fish
curl -fsS $BASE/api/status | jq
```

### 12.1 Bootstrap

```fish
curl -fsS $BASE/api/bootstrap | jq
```

### 12.2 List sessions

```fish
curl -fsS $BASE/api/sessions | jq '.sessions[] | {id, title, status, updatedAt}'
```

### 12.3 Create a session

```fish
set TOKEN (curl -fsS $BASE/api/bootstrap | jq -r '.token')

curl -fsS $BASE/api/sessions \
  -H 'Content-Type: application/json' \
  -H "X-Plugin-Token: $TOKEN" \
  -d '{"title":"Manual API test"}' | jq
```

### 12.4 Send a message

```fish
set SESSION_ID '<session-id>'

curl -fsS "$BASE/api/sessions/$SESSION_ID/messages" \
  -H 'Content-Type: application/json' \
  -H "X-Plugin-Token: $TOKEN" \
  -d '{"content":"Say hello in one short sentence."}' | jq
```

### 12.5 Fetch event list

```fish
curl -fsS "$BASE/api/sessions/$SESSION_ID/events?afterSeq=0" | jq '.events[] | {seq, kind, payload}'
```

### 12.6 Watch session WebSocket events

```fish
npx wscat -c "ws://127.0.0.1:4318/ws/sessions/$SESSION_ID"
```

### 12.7 Watch status WebSocket

```fish
npx wscat -c ws://127.0.0.1:4318/ws/status
```

## 13. Current Boundaries

1. The local console API is an adapter/cache layer, not a second execution engine.
2. Execution authority belongs to the local OpenClaw/QClaw Gateway.
3. UI disconnects do not stop a Claw run; they only drop the browser subscription.
4. Session history and events are persisted locally in JSON/JSONL-style plugin state.
5. `modelPrimary` and `enabledSkills` prefer runtime `skills.status`, then fall back to `openclaw.json`.
6. 53AIHub protocol is company-specific and separate from OpenClaw Gateway Protocol.
7. Secrets are never returned in `/api/bootstrap`, `/api/config`, `/api/status`, or frontend state.
