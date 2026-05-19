# Claw Control Center API 文档

本文档整理当前插件实际使用到的全部 API，并说明它们分别位于哪一层、如何调用、以及在代码中的实现位置。

---

## 1. 总体结构

当前插件不是“前端单独访问远程服务”的结构，而是三层串联：

```text
Browser UI
  -> local REST / WebSocket console
  -> gateway adapter
  -> QClaw / OpenClaw local gateway RPC
  -> QClaw / OpenClaw session runtime
```

对应实现文件：

- 插件宿主层：[plugin/src/index.ts](./plugin/src/index.ts)
- 本地控制台层：[plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Gateway 适配层：[plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts)
- 前端调用层：[web/src/api.ts](./web/src/api.ts)
- 前端页面层：[web/src/App.tsx](./web/src/App.tsx)

---

## 2. 宿主插件 API

这一层是插件与 QClaw / OpenClaw 宿主之间的接口。  
本项目当前只使用了最小必需集合。

### 2.1 使用到的宿主 API

来源类型：

- `OpenClawPluginApi`

实现位置：

- [plugin/src/index.ts](./plugin/src/index.ts)

实际使用项：

| API | 用途 | 代码位置 |
|---|---|---|
| `api.pluginConfig` | 读取插件配置 | `register()` |
| `api.rootDir` | 解析内置 `web-dist` 与 runtime 根目录 | `createRuntime()` |
| `api.version` | 暴露插件版本到状态页 | `createRuntime()` |
| `api.logger.info(...)` | 宿主日志输出 | `start()` |
| `api.registerService(...)` | 向宿主注册生命周期服务 | `register()` |

### 2.2 宿主启动行为

插件通过：

```ts
api.registerService({
  id: "claw-control-center-service",
  async start(ctx) { ... },
  async stop() { ... }
});
```

在宿主启动时完成：

1. 解析 `stateDir`
2. 推断 `openclaw.json`
3. 读取 gateway 配置
4. 创建本地控制台服务
5. 对外暴露 `http://127.0.0.1:4318`

---

## 3. 安装命令 API

这一层不是运行时 API，而是用户安装接口。

实现位置：

- [plugin/src/install-qclaw.ts](./plugin/src/install-qclaw.ts)
- [plugin/bin/install-qclaw.mjs](./plugin/bin/install-qclaw.mjs)

### 3.1 命令格式

```bash
node plugin/bin/install-qclaw.mjs install --target qclaw
node plugin/bin/install-qclaw.mjs install --target openclaw
```

可选参数：

| 参数 | 说明 |
|---|---|
| `--target` | 安装目标，支持 `qclaw` 或 `openclaw` |
| `--gateway` | 显式指定 gateway URL |
| `--secret` | 显式指定 gateway token / secret |
| `--bot-id` | 记录 bot 标识 |
| `--extensions-dir` | 覆盖扩展安装目录 |
| `--config-path` | 覆盖 `openclaw.json` 路径 |
| `--console-host` | 覆盖控制台监听地址 |
| `--console-port` | 覆盖控制台端口 |

### 3.2 默认行为

如果未显式传 `--gateway` / `--secret`，安装器会尝试从对应宿主的 `openclaw.json` 中推断：

- `gateway.host`
- `gateway.port`
- `gateway.auth.token`
- `gateway.auth.password`

默认路径：

| target | 配置文件 | 扩展目录 |
|---|---|---|
| `qclaw` | `~/.qclaw/openclaw.json` | `~/Library/Application Support/QClaw/openclaw/config/extensions` |
| `openclaw` | `~/.openclaw/openclaw.json` | `~/.openclaw/extensions` |

### 3.3 安装器实际做的事

1. 复制以下发布文件到目标宿主扩展目录：
   - `dist`
   - `openclaw.plugin.json`
   - `package.json`
   - `bin`
   - `web-dist`
2. 更新 `plugins.allow`
3. 更新 `plugins.load.paths`
4. 写入 `plugins.entries["claw-control-center"]`
5. 保留其他插件配置

---

## 4. 本地控制台 HTTP API

这一层由插件内置 HTTP 服务提供。  
前端页面与外部调试工具都通过它访问插件状态。

实现位置：

- 服务端：[plugin/src/console-server.ts](./plugin/src/console-server.ts)
- 前端封装：[web/src/api.ts](./web/src/api.ts)

### 4.1 认证方式

写操作使用：

```http
X-Plugin-Token: <token>
```

这个 token 通过 `GET /api/bootstrap` 返回。

### 4.2 `GET /api/bootstrap`

用途：

- 初始化页面
- 获取本地 token
- 获取状态与脱敏配置

前端调用：

- `fetchBootstrap()` in [web/src/api.ts](./web/src/api.ts)

返回形状：

```json
{
  "token": "local-token",
  "status": {
    "hostKind": "qclaw",
    "activeSessionCount": 15,
    "runningSessionCount": 1,
    "healthy": true
  },
  "config": {
    "gateway": {
      "baseUrl": "ws://127.0.0.1:28789",
      "secret": "[redacted]"
    },
    "config": {
      "console": {
        "host": "127.0.0.1",
        "port": 4318
      }
    }
  }
}
```

### 4.3 `GET /api/status`

用途：

- 获取插件运行状态

返回字段：

| 字段 | 说明 |
|---|---|
| `hostKind` | 宿主类型 |
| `stateDir` | 状态目录 |
| `configPath` | 配置文件路径 |
| `serviceVersion` | 本地服务版本 |
| `pluginVersion` | 插件版本 |
| `port` | 控制台端口 |
| `pid` | 当前进程 PID |
| `runnerCommand` | 固定为 `gateway` |
| `activeSessionCount` | 当前会话数 |
| `runningSessionCount` | 运行中的会话数 |
| `healthy` | gateway 连接是否健康 |

### 4.4 `GET /api/config`

用途：

- 展示脱敏配置

### 4.5 `GET /api/sessions`

用途：

- 拉取会话摘要列表

前端调用：

- `fetchSessions()` in [web/src/api.ts](./web/src/api.ts)

返回：

```json
{
  "sessions": [
    {
      "id": "agent:main:dashboard:...",
      "title": "Session 15",
      "status": "completed",
      "hostKind": "qclaw",
      "runnerCommand": "openclaw-gateway",
      "createdAt": "...",
      "updatedAt": "...",
      "lastEventSeq": 176
    }
  ]
}
```

### 4.6 `POST /api/sessions`

用途：

- 创建新会话

请求头：

```http
Content-Type: application/json
X-Plugin-Token: <token>
```

请求体：

```json
{
  "title": "My session",
  "initialPrompt": "Optional first prompt"
}
```

处理逻辑：

1. 调用 gateway `sessions.create`
2. 写入本地 store
3. 如果带 `initialPrompt`，继续调用 `chat.send`

### 4.7 `GET /api/sessions/:id`

用途：

- 拉取某个会话的详情
- 如果本地缓存尚未补水 (hydrate)，会自动从 gateway 拉取消息和事件

返回：

```json
{
  "session": { "...": "..." },
  "messages": [
    {
      "id": "user-1",
      "role": "user",
      "content": "Hello",
      "createdAt": "..."
    }
  ]
}
```

### 4.8 `POST /api/sessions/:id/messages`

用途：

- 给已有会话追加用户消息

请求体：

```json
{
  "content": "Ask Claw to do something..."
}
```

处理逻辑：

1. 追加本地用户消息
2. 把会话状态设为 `running`
3. 调用 gateway `chat.send`
4. 等待后续 WebSocket 事件推动 assistant 输出

### 4.9 `POST /api/sessions/:id/control`

用途：

- 控制会话

支持动作：

| action | 语义 |
|---|---|
| `stop` | 中止当前运行 |
| `retry` | 用最后一条用户消息重试 |
| `rename` | 修改标题 |
| `archive` | 本地归档 |

请求体示例：

```json
{
  "action": "rename",
  "title": "Renamed session"
}
```

### 4.10 `GET /api/sessions/:id/events?afterSeq=0`

用途：

- 拉取事件时间线

返回：

```json
{
  "events": [
    {
      "id": "session-1:status:1",
      "seq": 1,
      "kind": "run.started",
      "payload": {},
      "createdAt": "..."
    }
  ]
}
```

---

## 5. 本地控制台 WebSocket API

这一层用于实时推送状态和会话事件。

实现位置：

- [plugin/src/console-server.ts](./plugin/src/console-server.ts)
- [web/src/App.tsx](./web/src/App.tsx)

### 5.1 `WS /ws/status`

用途：

- 推送全局状态快照

典型前端用途：

1. 更新 `healthy`
2. 更新 `runningSessionCount`
3. 触发一次会话列表刷新

### 5.2 `WS /ws/sessions/:id`

用途：

- 推送指定会话的增量事件

事件由 gateway 事件映射而来，前端根据 `kind` 更新：

- `events`
- `messages`
- `session status`

---

## 6. Gateway RPC API

这一层不是 HTTP，而是通过 QClaw 本地 gateway 的 WebSocket RPC 调用。

实现位置：

- [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts)

### 6.1 连接方式

优先策略：

1. 优先复用宿主官方 `GatewayClient`
2. 若找不到宿主官方实现，再退回自定义 `RpcSocketClient`

连接参数：

| 参数 | 说明 |
|---|---|
| `baseUrl` | `ws://127.0.0.1:28789` 之类的 gateway 地址 |
| `secret` | gateway token |
| `requestTimeoutMs` | RPC 超时 |
| `streamReconnectMs` | 流断开后的重连等待 |
| `runtimeRoot` | 宿主 runtime 根目录，用于定位官方 `GatewayClient` |

### 6.2 实际调用的 RPC 方法

| RPC 方法 | 用途 | 代码位置 |
|---|---|---|
| `sessions.list` | 列出会话 | `listSessions()` |
| `sessions.create` | 创建会话 | `createSession()` |
| `sessions.patch` | 重命名会话 | `controlSession("rename")` |
| `sessions.subscribe` | 订阅会话总线 | `ensureSubscribed()` |
| `sessions.messages.subscribe` | 订阅单会话消息流 | `ensureSubscribed()` |
| `sessions.messages.unsubscribe` | 取消订阅 | `subscribe() -> cleanup` |
| `chat.history` | 拉取历史消息 / 历史事件 | `getSessionMessages()` / `listEvents()` |
| `chat.send` | 发送消息 | `sendMessage()` |
| `chat.abort` | 中止运行 | `controlSession("stop")` |

### 6.3 实际消费的 gateway 事件

| gateway 事件 | 映射后的本地事件 |
|---|---|
| `chat` | `assistant.delta` |
| `session.message` | `assistant.message` |
| `session.tool` | `tool.call` / `tool.result` |
| `sessions.changed` + `phase=start` | `run.started` |
| `sessions.changed` + `phase=end` + `status=done` | `run.completed` |
| `sessions.changed` + `phase=end` + `status=aborted` | `run.interrupted` |
| `sessions.changed` 其他情况 | `status.update` |

### 6.4 事件映射实现

实现函数：

- `mapGatewayFrameToEvents(...)`

实现文件：

- [plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts)

这层的作用是把 gateway 原始事件统一规整为前端固定使用的 `TimelineEvent`。

---

## 7. 前端内部 API 封装

这一层是 React 页面内部调用的 API 封装，不是对外协议本身。

实现位置：

- [web/src/api.ts](./web/src/api.ts)

实际封装函数：

| 函数 | 对应接口 |
|---|---|
| `fetchBootstrap()` | `GET /api/bootstrap` |
| `fetchSessions()` | `GET /api/sessions` |
| `fetchSessionDetail(sessionId)` | `GET /api/sessions/:id` |
| `fetchSessionEvents(sessionId, afterSeq)` | `GET /api/sessions/:id/events` |
| `createSession(token, title, initialPrompt)` | `POST /api/sessions` |
| `sendMessage(token, sessionId, content)` | `POST /api/sessions/:id/messages` |

错误处理逻辑：

- 非 2xx 响应统一抛出 `Error`
- 优先读取后端 `{ error: string }`

---

## 8. 类型定义

核心共享类型定义于：

- [plugin/src/models.ts](./plugin/src/models.ts)
- [web/src/types.ts](./web/src/types.ts)

关键类型：

| 类型 | 作用 |
|---|---|
| `SessionSummary` | 会话列表项 |
| `SessionDetail` | 单会话详情 |
| `SessionMessage` | 对话消息 |
| `TimelineEvent` | 时间线事件 |
| `PluginStatusSnapshot` | 状态栏快照 |
| `ControlAction` | 会话控制动作 |

---

## 9. 典型调用流程

### 9.1 页面初次打开

1. 前端调用 `GET /api/bootstrap`
2. 前端调用 `GET /api/sessions`
3. 选中第一个会话
4. 前端连接 `WS /ws/status`
5. 前端连接 `WS /ws/sessions/:id`

### 9.2 创建新会话

1. 前端 `POST /api/sessions`
2. 本地控制台调用 gateway `sessions.create`
3. 本地 store 写入新会话
4. 前端切换到新会话

### 9.3 发送消息

1. 前端 `POST /api/sessions/:id/messages`
2. 控制台调用 gateway `chat.send`
3. gateway 推送 `chat` / `session.message` / `sessions.changed`
4. 插件映射为本地 `TimelineEvent`
5. 前端实时更新消息区和时间线

### 9.4 打开旧会话

1. 前端请求 `GET /api/sessions/:id`
2. 如果本地缓存尚未补水，控制台自动：
   - 调用 `getSession()`
   - 调用 `getSessionMessages()`
   - 调用 `listEvents()`
3. 本地 store 写入完整详情
4. 前端显示消息和时间线

---

## 10. 运行与调试建议

### 10.1 直接查看插件状态

```bash
curl -s http://127.0.0.1:4318/api/status
```

### 10.2 获取本地 token

```bash
curl -s http://127.0.0.1:4318/api/bootstrap
```

### 10.3 手动创建会话

```bash
curl -s http://127.0.0.1:4318/api/sessions \
  -H 'Content-Type: application/json' \
  -H 'X-Plugin-Token: <token>' \
  -d '{"title":"Manual API test"}'
```

### 10.4 手动发送消息

```bash
curl -s http://127.0.0.1:4318/api/sessions/<session-id>/messages \
  -H 'Content-Type: application/json' \
  -H 'X-Plugin-Token: <token>' \
  -d '{"content":"Say hello in one short sentence."}'
```

---

## 11. 当前边界

当前文档对应的是“QClaw 本地 gateway 模式”。

这意味着：

1. 当前运行时真正依赖的是本机 QClaw gateway，而不是独立远程 HTTP backend。
2. `botId` 目前主要进入安装配置层；运行时并未单独把它作为一条业务 RPC 参数显式传出。
3. 前端页面是插件内置静态资源，由插件本地服务托管，不是单独部署的外部站点。
