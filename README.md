# 53AI-OpenClaw

53AI-OpenClaw 是 53AI Hub 接入 OpenClaw / QClaw / Hermes 的通道插件。当前分支是基于旧版 `53AI-OpenClaw` 的新版实现，保留 53AIHub WebSocket 通道能力，并新增本地控制台、会话管理、Gateway RPC 适配、Hermes 原生平台适配和断线重放能力。

## 功能

- 将 OpenClaw / QClaw / Hermes 接入 53AIHub。
- 支持直接消息 (DM) 模式。
- 支持 53AIHub WebSocket 实时通信。
- 支持文本、图片、文件等多模态消息输入。
- 支持 AI 回复流式回传、完成态回传和错误态回传。
- 支持"思考中"消息，用户发送消息后可立即收到处理反馈。
- 支持断线缓存、重连补发和消息去重。
- 支持本地控制台，可查看状态、配置、会话、消息、技能和定时任务。
- 支持由目标 Claw 提供安装路径后完成插件安装，无需插件维护宿主分支判断。

## 当前实现说明

新版代码与旧版源码结构基本不同：

```text
53AI-OpenClaw
  -> plugin/               # OpenClaw / QClaw 插件与 Hermes 平台适配器实现
  -> web/                  # 本地控制台前端
  -> plugin/web-dist/      # 构建后随插件发布的前端资源
  -> API.md                # 本地控制台与 Gateway API 文档
  -> CHANGELOG.md          # 旧版 release 记录与新版分支说明
```

当前插件 ID 为 `claw-control-center`，其中包含 53AIHub bridge。发布到公司 npm 作用域时，可按发布策略将包名调整为 `@53ai/53ai-openclaw`。

## 安装方式

### 方式一：复制给 Claw 一键安装（发布后）

发布到 npm 后，53AIHub 推荐生成如下安装内容。用户可以直接复制到当前使用的 Claw 内部，让 Claw 执行安装：

```bash
curl -fsSL https://raw.githubusercontent.com/53AI/53AI-OpenClaw/main/install.sh | bash -s -- \
  --hub-bot-id "<bot-id>" \
  --hub-secret "<secret>" \
  --hub-ws-url "wss://kmapirc.53ai.com/api/v1/openclaw/ws/connect"
```

`install.sh` 内部包含给 Claw 智能体 (agent) 的提示词：优先安装到当前正在运行的 Claw；如果能识别当前 Claw 的 `openclaw.json` 和 extensions 目录，可以自动补充精确路径；否则由安装器自动检测本机 Claw。

如果用户是在普通终端手动执行同一条 curl 命令，安装器会扫描当前用户电脑上的 QClaw / OpenClaw / Hermes。只发现一个宿主时会直接安装；发现多个宿主时会显示安装位置列表，让用户选择一个或多个 Claw 写入插件。OpenClaw / QClaw 会安装 `openclaw.plugin.json` 扩展；Hermes 会安装 `plugin.yaml` 平台适配器，并把 53AIHub 连接参数写入 `~/.hermes/.env`。

如发布包名调整为 `@53ai/53ai-openclaw`，`install.sh` 中调用的 npm 包名也应同步调整。

### 方式二：本地开发调试安装

适用于插件开发阶段。当前仓库使用 `pnpm`：

```bash
pnpm install
pnpm build
```

本地调试时可以直接运行安装器。若当前电脑只检测到一个 Claw 宿主，可以使用自动发现：

```bash
node plugin/bin/install-qclaw.mjs install
```

如果当前电脑有多个兼容 Claw，安装器会显示可选安装位置。可以输入单个编号、逗号分隔的多个编号，或输入 `all` 安装到全部兼容宿主。若需要跳过选择，显式传入配置文件和扩展目录：

```bash
node plugin/bin/install-qclaw.mjs install \
  --config-path "<claw-openclaw-json-path>" \
  --extensions-dir "<claw-extensions-dir>"
```

如果需要同时写入 53AIHub 鉴权配置：

```bash
node plugin/bin/install-qclaw.mjs install \
  --hub-ws-url "wss://kmapirc.53ai.com/api/v1/openclaw/ws/connect" \
  --hub-bot-id "<bot-id>" \
  --hub-secret "<secret>"
```

如果由 Claw 宿主或 53AIHub 生成精确安装路径，也可以显式传入配置文件和扩展目录：

```bash
node plugin/bin/install-qclaw.mjs install \
  --config-path "<claw-openclaw-json-path>" \
  --extensions-dir "<claw-extensions-dir>" \
  --hub-ws-url "wss://kmapirc.53ai.com/api/v1/openclaw/ws/connect" \
  --hub-bot-id "<bot-id>" \
  --hub-secret "<secret>"
```

通常不需要传 `--gateway` / `--secret`。插件会从目标宿主自己的 `openclaw.json` 自动读取当前本地 Gateway 端口和 token。只有连接自定义 Gateway 时才显式传 `--gateway` / `--secret`；`--hub-*` 表示公司 53AIHub 服务器。两组配置不要混用。

### 方式三：npm pack 打包安装

适用于分发 tarball 文件，无需先发布到 npm：

```bash
pnpm build
cd plugin
pnpm pack
```

打包文件生成后，可按 OpenClaw 插件安装方式安装；也可以使用本仓库提供的安装脚本复制到扩展目录。

## 配置

安装脚本会写入 `plugins.entries.claw-control-center.config`。默认情况下，本地 Gateway 地址和 token 不会固化写入插件配置，而是在插件运行时从宿主 `openclaw.json` 读取当前值。核心配置如下：

```json
{
  "plugins": {
    "enabled": true,
    "allow": ["claw-control-center"],
    "entries": {
      "claw-control-center": {
        "enabled": true,
        "config": {
          "gateway": {
            "preferResponsesApi": false
          },
          "hub53ai": {
            "enabled": true,
            "botId": "<bot-id>",
            "secret": "<secret>",
            "wsUrl": "wss://kmapirc.53ai.com/api/v1/openclaw/ws/connect",
            "accessPolicy": "open",
            "allowFrom": [],
            "sendThinkingMessage": true
          },
          "console": {
            "enabled": true,
            "host": "127.0.0.1",
            "port": 4318
          }
        }
      }
    }
  }
}
```

### 配置参数说明

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `gateway.baseUrl` | 否 | 宿主配置 | 自定义 OpenClaw / QClaw Gateway 地址；通常不需要配置 |
| `gateway.secret` | 否 | 宿主配置 | 自定义 Gateway token；通常不需要配置 |
| `gateway.preferResponsesApi` | 否 | `false` | 是否优先使用 HTTP responses 路径 |
| `gateway.modelOverride` | 否 | - | 使用 HTTP responses 路径时指定模型 |
| `hub53ai.enabled` | 否 | `false` | 是否启用 53AIHub bridge |
| `hub53ai.botId` | 启用时必填 | - | 53AIHub 智能体 ID |
| `hub53ai.secret` | 启用时必填 | - | 53AIHub App Secret / Token |
| `hub53ai.wsUrl` | 启用时必填 | - | 53AIHub WebSocket 地址 |
| `hub53ai.accessPolicy` | 否 | `open` | 访问策略：`open` 或 `allowlist` |
| `hub53ai.allowFrom` | 否 | `[]` | 访问控制白名单 |
| `hub53ai.sendThinkingMessage` | 否 | `true` | 是否发送"思考中"提示消息 |
| `console.enabled` | 否 | `true` | 是否启用本地控制台 |
| `console.host` | 否 | `127.0.0.1` | 本地控制台监听地址 |
| `console.port` | 否 | `4318` | 本地控制台端口 |

新版实现仍会读取旧版 `channels.53aihub` 配置作为回退，便于从旧插件迁移。

## 安装路径

安装脚本支持两种路径：

1. 用户一键安装：不传安装目标参数，安装器自动发现当前用户电脑上的 QClaw / OpenClaw / Hermes。
2. 宿主集成安装：显式传入 `--config-path` 与 `--extensions-dir`，由宿主决定安装位置。

```bash
npx claw-control-center install \
  --hub-bot-id "<bot-id>" \
  --hub-secret "<secret>" \
  --hub-ws-url "<hub-ws-url>"
```

如果只检测到一个宿主，安装器会直接安装并打印实际写入的 `Extensions` 与 `Config`。如果检测到多个宿主，安装器会显示编号列表，让用户选择一个或多个安装位置。由于 `curl | bash` 会占用标准输入 (stdin)，安装器会通过 `/dev/tty` 读取用户选择，以便普通终端手动安装时仍能交互。

如果宿主提供精确路径，则使用：

```bash
npx claw-control-center install \
  --config-path "<claw-openclaw-json-path>" \
  --extensions-dir "<claw-extensions-dir>" \
  --hub-bot-id "<bot-id>" \
  --hub-secret "<secret>" \
  --hub-ws-url "<hub-ws-url>"
```

`--target` 已删除。继续传入 `--target` 会报错，并提示改用自动发现或显式路径参数。

无论哪种方式，OpenClaw / QClaw 未显式传入 `--gateway` / `--secret` 时，插件都会在运行时读取宿主当前 Gateway 配置，避免把某台机器上的临时端口写死到插件配置中。Hermes 不使用 OpenClaw Gateway Protocol，而是通过原生 Hermes 平台适配器收发消息。

Gateway 协议版本由 OpenClaw / QClaw 插件运行时自动协商，当前自写 Gateway client 支持 protocol 3 到 4。安装目录发现不参与协议版本判断，Hermes 兼容也不通过 Gateway Protocol 实现。

## 启动与访问

安装后重启对应宿主或 Gateway，然后访问本地控制台：

```text
http://127.0.0.1:4318/
```

本地控制台提供：

- 当前宿主、Gateway 和 53AIHub bridge 状态。
- 会话列表、消息列表和会话切换。
- 文本发送、停止运行、状态刷新。
- 技能、模型、定时任务和配置摘要。

## 验证安装

```bash
# 检查插件是否加载
openclaw plugins list | grep claw-control-center

# 检查宿主配置
openclaw config get plugins.entries.claw-control-center

# 检查通道状态
openclaw channels status --deep
```

也可以打开本地控制台，查看 `53AIHub` 状态是否为 connected。

## 多模态消息支持

插件支持从 53AIHub 接收图片和文件 URL，并将其转换为 OpenClaw / QClaw 可处理的消息内容。

用户发送图片时，插件可解析类似结构：

```typescript
{
  type: "message",
  msgId: "msg-xxx",
  chatId: "user-123",
  text: "这张图片是什么？",
  imageUrls: ["https://hub.53ai.com/image.png"]
}
```

用户发送文件时，插件可解析类似结构：

```typescript
{
  type: "message",
  msgId: "msg-xxx",
  chatId: "user-123",
  text: "请分析这个文档",
  fileUrls: ["https://hub.53ai.com/document.pdf"]
}
```

回复发送到 53AIHub 时会使用 `message` action，并带有 `streaming`、`thinking`、`done` 或 `error` 状态。

## 开发

```bash
# 安装依赖
pnpm install

# 构建前端与插件
pnpm build

# 运行全部测试
pnpm test

# 只运行插件测试
pnpm test:plugin

# 只运行前端测试
pnpm test:web
```

## 代码入口

- 插件入口：[plugin/src/index.ts](./plugin/src/index.ts)
- 宿主配置解析：[plugin/src/host.ts](./plugin/src/host.ts)
- 本地控制台服务：[plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Gateway 适配器：[plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts)
- 53AIHub bridge：[plugin/src/53aihub-client.ts](./plugin/src/53aihub-client.ts)
- 安装器：[plugin/src/install-qclaw.ts](./plugin/src/install-qclaw.ts)
- 前端应用：[web/src/App.tsx](./web/src/App.tsx)

## 文档索引

- API 文档与使用说明：[API.md](./API.md)
- 变更记录：[CHANGELOG.md](./CHANGELOG.md)

## 故障排查

### 插件未加载

1. 确认扩展目录下存在 `claw-control-center`。
2. 确认插件目录包含 `dist/`、`openclaw.plugin.json`、`package.json` 和 `web-dist/`。
3. 检查 `openclaw.json` 中 `plugins.enabled`、`plugins.allow` 与 `plugins.entries.claw-control-center.enabled`。
4. 重启对应宿主或 Gateway。

### 53AIHub 未连接

1. 确认 `hub53ai.enabled` 为 `true`。
2. 确认 `hub53ai.botId`、`hub53ai.secret`、`hub53ai.wsUrl` 均已配置。
3. 确认 `wsUrl` 使用 `ws://` 或 `wss://`。
4. 打开本地控制台查看最近错误、最近心跳和连接状态。

### 本地控制台无法访问

1. 确认 `console.enabled` 为 `true`。
2. 确认端口未被占用，默认端口为 `4318`。
3. 检查宿主日志中插件启动信息。

## License

MIT
