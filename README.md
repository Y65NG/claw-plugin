# Claw Control Center

`claw-control-center` 是一个运行在 QClaw / OpenClaw 宿主中的多会话控制插件。  
它会在宿主内启动一个本地控制台 (local control console)，并通过本地 gateway (gateway) 与 QClaw / OpenClaw 会话系统交互。

当前实现包含四层：

1. 插件宿主层：负责读取宿主配置、注册服务、启动本地控制台。
2. 本地控制台层：提供网页、REST API、WebSocket API。
3. Gateway 适配层：默认使用 QClaw / OpenClaw gateway WebSocket RPC；同时保留可选的 OpenResponses HTTP SSE (`/v1/responses`) 执行路径。
4. 53AIHub 桥接层：通过公司 WebSocket 完成 Bot 鉴权，并把远端消息转发到本地 Claw。

## 快速使用

发布到 npm 后，用户可以用：

```bash
npx claw-control-center install --target qclaw
npx claw-control-center install --target openclaw
```

当前仓库本地开发时，使用下面的脚本入口。

安装到 QClaw：

```bash
node plugin/bin/install-qclaw.mjs install --target qclaw
```

安装到 OpenClaw：

```bash
node plugin/bin/install-qclaw.mjs install --target openclaw
```

如果要显式指定 gateway 或密钥：

```bash
node plugin/bin/install-qclaw.mjs install \
  --target openclaw \
  --gateway ws://127.0.0.1:28789 \
  --secret <gateway-token> \
  --bot-id <bot-id>
```

如果要替代旧的 53AIHub 插件，同时配置公司服务器鉴权：

```bash
node plugin/bin/install-qclaw.mjs install \
  --target qclaw \
  --hub-ws-url "wss://kmapirc.53ai.com/api/v1/openclaw/ws/connect" \
  --hub-bot-id "<bot-id>" \
  --hub-secret "<secret>"
```

`--gateway` / `--secret` 表示本地 QClaw/OpenClaw gateway；`--hub-*` 表示公司 53AIHub 服务器。两组配置不要混用。

插件默认使用 WebSocket RPC。若要临时启用 HTTP responses 路径，可以显式传入 `--prefer-responses-api`；安装器只会在该开关开启时写入本地 Gateway 的 `gateway.http.endpoints.responses.enabled`。若要为 HTTP responses 路径指定模型，可同时使用 `--gateway-model`：

```bash
node plugin/bin/install-qclaw.mjs install \
  --target openclaw \
  --prefer-responses-api \
  --gateway-model openai/gpt-5.5
```

安装后重启对应宿主，然后访问：

```text
http://127.0.0.1:4318/
```

默认安装位置：

| target | 配置文件 | 扩展目录 |
|---|---|---|
| `qclaw` | `~/.qclaw/openclaw.json` | `~/Library/Application Support/QClaw/openclaw/config/extensions` |
| `openclaw` | `~/.openclaw/openclaw.json` | `~/.openclaw/extensions` |

## 代码入口

- 插件入口：[plugin/src/index.ts](./plugin/src/index.ts)
- 宿主配置解析：[plugin/src/host.ts](./plugin/src/host.ts)
- 本地控制台服务：[plugin/src/console-server.ts](./plugin/src/console-server.ts)
- Gateway 适配器：[plugin/src/gateway-client.ts](./plugin/src/gateway-client.ts)
- 53AIHub 桥接器：[plugin/src/53aihub-client.ts](./plugin/src/53aihub-client.ts)
- 安装器：[plugin/src/install-qclaw.ts](./plugin/src/install-qclaw.ts)
- 前端应用：[web/src/App.tsx](./web/src/App.tsx)
- 前端 API 封装：[web/src/api.ts](./web/src/api.ts)

## 文档索引

- API 文档与使用说明：[API.md](./API.md)
- 旧版 `53AI-OpenClaw` 变更记录：[LEGACY_CHANGELOG.md](./LEGACY_CHANGELOG.md)
