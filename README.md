# Claw Control Center

`claw-control-center` 是一个运行在 QClaw / OpenClaw 宿主中的多会话控制插件。  
它会在宿主内启动一个本地控制台 (local control console)，并通过本地 gateway (gateway) 与 QClaw / OpenClaw 会话系统交互。

当前实现包含三层：

1. 插件宿主层：负责读取宿主配置、注册服务、启动本地控制台。
2. 本地控制台层：提供网页、REST API、WebSocket API。
3. Gateway 适配层：把本地控制台请求转换为 QClaw / OpenClaw gateway RPC。

## 快速使用

发布到 npm 后，用户可以用：

```bash
npx @claw-plugin/claw-control-center install --target qclaw
npx @claw-plugin/claw-control-center install --target openclaw
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
- 安装器：[plugin/src/install-qclaw.ts](./plugin/src/install-qclaw.ts)
- 前端应用：[web/src/App.tsx](./web/src/App.tsx)
- 前端 API 封装：[web/src/api.ts](./web/src/api.ts)

## 文档索引

- API 文档与使用说明：[API.md](./API.md)
