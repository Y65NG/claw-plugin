#!/usr/bin/env bash
set -euo pipefail

: <<'CLAW_AGENT_INSTRUCTIONS'
请将 53AI-OpenClaw 插件安装到当前正在运行的 Claw 中。

如果你能获取当前 Claw 的配置路径和插件目录，请在执行安装时自动补充：
  --config-path "<当前 Claw 的配置路径，例如 openclaw.json 或 Hermes config.yaml>"
  --extensions-dir "<当前 Claw 的插件目录，例如 extensions 或 Hermes plugins/platforms>"

如果你不能确定当前 Claw 的路径，请直接执行本脚本。
脚本会调用安装器自动检测本机已安装的 Claw；如果检测到多个 Claw，请选择当前正在运行的这个 Claw。
不要安装到其他 Claw 的目录。
CLAW_AGENT_INSTRUCTIONS

echo "Installing 53AI-OpenClaw into the current Claw host..."
echo "The installer will auto-detect local Claw paths and ask when multiple hosts are found."

if ! command -v npx >/dev/null 2>&1; then
  echo "npx is required to install claw-control-center. Please install Node.js and npm first." >&2
  exit 1
fi

exec npx --yes --registry=https://registry.npmjs.org/ claw-control-center install "$@"
