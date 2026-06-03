import { describe, expect, it } from "vitest";
import { readFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const __dirname = dirname(fileURLToPath(import.meta.url));
const execFileAsync = promisify(execFile);

async function readAdapterSource() {
  return readFile(join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py"), "utf8");
}

describe("Hermes 53AIHub adapter", () => {
  it("exposes frontend-compatible connection fields in runtime.get", async () => {
    const adapterSource = await readAdapterSource();

    expect(adapterSource).toContain('"connectionHealthy"');
    expect(adapterSource).toContain('"connectionStatus"');
    expect(adapterSource).toContain('"hub53ai"');
    expect(adapterSource).toContain('"enabledSkills"');
  });

  it("declares 53AIHub-owned access policy and platform-scoped allowlist env vars", async () => {
    const adapterSource = await readAdapterSource();

    expect(adapterSource).toContain("def enforces_own_access_policy");
    expect(adapterSource).toContain("return True");
    expect(adapterSource).toContain('allowed_users_env="HUB53AI_ALLOWED_USERS"');
    expect(adapterSource).toContain('allow_all_env="HUB53AI_ALLOW_ALL_USERS"');
  });

  it("registers a 53AIHub home channel env var and auto-populates it on inbound chat", async () => {
    const adapterSource = await readAdapterSource();

    expect(adapterSource).toContain('HOME_CHANNEL_ENV = "HUB53AI_HOME_CHANNEL"');
    expect(adapterSource).toContain('cron_deliver_env_var=HOME_CHANNEL_ENV');
    expect(adapterSource).toContain("def _ensure_home_channel");
    expect(adapterSource).toContain("save_env_value");
    expect(adapterSource).toContain("HomeChannel");
  });

  it("keeps stream-consumer chunks streaming but terminates plain notices and errors", async () => {
    const adapterSource = await readAdapterSource();

    expect(adapterSource).toContain("def _is_stream_consumer_send");
    expect(adapterSource).toContain("def _terminal_send_status");
    expect(adapterSource).toContain('status = "streaming" if streaming else');
    expect(adapterSource).toContain('"run.failed"');
    expect(adapterSource).toContain('"run.completed"');
  });

  it("emits Hermes progress and status updates as thinking chunks instead of terminal messages", async () => {
    const adapterPath = join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py");
    const script = String.raw`
import asyncio
import importlib.util
import json
import sys
import tempfile
import types

gateway = types.ModuleType("gateway")
gateway_config = types.ModuleType("gateway.config")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_base = types.ModuleType("gateway.platforms.base")

class Platform(str):
    def __new__(cls, value):
        return str.__new__(cls, value)

class HomeChannel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class PlatformConfig:
    def __init__(self, extra=None, enabled=True):
        self.extra = extra or {}
        self.enabled = enabled
        self.home_channel = None

class SendResult:
    def __init__(self, success=True, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error

class BasePlatformAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    def __init__(self, config=None, platform=None):
        self.config = config
        self.platform = platform
    def build_source(self, **kwargs):
        return kwargs
    async def handle_message(self, event):
        return None

class MessageType:
    TEXT = "text"

class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

gateway_config.HomeChannel = HomeChannel
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base

spec = importlib.util.spec_from_file_location("adapter_under_test", sys.argv[1])
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

async def main():
    state_path = tempfile.NamedTemporaryFile(delete=True).name
    adapter = module.Hermes53AIHubAdapter(PlatformConfig(extra={
        "bot_id": "bot",
        "secret": "secret",
        "ws_url": "ws://example.test/connect",
        "state_path": state_path,
    }))
    chunks = []
    messages = []
    events = []

    async def fake_send_chat_chunk(chat_id, content, *, status, message_id, replace, event_kind="", payload=None):
        chunks.append({
            "chat_id": chat_id,
            "content": content,
            "status": status,
            "message_id": message_id,
            "replace": replace,
            "event_kind": event_kind,
            "payload": payload,
        })

    async def fake_record_assistant_message(chat_id, message_id, content, replace=False):
        messages.append({
            "chat_id": chat_id,
            "message_id": message_id,
            "content": content,
            "replace": replace,
        })

    async def fake_record_event(chat_id, kind, payload):
        events.append({"chat_id": chat_id, "kind": kind, "payload": payload})
        return events[-1]

    adapter._send_chat_chunk = fake_send_chat_chunk
    adapter._record_assistant_message = fake_record_assistant_message
    adapter._record_event = fake_record_event

    async def _send_progress_text():
        return await adapter.send("chat-1", "🔍 search_tool: \"query\"")

    progress_result = await _send_progress_text()
    status_result = await adapter.send_or_update_status("chat-1", "provider", "⏳ Working — 1 min")

    print(json.dumps({
        "progress_success": progress_result.success,
        "status_success": status_result.success,
        "chunks": chunks,
        "messages": messages,
        "events": events,
    }))

asyncio.run(main())
`;
    const { stdout } = await execFileAsync("python3", ["-c", script, adapterPath], {
      maxBuffer: 1024 * 1024
    });
    const result = JSON.parse(stdout.trim());

    expect(result.progress_success).toBe(true);
    expect(result.status_success).toBe(true);
    expect(result.chunks.map((chunk: { status: string }) => chunk.status)).toEqual(["thinking", "thinking"]);
    expect(result.chunks.every((chunk: { replace: boolean }) => chunk.replace === true)).toBe(true);
    expect(result.messages).toEqual([]);
    expect(result.events.map((event: { kind: string }) => event.kind)).toEqual(["tool.call", "assistant.thinking"]);
    expect(result.chunks.map((chunk: { event_kind: string }) => chunk.event_kind)).toEqual(["tool.call", "assistant.thinking"]);
    expect(result.chunks[0].payload.content).toBe('🔍 search_tool: "query"');
    expect(result.events[0].payload.data).toMatchObject({
      phase: "call",
      name: "search_tool",
      args: { preview: "query" }
    });
  });

  it("uses generic visible text for tool chunks while preserving tool metadata", async () => {
    const adapterPath = join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py");
    const script = String.raw`
import asyncio
import importlib.util
import json
import sys
import tempfile
import types

gateway = types.ModuleType("gateway")
gateway_config = types.ModuleType("gateway.config")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_base = types.ModuleType("gateway.platforms.base")

class Platform(str):
    def __new__(cls, value):
        return str.__new__(cls, value)

class HomeChannel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class PlatformConfig:
    def __init__(self, extra=None, enabled=True):
        self.extra = extra or {}
        self.enabled = enabled
        self.home_channel = None

class SendResult:
    def __init__(self, success=True, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error

class BasePlatformAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    def __init__(self, config=None, platform=None):
        self.config = config
        self.platform = platform
    def build_source(self, **kwargs):
        return kwargs
    async def handle_message(self, event):
        return None

class MessageType:
    TEXT = "text"

class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

gateway_config.HomeChannel = HomeChannel
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base

spec = importlib.util.spec_from_file_location("adapter_under_test", sys.argv[1])
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

async def main():
    state_path = tempfile.NamedTemporaryFile(delete=True).name
    adapter = module.Hermes53AIHubAdapter(PlatformConfig(extra={
        "bot_id": "bot",
        "secret": "secret",
        "ws_url": "ws://example.test/connect",
        "state_path": state_path,
    }))
    frames = []

    async def fake_send_or_queue(frame):
        frames.append(frame)

    adapter._send_or_queue = fake_send_or_queue
    await adapter._upsert_conversation("chat-1", "53AI Hub：test", "req-1", "user")
    await adapter._send_chat_chunk(
        "chat-1",
        '🔍 search_tool: "query"',
        status="thinking",
        message_id="tool-1",
        replace=True,
        event_kind="tool.call",
        payload={
            "content": '🔍 search_tool: "query"',
            "data": {
                "name": "search_tool",
                "args": {"preview": "query"},
            },
        },
    )
    print(json.dumps(frames[-1]))

asyncio.run(main())
`;
    const { stdout } = await execFileAsync("python3", ["-c", script, adapterPath], {
      maxBuffer: 1024 * 1024
    });
    const frame = JSON.parse(stdout.trim());

    expect(frame.data.event_kind).toBe("tool.call");
    expect(frame.data.choices[0].delta.content).toBe("Used a tool");
    expect(frame.data.payload.content).toBe('🔍 search_tool: "query"');
    expect(frame.data.payload.data.name).toBe("search_tool");
  });

  it("terminates non-streaming final notices and provider errors", async () => {
    const adapterPath = join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py");
    const script = String.raw`
import asyncio
import importlib.util
import json
import sys
import tempfile
import types

gateway = types.ModuleType("gateway")
gateway_config = types.ModuleType("gateway.config")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_base = types.ModuleType("gateway.platforms.base")

class Platform(str):
    def __new__(cls, value):
        return str.__new__(cls, value)

class HomeChannel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class PlatformConfig:
    def __init__(self, extra=None, enabled=True):
        self.extra = extra or {}
        self.enabled = enabled
        self.home_channel = None

class SendResult:
    def __init__(self, success=True, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error

class BasePlatformAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    def __init__(self, config=None, platform=None):
        self.config = config
        self.platform = platform
    def build_source(self, **kwargs):
        return kwargs
    async def handle_message(self, event):
        return None

class MessageType:
    TEXT = "text"

class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

gateway_config.HomeChannel = HomeChannel
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base

spec = importlib.util.spec_from_file_location("adapter_under_test", sys.argv[1])
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

async def main():
    state_path = tempfile.NamedTemporaryFile(delete=True).name
    adapter = module.Hermes53AIHubAdapter(PlatformConfig(extra={
        "bot_id": "bot",
        "secret": "secret",
        "ws_url": "ws://example.test/connect",
        "state_path": state_path,
    }))
    chunks = []
    messages = []
    events = []

    async def fake_send_chat_chunk(chat_id, content, *, status, message_id, replace, event_kind="", payload=None):
        chunks.append({
            "chat_id": chat_id,
            "content": content,
            "status": status,
            "message_id": message_id,
            "replace": replace,
            "event_kind": event_kind,
            "payload": payload,
        })

    async def fake_record_assistant_message(chat_id, message_id, content, replace=False):
        messages.append({
            "chat_id": chat_id,
            "message_id": message_id,
            "content": content,
            "replace": replace,
        })

    async def fake_record_event(chat_id, kind, payload):
        events.append({"chat_id": chat_id, "kind": kind, "payload": payload})
        return events[-1]

    adapter._send_chat_chunk = fake_send_chat_chunk
    adapter._record_assistant_message = fake_record_assistant_message
    adapter._record_event = fake_record_event

    ok_result = await adapter.send("chat-1", "Done.")
    error_result = await adapter.send(
        "chat-1",
        "Sorry, I encountered an error (RuntimeError). Provider 'deepseek' is set in config.yaml but no API key was found.",
    )

    print(json.dumps({
        "ok_success": ok_result.success,
        "error_success": error_result.success,
        "chunks": chunks,
        "messages": messages,
        "events": events,
    }))

asyncio.run(main())
`;
    const { stdout } = await execFileAsync("python3", ["-c", script, adapterPath], {
      maxBuffer: 1024 * 1024
    });
    const result = JSON.parse(stdout.trim());

    expect(result.ok_success).toBe(true);
    expect(result.error_success).toBe(true);
    expect(result.chunks.map((chunk: { status: string }) => chunk.status)).toEqual(["done", "error"]);
    expect(result.messages).toHaveLength(2);
    expect(result.events.map((event: { kind: string }) => event.kind)).toEqual(["run.completed", "run.failed"]);
  });

  it("splits Hermes show_reasoning preface into a thinking event and clean final answer", async () => {
    const adapterPath = join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py");
    const script = String.raw`
import asyncio
import importlib.util
import json
import sys
import tempfile
import types

gateway = types.ModuleType("gateway")
gateway_config = types.ModuleType("gateway.config")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_base = types.ModuleType("gateway.platforms.base")

class Platform(str):
    def __new__(cls, value):
        return str.__new__(cls, value)

class HomeChannel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class PlatformConfig:
    def __init__(self, extra=None, enabled=True):
        self.extra = extra or {}
        self.enabled = enabled
        self.home_channel = None

class SendResult:
    def __init__(self, success=True, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error

class BasePlatformAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    def __init__(self, config=None, platform=None):
        self.config = config
        self.platform = platform
    def build_source(self, **kwargs):
        return kwargs
    async def handle_message(self, event):
        return None

class MessageType:
    TEXT = "text"

class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

gateway_config.HomeChannel = HomeChannel
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base

spec = importlib.util.spec_from_file_location("adapter_under_test", sys.argv[1])
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

async def main():
    state_path = tempfile.NamedTemporaryFile(delete=True).name
    adapter = module.Hermes53AIHubAdapter(PlatformConfig(extra={
        "bot_id": "bot",
        "secret": "secret",
        "ws_url": "ws://example.test/connect",
        "state_path": state_path,
    }))
    chunks = []
    messages = []
    events = []

    async def fake_send_chat_chunk(chat_id, content, *, status, message_id, replace, event_kind="", payload=None):
        chunks.append({
            "chat_id": chat_id,
            "content": content,
            "status": status,
            "message_id": message_id,
            "replace": replace,
            "event_kind": event_kind,
            "payload": payload,
        })

    async def fake_record_assistant_message(chat_id, message_id, content, replace=False):
        messages.append({
            "chat_id": chat_id,
            "message_id": message_id,
            "content": content,
            "replace": replace,
        })

    async def fake_record_event(chat_id, kind, payload):
        events.append({"chat_id": chat_id, "kind": kind, "payload": payload})
        return events[-1]

    adapter._send_chat_chunk = fake_send_chat_chunk
    adapter._record_assistant_message = fake_record_assistant_message
    adapter._record_event = fake_record_event

    fence = chr(96) * 3
    content = "💭 **Reasoning:**\n" + fence + "\nI need the city before checking weather.\n" + fence + "\n\n请问你想查询哪个城市的天气？"
    result = await adapter.send("chat-1", content)

    print(json.dumps({
        "success": result.success,
        "chunks": chunks,
        "messages": messages,
        "events": events,
    }))

asyncio.run(main())
`;
    const { stdout } = await execFileAsync("python3", ["-c", script, adapterPath], {
      maxBuffer: 1024 * 1024
    });
    const result = JSON.parse(stdout.trim());

    expect(result.success).toBe(true);
    expect(result.chunks.map((chunk: { status: string }) => chunk.status)).toEqual(["thinking", "done"]);
    expect(result.chunks[0].event_kind).toBe("assistant.thinking");
    expect(result.chunks[0].content).toBe("I need the city before checking weather.");
    expect(result.chunks[1].content).toBe("请问你想查询哪个城市的天气？");
    expect(result.messages).toHaveLength(1);
    expect(result.messages[0].content).toBe("请问你想查询哪个城市的天气？");
    expect(result.events.map((event: { kind: string }) => event.kind)).toEqual(["assistant.thinking", "run.completed"]);
  });

  it("exposes OpenClaw-shaped session ids while routing to the Hermes chat id", async () => {
    const adapterPath = join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py");
    const script = String.raw`
import asyncio
import importlib.util
import json
import sys
import tempfile
import types

gateway = types.ModuleType("gateway")
gateway_config = types.ModuleType("gateway.config")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_base = types.ModuleType("gateway.platforms.base")

class Platform(str):
    def __new__(cls, value):
        return str.__new__(cls, value)

class HomeChannel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class PlatformConfig:
    def __init__(self, extra=None, enabled=True):
        self.extra = extra or {}
        self.enabled = enabled
        self.home_channel = None

class SendResult:
    def __init__(self, success=True, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error

class BasePlatformAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    def __init__(self, config=None, platform=None):
        self.config = config
        self.platform = platform
    def build_source(self, **kwargs):
        return kwargs
    async def handle_message(self, event):
        return None

class MessageType:
    TEXT = "text"

class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

gateway_config.HomeChannel = HomeChannel
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base

spec = importlib.util.spec_from_file_location("adapter_under_test", sys.argv[1])
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

async def main():
    state_path = tempfile.NamedTemporaryFile(delete=True).name
    adapter = module.Hermes53AIHubAdapter(PlatformConfig(extra={
        "bot_id": "bot",
        "secret": "secret",
        "ws_url": "ws://example.test/connect",
        "state_path": state_path,
    }))
    frames = []
    chat_id = "agenthub_u507"
    external_id = "agent:main:53aihub:dm:agenthub_u507"

    async def fake_send_or_queue(frame):
        frames.append(frame)

    adapter._send_or_queue = fake_send_or_queue
    await adapter._upsert_conversation(chat_id, "53AI Hub-Y65NG：天气", "req-1", "Y65NG")
    await adapter._record_user_message(chat_id, "user-1", "查询天气", {"raw": {}})
    await adapter._record_event(chat_id, "assistant.thinking", {"content": "查询天气"})
    await adapter._send_chat_chunk(chat_id, "正在查询", status="thinking", message_id="thinking-1", replace=True)
    adapter._state["conversations"]["legacy_chat"] = {
        "id": "legacy_chat",
        "conversation_id": "legacy_chat",
        "session_id": "legacy_chat",
        "title": "53AI Hub-Y65NG：旧会话",
        "updatedAt": "2000-01-01T00:00:00Z",
        "createdAt": "2000-01-01T00:00:00Z",
    }
    adapter._state["conversations"][external_id] = {
        "id": external_id,
        "conversation_id": external_id,
        "session_id": external_id,
        "chat_id": external_id,
        "title": "53AI Hub-Y65NG：重复旧会话",
        "updatedAt": "1999-01-01T00:00:00Z",
        "createdAt": "1999-01-01T00:00:00Z",
    }

    sessions = await adapter._resolve_rpc("sessions.list", {"limit": 10})
    current = await adapter._resolve_rpc("sessions.current", {"chat_id": chat_id})
    messages = await adapter._resolve_rpc("sessions.messages", {"session_id": external_id})
    events = await adapter._resolve_rpc("sessions.events", {"session_id": external_id})

    print(json.dumps({
        "sessions": sessions,
        "current": current,
        "messages": messages,
        "events": events,
        "frame": frames[-1],
    }))

asyncio.run(main())
`;
    const { stdout } = await execFileAsync("python3", ["-c", script, adapterPath], {
      maxBuffer: 1024 * 1024
    });
    const result = JSON.parse(stdout.trim());

    expect(result.sessions.sessions[0].id).toBe("agent:main:53aihub:dm:agenthub_u507");
    expect(result.sessions.sessions[0].conversation_id).toBe("agent:main:53aihub:dm:agenthub_u507");
    expect(result.sessions.sessions[0].chat_id).toBe("agenthub_u507");
    expect(result.sessions.sessions).toHaveLength(2);
    expect(result.sessions.sessions[1].id).toBe("agent:main:53aihub:dm:legacy_chat");
    expect(result.sessions.sessions[1].chat_id).toBe("legacy_chat");
    expect(result.current.id).toBe("agent:main:53aihub:dm:agenthub_u507");
    expect(result.messages.messages).toHaveLength(1);
    expect(result.events.events).toHaveLength(1);
    expect(result.frame.data.conversation_id).toBe("agent:main:53aihub:dm:agenthub_u507");
    expect(result.frame.data.session_id).toBe("agent:main:53aihub:dm:agenthub_u507");
  });

  it("normalizes inbound OpenClaw-shaped conversation ids before routing to Hermes", async () => {
    const adapterPath = join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py");
    const script = String.raw`
import asyncio
import importlib.util
import json
import sys
import tempfile
import types

gateway = types.ModuleType("gateway")
gateway_config = types.ModuleType("gateway.config")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_base = types.ModuleType("gateway.platforms.base")

class Platform(str):
    def __new__(cls, value):
        return str.__new__(cls, value)

class HomeChannel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class PlatformConfig:
    def __init__(self, extra=None, enabled=True):
        self.extra = extra or {}
        self.enabled = enabled
        self.home_channel = None

class SendResult:
    def __init__(self, success=True, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error

class BasePlatformAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    def __init__(self, config=None, platform=None):
        self.config = config
        self.platform = platform
    def build_source(self, **kwargs):
        return kwargs
    async def handle_message(self, event):
        return None

class MessageType:
    TEXT = "text"

class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

gateway_config.HomeChannel = HomeChannel
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base

spec = importlib.util.spec_from_file_location("adapter_under_test", sys.argv[1])
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

async def main():
    state_path = tempfile.NamedTemporaryFile(delete=True).name
    adapter = module.Hermes53AIHubAdapter(PlatformConfig(extra={
        "bot_id": "bot",
        "secret": "secret",
        "ws_url": "ws://example.test/connect",
        "state_path": state_path,
    }))
    handled = []

    async def fake_handle_message(event):
        handled.append({
            "source_chat_id": event.source["chat_id"],
            "source_chat_name": event.source["chat_name"],
            "raw_chat_id": event.raw_message["data"]["conversation_id"],
        })

    adapter.handle_message = fake_handle_message
    await adapter._handle_raw_message(json.dumps({
        "action": "chat",
        "data": {
            "conversation_id": "agent:main:53aihub:dm:agenthub_u507",
            "msgId": "msg-1",
            "content": "继续查询天气",
            "user": {"id": "agenthub_u507", "name": "Y65NG"},
            "metadata": {"conversationTitle": "53AI Hub-Y65NG：天气"},
        },
    }))
    sessions = await adapter._resolve_rpc("sessions.list", {"limit": 10})
    messages = await adapter._resolve_rpc("sessions.messages", {
        "session_id": "agent:main:53aihub:dm:agenthub_u507",
    })
    legacy_session_id = "agent:main:53aihub:dm:legacy_chat"
    adapter._state["messages"] = {
        legacy_session_id: [{
            "id": "legacy-msg",
            "role": "assistant",
            "content": "旧状态消息",
        }],
    }
    adapter._state["events"] = {
        legacy_session_id: [{
            "id": "legacy-event",
            "sessionId": "legacy_chat",
            "session_id": "legacy_chat",
            "seq": 7,
            "kind": "run.completed",
            "payload": {"ok": True},
        }],
    }
    legacy_messages = await adapter._resolve_rpc("sessions.messages", {
        "session_id": legacy_session_id,
    })

    print(json.dumps({
        "handled": handled,
        "state_keys": list(adapter._state["conversations"].keys()),
        "sessions": sessions,
        "messages": messages,
        "legacy_messages": legacy_messages,
    }))

asyncio.run(main())
`;
    const { stdout } = await execFileAsync("python3", ["-c", script, adapterPath], {
      maxBuffer: 1024 * 1024
    });
    const result = JSON.parse(stdout.trim());

    expect(result.handled[0].source_chat_id).toBe("agenthub_u507");
    expect(result.state_keys).toEqual(["agenthub_u507"]);
    expect(result.sessions.sessions[0].id).toBe("agent:main:53aihub:dm:agenthub_u507");
    expect(result.messages.messages[0].content).toBe("继续查询天气");
    expect(result.legacy_messages.messages[0].content).toBe("旧状态消息");
    expect(result.legacy_messages.events[0].sessionId).toBe("agent:main:53aihub:dm:legacy_chat");
  });

  it("maps sessions.control stop to Hermes active session cancellation", async () => {
    const adapterPath = join(__dirname, "..", "hermes", "platforms", "53aihub", "adapter.py");
    const script = String.raw`
import asyncio
import importlib.util
import json
import sys
import tempfile
import types

gateway = types.ModuleType("gateway")
gateway_config = types.ModuleType("gateway.config")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_base = types.ModuleType("gateway.platforms.base")

class Platform(str):
    def __new__(cls, value):
        return str.__new__(cls, value)

class HomeChannel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class PlatformConfig:
    def __init__(self, extra=None, enabled=True):
        self.extra = extra or {}
        self.enabled = enabled
        self.home_channel = None

class SendResult:
    def __init__(self, success=True, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error

class BasePlatformAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    def __init__(self, config=None, platform=None):
        self.config = config
        self.platform = platform
        self._active_sessions = {}
        self._session_tasks = {}
        self._pending_messages = {}
    def build_source(self, **kwargs):
        return kwargs
    async def handle_message(self, event):
        return None
    async def interrupt_session_activity(self, session_key, chat_id):
        return None

class MessageType:
    TEXT = "text"

class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

gateway_config.HomeChannel = HomeChannel
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base

spec = importlib.util.spec_from_file_location("adapter_under_test", sys.argv[1])
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

async def main():
    state_path = tempfile.NamedTemporaryFile(delete=True).name
    adapter = module.Hermes53AIHubAdapter(PlatformConfig(extra={
        "bot_id": "bot",
        "secret": "secret",
        "ws_url": "ws://example.test/connect",
        "state_path": state_path,
    }))
    calls = []

    async def fake_cancel_session_processing(session_key, *, release_guard=True, discard_pending=True):
        calls.append({
            "session_key": session_key,
            "release_guard": release_guard,
            "discard_pending": discard_pending,
        })

    async def fake_record_event(chat_id, kind, payload):
        calls.append({"event": kind, "chat_id": chat_id, "payload": payload})
        return calls[-1]

    adapter.cancel_session_processing = fake_cancel_session_processing
    adapter._record_event = fake_record_event
    result = await adapter._resolve_rpc("sessions.control", {
        "session_id": "chat-1",
        "action": "stop",
    })
    print(json.dumps({"result": result, "calls": calls}))

asyncio.run(main())
`;
    const { stdout } = await execFileAsync("python3", ["-c", script, adapterPath], {
      maxBuffer: 1024 * 1024
    });
    const result = JSON.parse(stdout.trim());

    expect(result.result).toMatchObject({
      ok: true,
      action: "stop",
      session_id: "agent:main:53aihub:dm:chat-1",
      conversation_id: "agent:main:53aihub:dm:chat-1"
    });
    expect(result.result.unsupported).toBeFalsy();
    expect(result.calls[0]).toMatchObject({
      session_key: "agent:main:53aihub:dm:chat-1",
      release_guard: true,
      discard_pending: true
    });
    expect(result.calls[1]).toMatchObject({
      event: "run.interrupted",
      chat_id: "chat-1"
    });
  });
});
