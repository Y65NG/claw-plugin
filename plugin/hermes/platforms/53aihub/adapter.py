from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import inspect
import json
import logging
import mimetypes
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlparse, urlunparse

from gateway.config import HomeChannel, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

try:
    from hermes_constants import get_hermes_home
except Exception:  # pragma: no cover - older Hermes fallback
    def get_hermes_home() -> str:
        return os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))


logger = logging.getLogger(__name__)

PLATFORM_NAME = "53aihub"
MODEL_NAME = "hermes-agent"
STATE_FILENAME = "53aihub-state.json"
SESSION_ID_PREFIX = f"agent:main:{PLATFORM_NAME}:dm:"
HEARTBEAT_INTERVAL_SECONDS = 30
RECONNECT_BASE_SECONDS = 2
RECONNECT_MAX_SECONDS = 30
MAX_STORED_OUTBOX = 200
MAX_OUTPUT_FILE_INLINE_BYTES = 512 * 1024
HOME_CHANNEL_ENV = "HUB53AI_HOME_CHANNEL"
HOME_CHANNEL_THREAD_ENV = "HUB53AI_HOME_CHANNEL_THREAD_ID"

RPC_ACTIONS = {
    "sessions.list",
    "sessions.current",
    "sessions.messages",
    "sessions.events",
    "sessions.snapshot",
    "sessions.control",
    "runtime.get",
    "cron.tasks",
}


def check_requirements() -> bool:
    if not os.getenv("HUB53AI_BOT_ID"):
        logger.debug("53AIHub: HUB53AI_BOT_ID not set")
        return False
    if not os.getenv("HUB53AI_SECRET"):
        logger.debug("53AIHub: HUB53AI_SECRET not set")
        return False
    if not os.getenv("HUB53AI_WS_URL"):
        logger.debug("53AIHub: HUB53AI_WS_URL not set")
        return False
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        logger.warning("53AIHub: aiohttp is required for WebSocket transport")
        return False
    return True


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _now_unix() -> int:
    return int(time.time())


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _string_or(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return ""


def _int_or(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _coerce_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            record = _as_dict(item)
            if record.get("type") == "text":
                parts.append(_string_or(record.get("text")))
            elif "text" in record:
                parts.append(_string_or(record.get("text")))
        return "\n".join(part for part in parts if part)
    return ""


def _extract_latest_user_message(messages: Any) -> str:
    for message in reversed(_as_list(messages)):
        record = _as_dict(message)
        if record.get("role") == "user":
            text = _coerce_text(record.get("content"))
            if text:
                return text
    return ""


def _parse_tool_progress_text(content: str) -> Optional[tuple[str, str]]:
    text = _string_or(content)
    if not text:
        return None
    # Hermes progress lines are usually rendered as:
    #   "🔍 web_extract: \"https://...\""
    #   "🧠 memory: \"list\""
    #   "⚙️ execute_code..."
    #   "🔧 terminal(['cmd'])\n{...}"
    match = re.match(
        r"^\s*(?:[^\w\s./-]+(?:\ufe0f)?\s+)?"
        r"(?P<name>[A-Za-z_][\w.-]*)"
        r"(?:\((?P<keys>[^)]*)\))?"
        r"(?:\s*:\s*[\"“](?P<quoted>.*?)[\"”]\s*|\s*:\s*(?P<plain>.+?)\s*|\s*\.{3}\s*|\s*$)",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return None
    name = _string_or(match.group("name"))
    if not name or name.lower() in {"working", "loading", "thinking"}:
        return None
    preview = _string_or(match.group("quoted"), match.group("plain"), match.group("keys"))
    return name, preview


def _split_reasoning_preface(content: str) -> tuple[str, str]:
    text = _string_or(content)
    if not text:
        return "", ""
    match = re.match(
        r"^\s*(?:💭\s*)?\*\*Reasoning:\*\*\s*"
        r"```(?:[A-Za-z0-9_-]+)?\s*\n(?P<reasoning>.*?)\n```\s*"
        r"(?P<answer>.*)$",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return "", text
    reasoning = _string_or(match.group("reasoning"))
    answer = _string_or(match.group("answer"))
    return reasoning, answer


def _parse_incoming_message(raw_payload: str) -> Optional[Dict[str, Any]]:
    try:
        frame = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(frame, dict):
        return None

    action = _string_or(frame.get("action"), frame.get("type")).lower()
    if action not in {"chat", "message"}:
        return None

    data = _as_dict(frame.get("data"))
    req_id = _string_or(frame.get("req_id"), frame.get("reqId"), data.get("req_id"), data.get("reqId"), data.get("msgId"))
    user_obj = _as_dict(data.get("user"))
    metadata = _as_dict(data.get("metadata"))
    chat_id = _string_or(
        data.get("conversation_id"),
        data.get("conversationId"),
        data.get("chatId"),
        data.get("chat_id"),
        frame.get("conversation_id"),
    )
    user_id = _string_or(data.get("user_id"), data.get("userId"), data.get("user"), user_obj.get("id"), user_obj.get("userId"))
    user_name = _string_or(
        data.get("user_name"),
        data.get("userName"),
        metadata.get("user_name"),
        metadata.get("userName"),
        user_obj.get("name"),
        user_obj.get("userName"),
        user_obj.get("username"),
    )
    text = _string_or(data.get("content"), data.get("text"), data.get("message")) or _extract_latest_user_message(data.get("messages"))
    if not chat_id:
        chat_id = user_id or req_id
    if not user_id:
        user_id = chat_id
    if not req_id:
        req_id = str(uuid.uuid4())
    if not chat_id or not user_id or not text:
        return None

    return {
        "type": action,
        "reqId": req_id,
        "msgId": _string_or(data.get("msgId"), data.get("message_id"), req_id),
        "chatId": chat_id,
        "userId": user_id,
        "userName": user_name,
        "text": text,
        "conversationTitle": _string_or(data.get("conversation_title"), data.get("conversationTitle"), metadata.get("conversationTitle")),
        "raw": frame,
    }


def _parse_rpc_request(raw_payload: str) -> Optional[Dict[str, Any]]:
    try:
        frame = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(frame, dict):
        return None
    action = _string_or(frame.get("action"))
    if action not in RPC_ACTIONS:
        return None
    return {
        "reqId": _string_or(frame.get("req_id"), frame.get("reqId"), str(uuid.uuid4())),
        "action": action,
        "data": frame.get("data"),
    }


class Hermes53AIHubAdapter(BasePlatformAdapter):
    MAX_MESSAGE_LENGTH = 16000
    SUPPORTS_MESSAGE_EDITING = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        extra = config.extra or {}
        self.bot_id = _string_or(extra.get("bot_id"), extra.get("hub_bot_id"), os.getenv("HUB53AI_BOT_ID"))
        self.secret = _string_or(extra.get("secret"), extra.get("hub_secret"), os.getenv("HUB53AI_SECRET"))
        self.ws_url = _string_or(extra.get("ws_url"), extra.get("hub_ws_url"), os.getenv("HUB53AI_WS_URL"))
        self.artifact_upload_url = _string_or(
            extra.get("artifact_upload_url"),
            extra.get("hub_artifact_upload_url"),
            os.getenv("HUB53AI_ARTIFACT_UPLOAD_URL"),
        )
        self.inline_file_max_bytes = _int_or(
            _string_or(extra.get("inline_file_max_bytes"), os.getenv("HUB53AI_INLINE_FILE_MAX_BYTES")),
            MAX_OUTPUT_FILE_INLINE_BYTES,
        )
        state_path = _string_or(extra.get("state_path"), os.getenv("HUB53AI_STATE_PATH"))
        self.state_path = Path(state_path) if state_path else Path(get_hermes_home()) / STATE_FILENAME
        self._session: Any = None
        self._ws: Any = None
        self._ws_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._closing = False
        self._seq = 0
        self._state_lock = asyncio.Lock()
        self._state: Dict[str, Any] = {
            "conversations": {},
            "messages": {},
            "events": {},
            "outbox": [],
        }

    @property
    def enforces_own_access_policy(self) -> bool:
        return True

    async def connect(self) -> bool:
        import aiohttp

        if not self.bot_id or not self.secret or not self.ws_url:
            logger.error("53AIHub: HUB53AI_BOT_ID, HUB53AI_SECRET and HUB53AI_WS_URL are required")
            return False

        await self._load_state()
        self._closing = False
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None))
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._mark_connected()
        logger.info("53AIHub: adapter started for bot %s", self._masked_bot_id())
        return True

    async def disconnect(self) -> None:
        self._closing = True
        for task in (self._heartbeat_task, self._ws_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._heartbeat_task = None
        self._ws_task = None
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        await self._persist_state()
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        message_id = f"assistant:{uuid.uuid4()}"
        streaming = self._is_stream_consumer_send()
        if not streaming and self._is_hermes_interim_send(metadata):
            thinking_message_id = f"thinking:{uuid.uuid4()}"
            await self._emit_thinking_update(
                chat_id,
                thinking_message_id,
                content,
                replace=True,
                source="hermes-progress",
            )
            return SendResult(success=True, message_id=thinking_message_id)

        reasoning, final_content = _split_reasoning_preface(content)
        if reasoning and final_content:
            await self._emit_thinking_update(
                chat_id,
                f"thinking:{uuid.uuid4()}",
                reasoning,
                replace=True,
                source="hermes-reasoning",
            )

        status = "streaming" if streaming else self._terminal_send_status(final_content)
        await self._record_assistant_message(chat_id, message_id, final_content)
        event_payload: Optional[Dict[str, Any]] = None
        if not streaming:
            event_kind = "run.failed" if status == "error" else "run.completed"
            event = await self._record_event(chat_id, event_kind, {"message_id": message_id, "content": final_content})
            event_payload = _as_dict(event.get("payload"))
        await self._send_chat_chunk(
            chat_id,
            final_content,
            status=status,
            message_id=message_id,
            replace=False,
            payload=event_payload,
        )
        return SendResult(success=True, message_id=message_id)

    async def send_or_update_status(
        self,
        chat_id: str,
        status_key: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        message_id = f"status:{chat_id}:{status_key or 'default'}"
        await self._emit_thinking_update(
            chat_id,
            message_id,
            content,
            replace=True,
            source="hermes-status",
            status_key=status_key,
        )
        return SendResult(success=True, message_id=message_id)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        if not finalize and self._is_hermes_interim_edit():
            await self._emit_thinking_update(
                chat_id,
                message_id,
                content,
                replace=True,
                source="hermes-progress",
            )
            return SendResult(success=True, message_id=message_id)

        reasoning, final_content = _split_reasoning_preface(content)
        if reasoning and final_content:
            await self._emit_thinking_update(
                chat_id,
                f"thinking:{message_id}",
                reasoning,
                replace=True,
                source="hermes-reasoning",
            )

        await self._record_assistant_message(chat_id, message_id, final_content, replace=True)
        event_payload: Optional[Dict[str, Any]] = None
        if finalize:
            event = await self._record_event(
                chat_id,
                "run.completed",
                {"ok": True, "message_id": message_id, "content": final_content},
            )
            event_payload = _as_dict(event.get("payload"))
        await self._send_chat_chunk(
            chat_id,
            final_content,
            status="done" if finalize else "streaming",
            message_id=message_id,
            replace=True,
            payload=event_payload,
        )
        return SendResult(success=True, message_id=message_id)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_single_output_file(
            chat_id=chat_id,
            file_path=file_path,
            caption=caption,
            file_name=file_name,
            media_kind="file",
            metadata=metadata,
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_single_output_file(
            chat_id=chat_id,
            file_path=image_path,
            caption=caption,
            media_kind="image",
            metadata=metadata,
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_single_output_file(
            chat_id=chat_id,
            file_path=audio_path,
            caption=caption,
            media_kind="audio",
            metadata=metadata,
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_single_output_file(
            chat_id=chat_id,
            file_path=video_path,
            caption=caption,
            media_kind="video",
            metadata=metadata,
        )

    async def send_multiple_images(
        self,
        chat_id: str,
        images: List[tuple[str, str]],
        metadata: Optional[Dict[str, Any]] = None,
        human_delay: float = 0.0,
    ) -> None:
        files: List[Dict[str, Any]] = []
        errors: List[str] = []
        for image_url, alt_text in images:
            if human_delay > 0:
                await asyncio.sleep(human_delay)
            try:
                files.append(await self._output_file_from_image_reference(chat_id, image_url, alt_text, metadata))
            except Exception as exc:
                errors.append(f"{image_url}: {exc}")
        if files:
            await self._send_output_files_step(chat_id, files, caption=None)
        if errors:
            logger.warning("53AIHub: failed to deliver %d Hermes image attachment(s): %s", len(errors), "; ".join(errors))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        session = await self._latest_conversation(chat_id)
        name = _string_or(
            session.get("title") if session else None,
            session.get("name") if session else None,
            chat_id,
        )
        return {
            "name": name,
            "type": "dm",
            "chat_id": chat_id,
        }

    async def _ws_loop(self) -> None:
        import aiohttp

        delay = RECONNECT_BASE_SECONDS
        while not self._closing:
            try:
                headers = self._auth_headers()
                async with self._session.ws_connect(self.ws_url, headers=headers, heartbeat=HEARTBEAT_INTERVAL_SECONDS) as ws:
                    self._ws = ws
                    delay = RECONNECT_BASE_SECONDS
                    logger.info("53AIHub: connected to %s", self._safe_ws_url())
                    await self._send_app_ping()
                    await self._replay_outbox()
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_raw_message(message.data)
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            logger.warning("53AIHub: websocket error: %s", ws.exception())
                            break
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if not self._closing:
                    logger.warning("53AIHub: websocket disconnected: %s", exc)
            finally:
                self._ws = None

            if not self._closing:
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_SECONDS)

    async def _heartbeat_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            await self._send_app_ping()

    async def _handle_raw_message(self, raw_payload: str) -> None:
        heartbeat = raw_payload.strip().lower()
        if heartbeat == "ping":
            await self._send_raw(json.dumps({"action": "pong", "data": {"botId": self.bot_id}}))
            return
        if heartbeat == "pong":
            return

        rpc = _parse_rpc_request(raw_payload)
        if rpc is not None:
            await self._handle_rpc_request(rpc)
            return

        incoming = _parse_incoming_message(raw_payload)
        if incoming is None:
            return
        await self._handle_incoming_chat(incoming)

    async def _handle_incoming_chat(self, incoming: Dict[str, Any]) -> None:
        chat_id = self._internal_chat_id(incoming["chatId"])
        msg_id = incoming["msgId"]
        title = self._conversation_title(incoming)
        self._ensure_home_channel(chat_id, title)
        await self._upsert_conversation(chat_id, title, incoming["reqId"], incoming.get("userName") or incoming["userId"])
        await self._record_user_message(chat_id, msg_id, incoming["text"], incoming)
        await self._record_event(chat_id, "user.message", {"message_id": msg_id, "content": incoming["text"]})

        source = self.build_source(
            chat_id=chat_id,
            chat_name=title,
            chat_type="dm",
            user_id=incoming["userId"],
            user_name=incoming.get("userName") or incoming["userId"],
            message_id=msg_id,
        )
        event = MessageEvent(
            text=incoming["text"],
            message_type=MessageType.TEXT,
            source=source,
            raw_message=incoming["raw"],
            message_id=msg_id,
        )
        await self.handle_message(event)

    async def _handle_rpc_request(self, request: Dict[str, Any]) -> None:
        try:
            data = await self._resolve_rpc(request["action"], request.get("data"))
            await self._send_raw(json.dumps({
                "req_id": request["reqId"],
                "action": request["action"],
                "status": "done",
                "data": data,
            }, ensure_ascii=False))
        except Exception as exc:
            await self._send_raw(json.dumps({
                "req_id": request["reqId"],
                "action": request["action"],
                "status": "error",
                "data": {
                    "code": "HERMES_RPC_ERROR",
                    "message": str(exc),
                },
            }, ensure_ascii=False))

    async def _resolve_rpc(self, action: str, payload: Any) -> Any:
        record = _as_dict(payload)
        if action == "sessions.list":
            limit, offset = self._pagination(record, 50)
            sessions = await self._list_conversations()
            page = sessions[offset:offset + limit]
            return {
                "sessions": page,
                "conversations": page,
                "pagination": self._pagination_response(limit, offset, len(sessions), len(page)),
            }
        if action == "sessions.current":
            chat_id = _string_or(record.get("chat_id"), record.get("chatId"), record.get("user"), record.get("user_id"), record.get("userId"))
            session = await self._latest_conversation(chat_id)
            return session
        if action == "sessions.messages":
            session_id = self._read_session_id(record)
            chat_id = self._internal_chat_id(session_id)
            limit, offset = self._pagination(record, 100)
            messages = await self._messages_for(chat_id)
            events = await self._events_for(chat_id, 0)
            ledger_events = await self._ledger_events_for(chat_id, 0)
            page = self._slice_latest(messages, limit, offset)
            total = len(messages)
            return {
                "messages": page,
                "events": events,
                "ledger_events": ledger_events,
                "ledgerEvents": ledger_events,
                "pagination": self._pagination_response(limit, offset, total, len(page)),
            }
        if action == "sessions.events":
            session_id = self._read_session_id(record)
            chat_id = self._internal_chat_id(session_id)
            limit, offset = self._pagination(record, 100)
            after_seq = int(record.get("after_seq") or record.get("afterSeq") or 0)
            events = await self._events_for(chat_id, after_seq)
            ledger_events = await self._ledger_events_for(chat_id, after_seq)
            page = events[offset:offset + limit]
            return {
                "events": page,
                "ledger_events": ledger_events,
                "ledgerEvents": ledger_events,
                "pagination": self._pagination_response(limit, offset, len(events), len(page)),
            }
        if action == "sessions.snapshot":
            session_id = self._read_session_id(record)
            chat_id = self._internal_chat_id(session_id)
            after_seq = int(record.get("after_seq") or record.get("afterSeq") or 0)
            return await self._session_snapshot(chat_id, after_seq)
        if action == "sessions.control":
            session_id = self._read_session_id(record)
            chat_id = self._internal_chat_id(session_id)
            control_action = _string_or(record.get("action"), "stop")
            if control_action != "stop":
                return {
                    "ok": False,
                    "action": control_action,
                    "session_id": session_id,
                    "conversation_id": session_id,
                    "unsupported": True,
                    "message": f"Hermes 53AIHub adapter does not support {control_action!r} control yet.",
                }
            return await self._stop_session(chat_id)
        if action == "runtime.get":
            connected = self._ws is not None
            connection_status = "connected" if connected else "disconnected"
            return {
                "healthy": connected,
                "connectionHealthy": connected,
                "connectionStatus": connection_status,
                "hub53ai": {
                    "connectionStatus": connection_status,
                    "connected": connected,
                    "botId": self._masked_bot_id(),
                    "wsUrl": self._safe_ws_url(),
                },
                "status": {
                    "platform": PLATFORM_NAME,
                    "connected": connected,
                    "connectionStatus": connection_status,
                    "botId": self._masked_bot_id(),
                    "wsUrl": self._safe_ws_url(),
                },
                "gatewayHealth": {
                    "status": "ok" if connected else "disconnected",
                },
                "config": {
                    "platform": PLATFORM_NAME,
                    "streaming": True,
                },
                "skills": [],
                "enabledSkills": [],
                "cronTasks": [],
            }
        if action == "cron.tasks":
            limit, offset = self._pagination(record, 100)
            return {
                "tasks": [],
                "cronTasks": [],
                "pagination": self._pagination_response(limit, offset, 0, 0),
            }
        raise ValueError(f"Unsupported RPC action: {action}")

    async def _send_chat_chunk(
        self,
        chat_id: str,
        content: str,
        *,
        status: str,
        message_id: str,
        replace: bool,
        event_kind: str = "",
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        req_id = await self._req_id_for(chat_id)
        session_id = self._external_session_id(chat_id)
        delta_content = content
        if event_kind == "tool.call":
            delta_content = "Used a tool"
        elif event_kind == "tool.result":
            delta_content = "Tool returned a result"
        chunk_payload = payload
        if chunk_payload is None:
            chunk_payload = self._payload_with_openclaw_ledger(
                chat_id,
                self._event_kind_for_chunk_status(status, event_kind),
                {
                    "message_id": message_id,
                    "content": content,
                    "mode": "replace" if replace else "append",
                    "replace": replace,
                },
                max(self._seq + 1, 1),
                f"{chat_id}:chunk:{message_id}",
                _now_iso(),
            )
        frame = {
            "req_id": req_id,
            "action": "chat",
            "status": status,
            "data": {
                "id": message_id,
                "object": "chat.completion.chunk",
                "created": _now_unix(),
                "model": MODEL_NAME,
                "status": status,
                "mode": "replace" if replace else "append",
                "replace": replace,
                **({"event_kind": event_kind} if event_kind else {}),
                **({"payload": chunk_payload} if chunk_payload else {}),
                "session_id": session_id,
                "conversation_id": session_id,
                "chat_id": chat_id,
                "choices": [{
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": delta_content,
                    },
                    "finish_reason": "stop" if status == "done" else None,
                }],
            },
        }
        await self._send_or_queue(frame)

    async def _emit_thinking_update(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        replace: bool,
        source: str,
        status_key: str = "",
    ) -> None:
        text = _string_or(content)
        if not text:
            return
        payload: Dict[str, Any] = {
            "message_id": message_id,
            "content": text,
            "source": source,
            "mode": "replace" if replace else "append",
            "replace": replace,
        }
        if status_key:
            payload["status_key"] = status_key
        event_kind = "assistant.thinking"
        event_payload = payload
        if source == "hermes-progress":
            tool_payload = self._tool_call_payload_from_progress(message_id, text, payload)
            if tool_payload is not None:
                event_kind = "tool.call"
                event_payload = tool_payload

        event = await self._record_event(chat_id, event_kind, event_payload)
        await self._send_chat_chunk(
            chat_id,
            text,
            status="thinking",
            message_id=message_id,
            replace=replace,
            event_kind=event_kind,
            payload=_as_dict(event.get("payload")),
        )

    async def _send_single_output_file(
        self,
        *,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        media_kind: str,
        file_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        try:
            file = await self._build_local_output_file(
                chat_id=chat_id,
                file_path=file_path,
                file_name=file_name,
                media_kind=media_kind,
                metadata=metadata,
            )
            return await self._send_output_files_step(chat_id, [file], caption=caption)
        except Exception as exc:
            logger.warning("53AIHub: failed to deliver Hermes output file %s: %s", file_path, exc)
            return SendResult(success=False, error=str(exc))

    async def _output_file_from_image_reference(
        self,
        chat_id: str,
        image_url: str,
        alt_text: str,
        metadata: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        value = _string_or(image_url)
        parsed = urlparse(value)
        if parsed.scheme == "file":
            path_value = unquote(parsed.path or "")
            if parsed.netloc and not path_value.startswith("/"):
                path_value = f"{parsed.netloc}/{path_value}"
            return await self._build_local_output_file(
                chat_id=chat_id,
                file_path=path_value,
                media_kind="image",
                metadata=metadata,
            )
        if parsed.scheme in {"http", "https"}:
            return self._build_remote_output_file(value, alt_text, "image")
        if Path(value).expanduser().exists():
            return await self._build_local_output_file(
                chat_id=chat_id,
                file_path=value,
                media_kind="image",
                metadata=metadata,
            )
        return self._build_remote_output_file(value, alt_text, "image")

    async def _build_local_output_file(
        self,
        *,
        chat_id: str,
        file_path: str,
        media_kind: str,
        file_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        path = Path(file_path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(file_path)
        path = path.resolve()
        resolved_name = _string_or(file_name, path.name, "file")
        mime_type = mimetypes.guess_type(resolved_name)[0] or "application/octet-stream"
        size = path.stat().st_size
        sha256 = await asyncio.to_thread(self._sha256_file, path)
        active_request_id = self._ledger_active_request_id(chat_id)
        turn_id = self._ledger_turn_id(chat_id, active_request_id)
        part_id = self._output_file_part_id(chat_id, resolved_name, sha256)

        if self._ws is not None or self.artifact_upload_url:
            uploaded = await self._upload_output_file(
                chat_id=chat_id,
                path=path,
                file_name=resolved_name,
                mime_type=mime_type,
                sha256=sha256,
                turn_id=turn_id,
                active_request_id=active_request_id,
                part_id=part_id,
            )
            if uploaded:
                uploaded.setdefault("source_kind", "hermes_generated")
                uploaded.setdefault("sha256", sha256)
                uploaded["kind"] = media_kind
                return uploaded

        if size > self.inline_file_max_bytes:
            raise ValueError(
                f"{resolved_name} is {size} bytes, above inline limit {self.inline_file_max_bytes}; artifact upload is unavailable"
            )

        content = await asyncio.to_thread(path.read_bytes)
        return {
            "id": f"hermes-output:{sha256[:16]}",
            "file_name": resolved_name,
            "mime_type": mime_type,
            "size": size,
            "sha256": sha256,
            "base64": base64.b64encode(content).decode("ascii"),
            "source_kind": "hermes_generated",
            "kind": media_kind,
        }

    def _build_remote_output_file(self, url: str, label: str, media_kind: str) -> Dict[str, Any]:
        parsed = urlparse(url)
        file_name = Path(unquote(parsed.path or "")).name or _string_or(label, "file")
        mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return {
            "id": f"hermes-output:{digest[:16]}",
            "file_name": file_name,
            "url": url,
            "preview_url": url if media_kind == "image" else "",
            "mime_type": mime_type,
            "sha256": digest,
            "source_kind": "hermes_remote",
            "kind": media_kind,
        }

    async def _upload_output_file(
        self,
        *,
        chat_id: str,
        path: Path,
        file_name: str,
        mime_type: str,
        sha256: str,
        turn_id: str,
        active_request_id: str,
        part_id: str,
    ) -> Optional[Dict[str, Any]]:
        upload_url = self._artifact_upload_endpoint()
        if not upload_url:
            return None
        try:
            import aiohttp
        except ImportError:
            return None

        owns_session = self._session is None or getattr(self._session, "closed", False)
        session = self._session
        if owns_session:
            session = aiohttp.ClientSession()
        try:
            form = aiohttp.FormData()
            form.add_field("agent_id", self.bot_id)
            form.add_field("user_id", self._internal_chat_id(chat_id))
            form.add_field("conversation_id", self._external_session_id(chat_id))
            form.add_field("turn_id", turn_id)
            form.add_field("active_request_id", active_request_id)
            form.add_field("part_id", part_id)
            form.add_field("logical_path", file_name)
            with path.open("rb") as file_obj:
                form.add_field("file", file_obj, filename=file_name, content_type=mime_type)
                async with session.post(upload_url, data=form, headers=self._auth_headers()) as response:
                    if response.status < 200 or response.status >= 300:
                        logger.warning("53AIHub: artifact upload failed status=%s file=%s", response.status, file_name)
                        return None
                    body = await response.json(content_type=None)
            return self._normalize_uploaded_output_file(body, fallback_name=file_name, fallback_mime=mime_type, fallback_sha256=sha256)
        except Exception as exc:
            logger.warning("53AIHub: artifact upload failed for %s: %s", file_name, exc)
            return None
        finally:
            if owns_session and session is not None:
                await session.close()

    def _artifact_upload_endpoint(self) -> str:
        explicit = _string_or(self.artifact_upload_url)
        if explicit:
            return explicit
        parsed = urlparse(self.ws_url)
        if parsed.scheme not in {"ws", "wss", "http", "https"} or not parsed.netloc:
            return ""
        scheme = "https" if parsed.scheme in {"wss", "https"} else "http"
        path = parsed.path or ""
        marker = "/v1/openclaw/ws"
        if marker in path:
            upload_path = path.split(marker, 1)[0] + "/v1/openclaw/artifacts"
        else:
            upload_path = "/api/v1/openclaw/artifacts"
        return urlunparse((scheme, parsed.netloc, upload_path, "", "", ""))

    def _normalize_uploaded_output_file(
        self,
        response_body: Any,
        *,
        fallback_name: str,
        fallback_mime: str,
        fallback_sha256: str,
    ) -> Optional[Dict[str, Any]]:
        body = _as_dict(response_body)
        data = _as_dict(body.get("data")) or body
        file_id = _string_or(data.get("id"), data.get("artifact_id"), data.get("upload_file_id"))
        preview_url = _string_or(data.get("preview_url"), data.get("previewUrl"), data.get("url"))
        download_url = _string_or(data.get("download_url"), data.get("downloadUrl"))
        signed_download_url = _string_or(data.get("signed_download_url"), data.get("signedDownloadUrl"))
        if not file_id and not preview_url and not download_url and not signed_download_url:
            return None
        result = {
            "id": file_id or preview_url or download_url or signed_download_url,
            "artifact_id": _string_or(data.get("artifact_id"), data.get("artifactId")),
            "upload_file_id": _string_or(data.get("upload_file_id"), data.get("uploadFileId")),
            "file_name": _string_or(data.get("file_name"), data.get("fileName"), fallback_name),
            "mime_type": _string_or(data.get("mime_type"), data.get("mimeType"), fallback_mime),
            "sha256": _string_or(data.get("sha256"), data.get("hash"), fallback_sha256),
            "preview_key": _string_or(data.get("preview_key"), data.get("previewKey")),
            "preview_url": preview_url,
            "url": _string_or(data.get("url"), preview_url, download_url, signed_download_url),
            "download_url": download_url,
            "signed_download_url": signed_download_url,
            "source_kind": _string_or(data.get("source_kind"), data.get("sourceKind"), "openclaw_artifact"),
        }
        size = _int_or(data.get("size"), 0)
        if size > 0:
            result["size"] = size
        return {key: value for key, value in result.items() if value is not None and value != ""}

    async def _send_output_files_step(
        self,
        chat_id: str,
        files: List[Dict[str, Any]],
        *,
        caption: Optional[str],
    ) -> SendResult:
        if not files:
            return SendResult(success=False, error="No files to deliver")
        req_id = await self._req_id_for(chat_id)
        session_id = self._external_session_id(chat_id)
        active_request_id = self._ledger_active_request_id(chat_id)
        turn_id = self._ledger_turn_id(chat_id, active_request_id)
        step_id = f"process:{uuid.uuid4()}"
        message = _string_or(caption, f"生成了 {len(files)} 个文件")
        timeline = self._build_output_files_timeline(chat_id, files, turn_id)
        process_step = {
            "step_code": "output_files",
            "name": "生成文件",
            "status": "completed",
            "message": message,
            "data": {
                "files": [self._frame_output_file(file) for file in files],
                "contract_version": "v1",
                "openclaw_timeline": timeline,
                "media_attachments": self._media_attachments_for(files),
                "media_contract_version": "v1",
            },
            "timestamp": _now_unix(),
        }
        event = await self._record_event(chat_id, "process.step", {
            "message_id": step_id,
            "content": message,
            "process_step": process_step,
        })
        ledger = _as_dict(_as_dict(event.get("payload")).get("openclaw_ledger"))
        frame_process_step = copy.deepcopy(process_step)
        if ledger:
            frame_process_step["data"]["openclaw_ledger"] = ledger
        frame = {
            "req_id": req_id,
            "action": "chat",
            "status": "streaming",
            "data": {
                "id": step_id,
                "object": "process.step",
                "created": _now_unix(),
                "model": MODEL_NAME,
                "status": "streaming",
                "session_id": session_id,
                "conversation_id": session_id,
                "process_step": frame_process_step,
            },
        }
        await self._send_or_queue(frame)
        return SendResult(success=True, message_id=step_id)

    def _frame_output_file(self, file: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in file.items() if key != "kind" and value is not None and value != ""}

    def _media_attachments_for(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        attachments: List[Dict[str, Any]] = []
        for file in files:
            attachment = {
                key: value
                for key, value in self._frame_output_file(file).items()
                if key not in {"base64", "content"}
            }
            attachment["kind"] = self._media_kind_for_output_file(file)
            attachments.append(attachment)
        return attachments

    def _build_output_files_timeline(self, chat_id: str, files: List[Dict[str, Any]], turn_id: str) -> Dict[str, Any]:
        file_key = ",".join(sorted(
            _string_or(file.get("id"), file.get("sha256"), file.get("url"), file.get("file_name"))
            for file in files
        ))
        safe_key = re.sub(r"[^A-Za-z0-9_.:-]+", "_", file_key)[:160] or "generated"
        return {
            "protocol_version": "openclaw.timeline.v2",
            "turn_id": turn_id,
            "segment_id": f"{turn_id}:output_files:{safe_key}",
            "segment_type": "output_files",
            "segment_index": 0,
            "delta_index": max(self._seq + 1, 1),
            "operation": "replace",
            "visibility": "final",
            "final": True,
        }

    def _media_kind_for_output_file(self, file: Dict[str, Any]) -> str:
        explicit = _string_or(file.get("kind"))
        if explicit in {"image", "audio", "video", "text", "file"}:
            return explicit
        mime_type = _string_or(file.get("mime_type")).lower()
        file_name = _string_or(file.get("file_name")).lower()
        if mime_type.startswith("image/"):
            return "image"
        if mime_type.startswith("audio/"):
            return "audio"
        if mime_type.startswith("video/"):
            return "video"
        if mime_type.startswith("text/") or file_name.endswith((".md", ".json", ".csv", ".tsv", ".txt", ".html", ".css", ".js", ".ts", ".py")):
            return "text"
        return "file"

    def _ledger_turn_id(self, chat_id: str, active_request_id: Optional[str] = None) -> str:
        request_id = _string_or(active_request_id, self._ledger_active_request_id(chat_id))
        return f"{self._external_session_id(chat_id)}:turn:{request_id}"

    def _output_file_part_id(self, chat_id: str, file_name: str, sha256: str) -> str:
        safe_name = re.sub(r"[^A-Za-z0-9_.:-]+", "_", file_name or "file")
        return f"{self._ledger_turn_id(chat_id)}:output_file:{sha256[:16]}:{safe_name}"

    def _sha256_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file_obj:
            for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    async def _send_app_ping(self) -> None:
        await self._send_raw(json.dumps({"action": "ping", "data": {"botId": self.bot_id}}, ensure_ascii=False))

    async def _send_or_queue(self, frame: Dict[str, Any]) -> None:
        raw = json.dumps(frame, ensure_ascii=False)
        if not await self._send_raw(raw):
            async with self._state_lock:
                outbox = _as_list(self._state.setdefault("outbox", []))
                outbox.append(frame)
                self._state["outbox"] = outbox[-MAX_STORED_OUTBOX:]
            await self._persist_state()

    async def _send_raw(self, raw: str) -> bool:
        if self._ws is None or self._ws.closed:
            return False
        try:
            await self._ws.send_str(raw)
            return True
        except Exception as exc:
            logger.warning("53AIHub: failed to send frame: %s", exc)
            return False

    async def _replay_outbox(self) -> None:
        async with self._state_lock:
            outbox = list(_as_list(self._state.get("outbox")))
            self._state["outbox"] = []
        for frame in outbox:
            if not await self._send_raw(json.dumps(frame, ensure_ascii=False)):
                async with self._state_lock:
                    remaining = _as_list(self._state.setdefault("outbox", []))
                    remaining.append(frame)
                    self._state["outbox"] = remaining[-MAX_STORED_OUTBOX:]
                break
        await self._persist_state()

    async def _load_state(self) -> None:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                self._state.update(data)
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning("53AIHub: failed to load state: %s", exc)
        self._seq = max((event.get("seq", 0) for events in _as_dict(self._state.get("events")).values() for event in _as_list(events)), default=0)

    async def _persist_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(self._state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(self.state_path)

    async def _upsert_conversation(self, chat_id: str, title: str, req_id: str, user_name: str) -> None:
        async with self._state_lock:
            conversations = _as_dict(self._state.setdefault("conversations", {}))
            previous = _as_dict(conversations.get(chat_id))
            now = _now_iso()
            session_id = self._external_session_id(chat_id)
            conversations[chat_id] = {
                **previous,
                "id": session_id,
                "conversation_id": session_id,
                "session_id": session_id,
                "chat_id": chat_id,
                "chatId": chat_id,
                "hermes_chat_id": chat_id,
                "title": previous.get("title") or title,
                "status": previous.get("status") or "running",
                "createdAt": previous.get("createdAt") or now,
                "updatedAt": now,
                "created_at": previous.get("created_at") or now,
                "updated_at": now,
                "reqId": req_id,
                "userName": user_name,
            }
        await self._persist_state()

    async def _record_user_message(self, chat_id: str, message_id: str, content: str, incoming: Dict[str, Any]) -> None:
        await self._append_or_replace_message(chat_id, {
            "id": message_id,
            "message_id": message_id,
            "role": "user",
            "content": content,
            "createdAt": _now_iso(),
            "created_at": _now_iso(),
            "raw": incoming.get("raw"),
        }, replace=False)

    async def _record_assistant_message(self, chat_id: str, message_id: str, content: str, replace: bool = False) -> None:
        await self._append_or_replace_message(chat_id, {
            "id": message_id,
            "message_id": message_id,
            "role": "assistant",
            "content": content,
            "createdAt": _now_iso(),
            "created_at": _now_iso(),
        }, replace=replace)

    async def _append_or_replace_message(self, chat_id: str, message: Dict[str, Any], replace: bool) -> None:
        async with self._state_lock:
            messages_by_chat = _as_dict(self._state.setdefault("messages", {}))
            messages = list(_as_list(messages_by_chat.get(chat_id)))
            if replace:
                messages = [existing for existing in messages if _as_dict(existing).get("id") != message["id"]]
            messages.append(message)
            messages_by_chat[chat_id] = messages
            conversations = _as_dict(self._state.setdefault("conversations", {}))
            conv = _as_dict(conversations.get(chat_id))
            if conv:
                now = _now_iso()
                conv["updatedAt"] = now
                conv["updated_at"] = now
                conversations[chat_id] = conv
        await self._persist_state()

    async def _record_event(self, chat_id: str, kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        async with self._state_lock:
            self._seq += 1
            session_id = self._external_session_id(chat_id)
            event_id = f"{chat_id}:event:{self._seq}"
            created_at = _now_iso()
            event_payload = self._payload_with_openclaw_ledger(
                chat_id,
                kind,
                payload,
                self._seq,
                event_id,
                created_at,
            )
            event = {
                "id": event_id,
                "sessionId": session_id,
                "session_id": session_id,
                "chat_id": chat_id,
                "seq": self._seq,
                "kind": kind,
                "payload": event_payload,
                "createdAt": created_at,
                "created_at": created_at,
            }
            events_by_chat = _as_dict(self._state.setdefault("events", {}))
            events = list(_as_list(events_by_chat.get(chat_id)))
            events.append(event)
            events_by_chat[chat_id] = events
        await self._persist_state()
        return event

    def _payload_with_openclaw_ledger(
        self,
        chat_id: str,
        kind: str,
        payload: Dict[str, Any],
        seq: int,
        event_id: str,
        created_at: str,
    ) -> Dict[str, Any]:
        next_payload = dict(_as_dict(payload))
        if _as_dict(next_payload.get("openclaw_ledger")).get("protocol_version") == "openclaw.ledger.v1":
            return next_payload
        ledger = self._build_openclaw_ledger_event(chat_id, kind, next_payload, seq, event_id, created_at)
        if ledger is not None:
            next_payload["openclaw_ledger"] = ledger
        return next_payload

    def _build_openclaw_ledger_event(
        self,
        chat_id: str,
        kind: str,
        payload: Dict[str, Any],
        seq: int,
        event_id: str,
        created_at: str,
    ) -> Optional[Dict[str, Any]]:
        session_id = self._external_session_id(chat_id)
        active_request_id = self._ledger_active_request_id(chat_id)
        turn_id = f"{session_id}:turn:{active_request_id}"
        content = _string_or(payload.get("content"), payload.get("message"), payload.get("text"))
        message_id = _string_or(
            payload.get("message_id"),
            payload.get("messageId"),
            payload.get("status_key"),
            _as_dict(payload.get("data")).get("toolCallId"),
            event_id,
        )
        source_kind = self._source_kind_for_event(kind)
        ledger_payload = {key: value for key, value in payload.items() if key != "openclaw_ledger"}
        ledger_payload.setdefault("source_kind", source_kind)

        part_type = "status"
        event_type = "part.replace"
        operation = "replace"
        visibility = "hidden"
        terminal_status: Optional[str] = None
        text: Optional[str] = content

        if kind == "user.message":
            part_type = "status"
            event_type = "turn.started"
            operation = "noop"
            visibility = "hidden"
            terminal_status = "running"
            text = None
        elif kind == "assistant.thinking":
            part_type = "thinking"
            event_type = "part.replace"
            operation = "replace"
            visibility = "stream"
        elif kind in {"tool.call", "tool.result"}:
            part_type = "tool"
            event_type = "part.replace" if kind == "tool.call" else "part.done"
            operation = "replace"
            visibility = "stream"
        elif kind == "process.step" and _as_dict(payload.get("process_step")).get("step_code") == "output_files":
            part_type = "output_file"
            event_type = "part.done"
            operation = "replace"
            visibility = "final"
            text = _string_or(content, _as_dict(payload.get("process_step")).get("message"))
        elif kind == "assistant.delta":
            part_type = "answer"
            event_type = "part.delta"
            operation = "append"
            visibility = "stream"
        elif kind == "run.completed":
            part_type = "answer"
            event_type = "part.replace"
            operation = "replace"
            visibility = "final"
            terminal_status = "completed"
        elif kind == "run.failed":
            part_type = "answer"
            event_type = "part.replace"
            operation = "replace"
            visibility = "final"
            terminal_status = "failed"
        elif kind == "run.interrupted":
            part_type = "status"
            event_type = "turn.interrupted"
            operation = "close"
            visibility = "hidden"
            terminal_status = "interrupted"
            text = None

        safe_message_id = re.sub(r"[^A-Za-z0-9_.:-]+", "_", message_id or event_id)
        part_id = f"{turn_id}:{part_type}:{safe_message_id}"
        return {
            "protocol_version": "openclaw.ledger.v1",
            "seq": seq,
            "session_id": session_id,
            "conversation_id": session_id,
            "turn_id": turn_id,
            "run_id": active_request_id,
            "active_request_id": active_request_id,
            "part_id": part_id,
            "part_type": part_type,
            "event_type": event_type,
            "operation": operation,
            "visibility": visibility,
            **({"text": text} if text is not None else {}),
            "payload": ledger_payload,
            **({"terminal_status": terminal_status} if terminal_status else {}),
            "created_at": created_at,
            "raw_event_ref": event_id,
        }

    def _event_kind_for_chunk_status(self, status: str, event_kind: str = "") -> str:
        if event_kind:
            return event_kind
        if status == "thinking":
            return "assistant.thinking"
        if status == "done":
            return "run.completed"
        if status == "error":
            return "run.failed"
        return "assistant.delta"

    def _source_kind_for_event(self, kind: str) -> str:
        return {
            "user.message": "hermes.user",
            "assistant.thinking": "hermes.thinking",
            "tool.call": "hermes.tool_call",
            "tool.result": "hermes.tool_result",
            "process.step": "hermes.output_files",
            "run.completed": "hermes.final",
            "run.failed": "hermes.error",
            "run.interrupted": "hermes.interrupted",
            "assistant.delta": "hermes.delta",
        }.get(kind, f"hermes.{kind.replace('.', '_')}")

    def _ledger_active_request_id(self, chat_id: str) -> str:
        conversations = _as_dict(self._state.get("conversations"))
        for key in self._chat_state_keys(chat_id):
            req_id = _string_or(_as_dict(conversations.get(key)).get("reqId"))
            if req_id:
                return req_id
        return self._internal_chat_id(chat_id) or str(uuid.uuid4())

    async def _set_conversation_status(self, chat_id: str, status: str) -> None:
        async with self._state_lock:
            conversations = _as_dict(self._state.setdefault("conversations", {}))
            conv = _as_dict(conversations.get(chat_id))
            if conv:
                now = _now_iso()
                conv["status"] = status
                conv["updatedAt"] = now
                conv["updated_at"] = now
                conversations[chat_id] = conv
        await self._persist_state()

    async def _stop_session(self, chat_id: str) -> Dict[str, Any]:
        session_key = self._session_key_for_chat_id(chat_id)
        cancel = getattr(self, "cancel_session_processing", None)
        if callable(cancel):
            await cancel(session_key, release_guard=True, discard_pending=True)
        else:
            await self.interrupt_session_activity(session_key, chat_id)
        await self._set_conversation_status(chat_id, "interrupted")
        await self._record_event(chat_id, "run.interrupted", {"reason": "stopped by 53AIHub"})
        return {
            "ok": True,
            "action": "stop",
            "session_id": self._external_session_id(chat_id),
            "conversation_id": self._external_session_id(chat_id),
        }

    async def _list_conversations(self) -> List[Dict[str, Any]]:
        async with self._state_lock:
            by_session_id: Dict[str, Dict[str, Any]] = {}
            for chat_id, value in _as_dict(self._state.get("conversations")).items():
                session = self._normalize_conversation_record(chat_id, _as_dict(value).copy())
                session_id = _string_or(session.get("id"), session.get("conversation_id"), session.get("session_id"))
                previous = by_session_id.get(session_id)
                if not previous:
                    by_session_id[session_id] = session
                    continue
                previous_ts = _string_or(previous.get("updatedAt"), previous.get("updated_at"), previous.get("createdAt"), previous.get("created_at"))
                current_ts = _string_or(session.get("updatedAt"), session.get("updated_at"), session.get("createdAt"), session.get("created_at"))
                if current_ts >= previous_ts:
                    by_session_id[session_id] = session
            sessions = list(by_session_id.values())
        return sorted(sessions, key=lambda item: _string_or(item.get("updatedAt"), item.get("createdAt")), reverse=True)

    async def _latest_conversation(self, chat_id: str = "") -> Optional[Dict[str, Any]]:
        sessions = await self._list_conversations()
        if chat_id:
            internal_chat_id = self._internal_chat_id(chat_id)
            for session in sessions:
                if (
                    session.get("id") == chat_id
                    or session.get("conversation_id") == chat_id
                    or session.get("session_id") == chat_id
                    or session.get("chat_id") == internal_chat_id
                    or session.get("chatId") == internal_chat_id
                    or session.get("hermes_chat_id") == internal_chat_id
                ):
                    return session
        return sessions[0] if sessions else None

    async def _messages_for(self, chat_id: str) -> List[Dict[str, Any]]:
        async with self._state_lock:
            messages_by_chat = _as_dict(self._state.get("messages"))
            messages: List[Dict[str, Any]] = []
            seen: set[str] = set()
            for key in self._chat_state_keys(chat_id):
                for message in _as_list(messages_by_chat.get(key)):
                    item = self._normalize_message_record(chat_id, _as_dict(message).copy())
                    dedupe_key = _string_or(item.get("id"), item.get("message_id"), json.dumps(item, sort_keys=True, ensure_ascii=False))
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    messages.append(item)
            return messages

    async def _events_for(self, chat_id: str, after_seq: int) -> List[Dict[str, Any]]:
        async with self._state_lock:
            events_by_chat = _as_dict(self._state.get("events"))
            events: List[Dict[str, Any]] = []
            seen: set[str] = set()
            for key in self._chat_state_keys(chat_id):
                for event in _as_list(events_by_chat.get(key)):
                    item = self._normalize_event_record(chat_id, _as_dict(event).copy())
                    dedupe_key = _string_or(item.get("id"), f"{item.get('kind')}:{item.get('seq')}")
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    events.append(item)
        return [event for event in events if int(event.get("seq", 0)) > after_seq]

    async def _ledger_events_for(self, chat_id: str, after_seq: int) -> List[Dict[str, Any]]:
        events = await self._events_for(chat_id, 0)
        ledger_events: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for event in events:
            payload = _as_dict(event.get("payload"))
            ledger = _as_dict(payload.get("openclaw_ledger"))
            if ledger.get("protocol_version") != "openclaw.ledger.v1":
                continue
            seq = int(ledger.get("seq") or event.get("seq") or 0)
            if seq <= after_seq:
                continue
            dedupe_key = _string_or(
                ledger.get("raw_event_ref"),
                f"{ledger.get('session_id')}:{seq}:{ledger.get('turn_id')}:{ledger.get('part_id')}:{ledger.get('event_type')}",
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            ledger_events.append(ledger)
        return sorted(ledger_events, key=lambda item: (int(item.get("seq") or 0), _string_or(item.get("part_id"))))

    async def _session_snapshot(self, chat_id: str, after_seq: int) -> Dict[str, Any]:
        all_ledger_events = await self._ledger_events_for(chat_id, 0)
        delta_ledger_events = [event for event in all_ledger_events if int(event.get("seq") or 0) > after_seq]
        last_seq = max((int(event.get("seq") or 0) for event in all_ledger_events), default=0)
        session_id = self._external_session_id(chat_id)
        return {
            "session_id": session_id,
            "conversation_id": session_id,
            "last_seq": last_seq,
            "active_turns": self._active_turns_from_ledger_events(all_ledger_events),
            "recent_events": all_ledger_events[-100:],
            "ledger_events": delta_ledger_events,
            "ledgerEvents": delta_ledger_events,
        }

    def _active_turns_from_ledger_events(self, ledger_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        turns: Dict[str, Dict[str, Any]] = {}
        for event in ledger_events:
            turn_id = _string_or(event.get("turn_id"))
            if not turn_id:
                continue
            seq = int(event.get("seq") or 0)
            turn = turns.setdefault(turn_id, {
                "turn_id": turn_id,
                "run_id": _string_or(event.get("run_id")),
                "active_request_id": _string_or(event.get("active_request_id")),
                "status": "running",
                "last_seq": 0,
                "part_ids": [],
            })
            if _string_or(event.get("run_id")):
                turn["run_id"] = _string_or(event.get("run_id"))
            if _string_or(event.get("active_request_id")):
                turn["active_request_id"] = _string_or(event.get("active_request_id"))
            terminal_status = _string_or(event.get("terminal_status"))
            if terminal_status:
                turn["status"] = terminal_status
            turn["last_seq"] = max(int(turn.get("last_seq") or 0), seq)
            part_id = _string_or(event.get("part_id"))
            if part_id and part_id not in turn["part_ids"]:
                turn["part_ids"].append(part_id)
        return [
            turn for turn in sorted(turns.values(), key=lambda item: int(item.get("last_seq") or 0))
            if turn.get("status") == "running"
        ]

    async def _req_id_for(self, chat_id: str) -> str:
        async with self._state_lock:
            conversations = _as_dict(self._state.get("conversations"))
            for key in self._chat_state_keys(chat_id):
                conv = _as_dict(conversations.get(key))
                req_id = _string_or(conv.get("reqId"))
                if req_id:
                    return req_id
            return self._internal_chat_id(chat_id)

    def _auth_headers(self) -> Dict[str, str]:
        auth = base64.b64encode(f"{self.bot_id}:{self.secret}".encode("utf-8")).decode("ascii")
        return {
            "Authorization": f"Bearer {self.secret}",
            "Proxy-Authorization": f"Basic {auth}",
            "X-Bot-Id": self.bot_id,
            "X-Api-Key": self.secret,
        }

    def _conversation_title(self, incoming: Dict[str, Any]) -> str:
        explicit = _string_or(incoming.get("conversationTitle"))
        if explicit:
            return explicit
        user = _string_or(incoming.get("userName"), incoming.get("userId"), "user")
        summary = incoming["text"].replace("\n", " ").strip()[:40] or "新对话"
        return f"53AI Hub-{user}：{summary}"

    def _ensure_home_channel(self, chat_id: str, title: str) -> None:
        if not chat_id or os.getenv(HOME_CHANNEL_ENV):
            return
        os.environ[HOME_CHANNEL_ENV] = str(chat_id)
        os.environ.setdefault(HOME_CHANNEL_THREAD_ENV, "")
        try:
            from hermes_cli.config import save_env_value

            save_env_value(HOME_CHANNEL_ENV, str(chat_id))
        except Exception as exc:
            logger.warning("53AIHub: failed to persist %s: %s", HOME_CHANNEL_ENV, exc)
        try:
            self.config.home_channel = HomeChannel(
                platform=Platform(PLATFORM_NAME),
                chat_id=str(chat_id),
                name=title or "53AIHub",
            )
        except Exception as exc:
            logger.debug("53AIHub: failed to update in-process home channel: %s", exc)

    def _is_stream_consumer_send(self) -> bool:
        frame = inspect.currentframe()
        if frame is None:
            return False
        frame = frame.f_back
        while frame is not None:
            filename = frame.f_code.co_filename.replace("\\", "/")
            if filename.endswith("/gateway/stream_consumer.py"):
                return True
            frame = frame.f_back
        return False

    def _is_hermes_interim_send(self, metadata: Optional[Dict[str, Any]]) -> bool:
        if _as_dict(metadata).get("notify") is True:
            return False
        return self._has_stack_function({
            "_send_progress_text",
            "_send_or_update_status_coro",
            "_notify_long_running",
        })

    def _is_hermes_interim_edit(self) -> bool:
        return self._has_stack_function({"_edit_progress_message"})

    def _has_stack_function(self, names: set[str]) -> bool:
        frame = inspect.currentframe()
        if frame is None:
            return False
        frame = frame.f_back
        while frame is not None:
            if frame.f_code.co_name in names:
                return True
            frame = frame.f_back
        return False

    def _terminal_send_status(self, content: str) -> str:
        normalized = (content or "").lower()
        error_markers = (
            "sorry, i encountered an error",
            "the request failed:",
            "runtimeerror",
            "provider '",
            "api key was found",
            "try again or use /reset",
        )
        return "error" if any(marker in normalized for marker in error_markers) else "done"

    def _tool_call_payload_from_progress(
        self,
        message_id: str,
        content: str,
        base_payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        parsed = _parse_tool_progress_text(content)
        if parsed is None:
            return None
        tool_name, preview = parsed
        args: Dict[str, Any] = {}
        if preview:
            args["preview"] = preview
        return {
            **base_payload,
            "data": {
                "phase": "call",
                "name": tool_name,
                "toolCallId": message_id,
                "args": args,
                "meta": preview,
            },
        }

    def _read_session_id(self, record: Dict[str, Any]) -> str:
        session_id = _string_or(record.get("session_id"), record.get("sessionId"), record.get("conversation_id"), record.get("conversationId"))
        if not session_id:
            raise ValueError("session_id or conversation_id is required")
        return session_id

    def _normalize_conversation_record(self, chat_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        raw_chat_id = _string_or(
            record.get("chat_id"),
            record.get("chatId"),
            record.get("hermes_chat_id"),
            self._internal_chat_id(_string_or(record.get("id"), record.get("conversation_id"), record.get("session_id"))),
            chat_id,
        )
        internal_chat_id = self._internal_chat_id(raw_chat_id)
        session_id = self._external_session_id(internal_chat_id)
        return {
            **record,
            "id": session_id,
            "conversation_id": session_id,
            "session_id": session_id,
            "chat_id": internal_chat_id,
            "chatId": internal_chat_id,
            "hermes_chat_id": internal_chat_id,
        }

    def _normalize_message_record(self, chat_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        internal_chat_id = self._internal_chat_id(chat_id)
        session_id = self._external_session_id(internal_chat_id)
        return {
            **record,
            "sessionId": session_id,
            "session_id": session_id,
            "conversation_id": session_id,
            "chat_id": internal_chat_id,
        }

    def _normalize_event_record(self, chat_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        internal_chat_id = self._internal_chat_id(_string_or(
            record.get("chat_id"),
            record.get("chatId"),
            record.get("hermes_chat_id"),
            record.get("sessionId"),
            record.get("session_id"),
            chat_id,
        ))
        session_id = self._external_session_id(internal_chat_id)
        seq = int(record.get("seq") or 0)
        event_id = _string_or(record.get("id"), f"{internal_chat_id}:event:{seq}")
        created_at = _string_or(record.get("createdAt"), record.get("created_at"), _now_iso())
        payload = self._payload_with_openclaw_ledger(
            internal_chat_id,
            _string_or(record.get("kind")),
            _as_dict(record.get("payload")),
            seq,
            event_id,
            created_at,
        )
        return {
            **record,
            "sessionId": session_id,
            "session_id": session_id,
            "conversation_id": session_id,
            "chat_id": internal_chat_id,
            "payload": payload,
        }

    def _external_session_id(self, chat_id: str) -> str:
        value = _string_or(chat_id)
        if value.startswith(SESSION_ID_PREFIX):
            return value
        return f"{SESSION_ID_PREFIX}{value}"

    def _internal_chat_id(self, session_id: str) -> str:
        value = _string_or(session_id)
        if value.startswith(SESSION_ID_PREFIX):
            return value[len(SESSION_ID_PREFIX):]
        return value

    def _chat_state_keys(self, chat_id: str) -> List[str]:
        internal_chat_id = self._internal_chat_id(chat_id)
        external_session_id = self._external_session_id(internal_chat_id)
        keys = [internal_chat_id, external_session_id]
        original = _string_or(chat_id)
        if original and original not in keys:
            keys.append(original)
        return keys

    def _session_key_for_chat_id(self, chat_id: str) -> str:
        return self._external_session_id(chat_id)

    def _pagination(self, record: Dict[str, Any], default_limit: int) -> tuple[int, int]:
        limit = int(record.get("limit") or default_limit)
        offset = int(record.get("offset") or 0)
        return max(1, min(limit, 200)), max(0, offset)

    def _pagination_response(self, limit: int, offset: int, total: int, returned: int) -> Dict[str, Any]:
        return {
            "limit": limit,
            "offset": offset,
            "total": total,
            "returned": returned,
            "has_more": offset + returned < total,
        }

    def _slice_latest(self, messages: List[Dict[str, Any]], limit: int, offset: int) -> List[Dict[str, Any]]:
        start = max(0, len(messages) - offset - limit)
        end = len(messages) - offset if offset else len(messages)
        return messages[start:end]

    def _masked_bot_id(self) -> str:
        if len(self.bot_id) <= 4:
            return self.bot_id
        return f"{self.bot_id[:2]}***{self.bot_id[-2:]}"

    def _safe_ws_url(self) -> str:
        return self.ws_url.split("?", 1)[0]


def _apply_yaml_config(yaml_cfg: Dict[str, Any], platform_cfg: Dict[str, Any]) -> Dict[str, Any]:
    extra = _as_dict(platform_cfg.get("extra"))
    for env_name, key in (
        ("HUB53AI_BOT_ID", "bot_id"),
        ("HUB53AI_SECRET", "secret"),
        ("HUB53AI_WS_URL", "ws_url"),
    ):
        value = _string_or(extra.get(key), extra.get(f"hub_{key}"))
        if value and not os.getenv(env_name):
            os.environ[env_name] = value
    return extra


def _env_enablement() -> Optional[Dict[str, Any]]:
    if check_requirements():
        return {
            "bot_id": os.getenv("HUB53AI_BOT_ID", ""),
            "ws_url": os.getenv("HUB53AI_WS_URL", ""),
        }
    return None


def register(ctx) -> None:
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="53AIHub",
        adapter_factory=lambda config: Hermes53AIHubAdapter(config),
        check_fn=check_requirements,
        validate_config=lambda config: bool(config.enabled),
        required_env=["HUB53AI_BOT_ID", "HUB53AI_SECRET", "HUB53AI_WS_URL"],
        allowed_users_env="HUB53AI_ALLOWED_USERS",
        allow_all_env="HUB53AI_ALLOW_ALL_USERS",
        cron_deliver_env_var=HOME_CHANNEL_ENV,
        install_hint="Set HUB53AI_BOT_ID, HUB53AI_SECRET, HUB53AI_WS_URL and enable platforms/53aihub.",
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
    )
