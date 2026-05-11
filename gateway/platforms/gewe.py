"""GeWe platform adapter for WeChat messages.

This adapter talks to GeWe directly for outbound operations and supports three
inbound modes:

- ``direct-callback``: GeWe posts callbacks directly to this gateway.
- ``relay-callback``: webhook-router forwards callbacks to this gateway.
- ``relay-sse``: this gateway connects to webhook-router by SSE and consumes
  forwarded callback events.

Only GeWe callback v2 payloads are supported.  The adapter intentionally keeps
the platform shape close to Hermes' native callback adapters: aiohttp owns the
HTTP lifecycle, httpx owns outbound API calls, and inbound payloads are
normalized into ``MessageEvent`` before entering the shared gateway pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import shutil
import socket as _socket
import subprocess
from dataclasses import dataclass, field
from html import unescape
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import quote, urljoin, urlsplit
from xml.etree import ElementTree as ET

try:
    from aiohttp import web

    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency gate
    web = None  # type: ignore[assignment]
    AIOHTTP_AVAILABLE = False

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency gate
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
    cache_video_from_bytes,
    merge_pending_message_event,
    safe_url_for_log,
)
from gateway.platforms.helpers import MessageDeduplicator
from gateway.session import build_session_key
from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "https://api.geweapi.com"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8656
DEFAULT_DIRECT_PATH = "/gewe/callback"
DEFAULT_RELAY_PATH = "/gewe/relay"
DEFAULT_RELAY_BASE_URL = "https://hook.yunzxu.com"
GEWE_SILK_DECODER_ENV = "GEWE_SILK_DECODER"
SUPPORTED_INBOUND_MODES = {"direct-callback", "relay-callback", "relay-sse"}
_INSECURE_NO_AUTH = "INSECURE_NO_AUTH"
_MEDIA_ONLY_PLACEHOLDERS = {"[图片]", "[表情]"}


def check_gewe_requirements() -> bool:
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE


@dataclass
class GeweDownloadHint:
    endpoint: str
    request_body: Dict[str, Any]
    fallbacks: List["GeweDownloadHint"] = field(default_factory=list)


@dataclass
class GeweAttachment:
    kind: str
    title: str = ""
    description: str = ""
    file_name: str = ""
    file_ext: str = ""
    file_size: Optional[int] = None
    url: str = ""
    thumb_url: str = ""
    md5: str = ""
    aes_key: str = ""
    cdn_file_id: str = ""
    cdn_file_ids: List[str] = field(default_factory=list)
    duration_seconds: Optional[int] = None
    quoted_message_id: str = ""
    quoted_sender_id: str = ""
    quoted_sender_name: str = ""
    quoted_text: str = ""
    quoted_message_type: str = ""
    needs_download: bool = False
    download_hint: Optional[GeweDownloadHint] = None
    local_path: str = ""
    raw: Any = None


@dataclass
class NormalizedGeweMessage:
    account_id: str
    device_id: str
    peer_id: str
    sender_id: str
    conversation_type: str
    message_type: str
    provider_message_id: str = ""
    text: str = ""
    content_xml: str = ""
    create_time: Optional[int] = None
    is_self: Optional[bool] = None
    attachments: List[GeweAttachment] = field(default_factory=list)
    items: List["NormalizedGeweMessage"] = field(default_factory=list)
    relay: Dict[str, Any] = field(default_factory=dict)
    mentioned_user_ids: set[str] = field(default_factory=set)
    reply_to_message_id: str = ""
    reply_to_text: str = ""
    raw: Any = None



class GeweAdapter(BasePlatformAdapter):
    """Native Hermes adapter for GeWe WeChat API."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.GEWE)
        extra = config.extra or {}
        self._api_base_url = str(extra.get("api_base_url") or DEFAULT_API_BASE_URL).rstrip("/")
        self._token = str(config.token or extra.get("token") or os.getenv("GEWE_TOKEN", ""))
        self._app_id = str(extra.get("app_id") or os.getenv("GEWE_APP_ID", ""))
        self._inbound_mode = str(extra.get("inbound_mode") or os.getenv("GEWE_INBOUND_MODE") or "direct-callback")
        if self._inbound_mode not in SUPPORTED_INBOUND_MODES:
            logger.error("[GeWe] Unknown inbound_mode=%s; supported modes are %s", self._inbound_mode, sorted(SUPPORTED_INBOUND_MODES))

        self._host = str(extra.get("callback_host") or extra.get("host") or DEFAULT_HOST)
        self._port = int(extra.get("callback_port") or extra.get("port") or DEFAULT_PORT)
        default_path = DEFAULT_RELAY_PATH if self._inbound_mode == "relay-callback" else DEFAULT_DIRECT_PATH
        self._path = str(extra.get("callback_path") or extra.get("path") or default_path)
        self._callback_secret = str(extra.get("callback_secret") or os.getenv("GEWE_CALLBACK_SECRET", ""))

        self._relay_base_url = str(extra.get("relay_base_url") or os.getenv("GEWE_RELAY_BASE_URL") or DEFAULT_RELAY_BASE_URL).rstrip("/")
        self._relay_app_id = str(extra.get("relay_app_id") or os.getenv("GEWE_RELAY_APP_ID") or "")
        self._relay_app_token = str(extra.get("relay_app_token") or os.getenv("GEWE_RELAY_APP_TOKEN") or "")
        self._relay_sse_url = str(extra.get("relay_sse_url") or os.getenv("GEWE_RELAY_SSE_URL") or "")
        self._relay_channel = str(extra.get("relay_channel") or os.getenv("GEWE_RELAY_CHANNEL") or "")
        self._group_policy = str(extra.get("group_policy") or os.getenv("GEWE_GROUP_POLICY") or "paired").strip().lower()
        self._group_allowed_chats = _split_csv(extra.get("group_allowed_chats") or os.getenv("GEWE_GROUP_ALLOWED_CHATS") or "")
        self._group_require_mention = _as_bool(extra.get("group_require_mention") or os.getenv("GEWE_GROUP_REQUIRE_MENTION"), False)
        self._bot_wxids = _split_csv(extra.get("bot_wxid") or os.getenv("GEWE_BOT_WXID") or "")

        self._download_media = _as_bool(extra.get("download_media"), True)
        self._dedup = MessageDeduplicator(ttl_seconds=300)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._sse_task: Optional[asyncio.Task] = None
        self._last_event_id_file = Path(extra.get("last_event_id_file") or (get_hermes_home() / "gewe_last_event_id"))
        self._media_followup_grace_seconds = _as_float(
            extra.get("media_followup_grace_seconds")
            or os.getenv("GEWE_MEDIA_FOLLOWUP_GRACE_SECONDS"),
            2.0,
        )
        self._pending_media_followups: Dict[str, tuple[MessageEvent, asyncio.Task]] = {}
        self._sent_message_revoke_payloads: Dict[str, Dict[str, str]] = {}
        self._gewe_lock_keys: List[tuple[str, str]] = []

    async def connect(self) -> bool:
        if not check_gewe_requirements():
            logger.warning("[GeWe] aiohttp/httpx not installed")
            return False
        if not self._token or not self._app_id:
            logger.warning("[GeWe] GEWE_TOKEN and GEWE_APP_ID are required")
            return False
        if self._inbound_mode not in SUPPORTED_INBOUND_MODES:
            logger.warning("[GeWe] GEWE_INBOUND_MODE must be one of: %s", ", ".join(sorted(SUPPORTED_INBOUND_MODES)))
            return False
        if not self._bot_wxids:
            logger.warning("[GeWe] GEWE_BOT_WXID is required for reliable group mention routing")
            return False
        if self._inbound_mode in {"relay-callback", "relay-sse"} and (not self._relay_app_id or not self._relay_app_token):
            logger.warning("[GeWe] GEWE_RELAY_APP_ID and GEWE_RELAY_APP_TOKEN are required for webhook-router modes")
            return False
        if not self._acquire_gewe_locks():
            return False

        self._http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

        if self._inbound_mode in {"direct-callback", "relay-callback"}:
            if not await self._start_callback_server():
                await self._cleanup()
                self._release_gewe_locks()
                return False
        self._mark_connected()
        if self._inbound_mode == "relay-sse":
            self._sse_task = asyncio.create_task(self._sse_loop())
        logger.info("[GeWe] Connected mode=%s app_id=%s", self._inbound_mode, _safe_id(self._app_id))
        return True

    async def disconnect(self) -> None:
        await self._cancel_media_followup_tasks()
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass
            self._sse_task = None
        await self._cleanup()
        self._release_gewe_locks()
        self._mark_disconnected()
        logger.info("[GeWe] Disconnected")

    def _acquire_gewe_locks(self) -> bool:
        locks = [
            ("gewe-app-id", self._app_id, "GeWe app ID"),
            ("gewe-token", self._token, "GeWe token"),
        ]
        if self._inbound_mode in {"relay-callback", "relay-sse"}:
            locks.extend([
                ("gewe-relay-app-id", self._relay_app_id, "Webhook-router app ID"),
                ("gewe-relay-app-token", self._relay_app_token, "Webhook-router token"),
            ])
        for scope, identity, resource_desc in locks:
            if not self._acquire_gewe_lock(scope, identity, resource_desc):
                self._release_gewe_locks()
                return False
        return True

    def _acquire_gewe_lock(self, scope: str, identity: str, resource_desc: str) -> bool:
        from gateway.status import acquire_scoped_lock

        acquired, existing = acquire_scoped_lock(scope, identity, metadata={"platform": self.platform.value})
        if acquired:
            self._gewe_lock_keys.append((scope, identity))
            return True
        owner_pid = existing.get("pid") if isinstance(existing, dict) else None
        message = (
            f"{resource_desc} already in use"
            + (f" (PID {owner_pid})" if owner_pid else "")
            + ". Stop the other gateway first."
        )
        logger.error("[GeWe] %s", message)
        self._set_fatal_error(f"{scope}_lock", message, retryable=False)
        return False

    def _release_gewe_locks(self) -> None:
        from gateway.status import release_scoped_lock

        while self._gewe_lock_keys:
            scope, identity = self._gewe_lock_keys.pop()
            release_scoped_lock(scope, identity)

    async def _cleanup(self) -> None:
        self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._post_message("/gewe/v2/api/message/postText", {
            "appId": self._app_id,
            "toWxid": chat_id,
            "content": content,
        })

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        result = await self._post_message("/gewe/v2/api/message/postImage", {
            "appId": self._app_id,
            "toWxid": chat_id,
            "imgUrl": image_url,
        })
        if result.success and caption:
            await self.send(chat_id, caption, reply_to=reply_to, metadata=metadata)
        return result

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        if image_path.startswith(("http://", "https://")):
            return await self.send_image(chat_id, image_path, caption=caption, reply_to=reply_to, metadata=metadata)
        try:
            image_url = await self._upload_local_file(image_path)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)
        return await self.send_image(chat_id, image_url, caption=caption, reply_to=reply_to, metadata=metadata)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        voice_url = audio_path
        if not voice_url.startswith(("http://", "https://")):
            try:
                voice_url = await self._upload_local_file(audio_path)
            except Exception as exc:
                return SendResult(success=False, error=str(exc), retryable=True)

        ext = Path(urlsplit(voice_url).path).suffix.lower()
        if ext != ".silk":
            return SendResult(success=False, error="GeWe voiceUrl only supports .silk files")

        duration = kwargs.get("voice_duration_ms") or kwargs.get("duration_ms")
        if duration is None and metadata:
            duration = metadata.get("voice_duration_ms") or metadata.get("duration_ms")
        try:
            voice_duration = int(duration or 0)
        except (TypeError, ValueError):
            voice_duration = 0

        result = await self._post_message("/gewe/v2/api/message/postVoice", {
            "appId": self._app_id,
            "toWxid": chat_id,
            "voiceUrl": voice_url,
            "voiceDuration": voice_duration,
        })
        if result.success and caption:
            await self.send(chat_id, caption, reply_to=reply_to, metadata=metadata)
        return result

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        file_url = file_path
        resolved_name = file_name
        if not file_url.startswith(("http://", "https://")):
            try:
                file_url = await self._upload_local_file(file_path)
            except Exception as exc:
                return SendResult(success=False, error=str(exc), retryable=True)
            resolved_name = resolved_name or Path(file_path).name

        return await self._post_message("/gewe/v2/api/message/postFile", {
            "appId": self._app_id,
            "toWxid": chat_id,
            "fileUrl": file_url,
            "fileName": resolved_name or Path(urlsplit(file_url).path).name or "file",
        })

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "group" if chat_id.endswith("@chatroom") else "dm"}

    async def delete_message(
        self,
        chat_id: str,
        message_id: str,
    ) -> bool:
        revoke_payload = self._sent_message_revoke_payloads.get(str(message_id or ""))
        if not revoke_payload:
            return False
        if revoke_payload.get("toWxid") != chat_id:
            return False

        data = await self._api_post("/gewe/v2/api/message/revokeMsg", revoke_payload)
        ok = _gewe_ok(data)
        if ok:
            self._forget_revoke_payload(revoke_payload)
        return ok

    async def _post_message(self, path: str, payload: Dict[str, Any]) -> SendResult:
        try:
            data = await self._api_post(path, payload)
            ok = _gewe_ok(data)
            body = data.get("data") if isinstance(data.get("data"), dict) else {}
            message_id = str(body.get("msgId") or body.get("newMsgId") or "")
            if ok and message_id:
                self._remember_revoke_payload(payload.get("toWxid"), body)
            return SendResult(
                success=ok,
                message_id=message_id,
                error=None if ok else str(data),
                raw_response=data,
                retryable=not ok,
            )
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    def _remember_revoke_payload(self, chat_id: Any, body: Dict[str, Any]) -> None:
        msg_id = str(body.get("msgId") or "").strip()
        new_msg_id = str(body.get("newMsgId") or "").strip()
        create_time = str(body.get("createTime") or "").strip()
        to_wxid = str(chat_id or body.get("toWxid") or "").strip()
        if not (to_wxid and msg_id and new_msg_id and create_time):
            return

        revoke_payload = {
            "appId": self._app_id,
            "toWxid": to_wxid,
            "msgId": msg_id,
            "newMsgId": new_msg_id,
            "createTime": create_time,
        }
        self._sent_message_revoke_payloads[msg_id] = revoke_payload
        self._sent_message_revoke_payloads[new_msg_id] = revoke_payload

    def _forget_revoke_payload(self, revoke_payload: Dict[str, str]) -> None:
        for key in (revoke_payload.get("msgId"), revoke_payload.get("newMsgId")):
            if key:
                self._sent_message_revoke_payloads.pop(str(key), None)

    async def _upload_local_file(self, file_path: str) -> str:
        if not self._http_client:
            raise RuntimeError("GeWe HTTP client is not connected")
        if not self._relay_base_url or not self._relay_app_id or not self._relay_app_token:
            raise RuntimeError("GeWe local media upload requires GEWE_RELAY_BASE_URL, GEWE_RELAY_APP_ID, and GEWE_RELAY_APP_TOKEN")

        path = Path(file_path).expanduser()
        if not path.exists() or not path.is_file():
            raise RuntimeError(f"Media file not found: {file_path}")
        size = path.stat().st_size
        max_size = 50 * 1024 * 1024
        if size > max_size:
            raise RuntimeError("GeWe local media upload exceeds webhook-router 50MB file limit")

        upload_url = (
            f"{self._relay_base_url}/apps/{quote(self._relay_app_id, safe='')}/files"
            f"?token={quote(self._relay_app_token, safe='')}"
        )
        mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        with path.open("rb") as file_obj:
            response = await self._http_client.post(
                upload_url,
                files={"file": (path.name, file_obj, mime)},
            )
        text = response.text
        response.raise_for_status()
        data = json.loads(text) if text else {}
        if not data.get("ok") or not data.get("path"):
            raise RuntimeError(f"Webhook-router file upload failed: {data}")
        return urljoin(f"{self._relay_base_url}/", str(data["path"]).lstrip("/"))

    async def _api_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self._http_client:
            raise RuntimeError("GeWe HTTP client is not connected")
        response = await self._http_client.post(
            urljoin(f"{self._api_base_url}/", path.lstrip("/")),
            headers={"X-GEWE-TOKEN": self._token, "Content-Type": "application/json"},
            json=payload,
        )
        text = response.text
        response.raise_for_status()
        return json.loads(text) if text else {}

    async def _start_callback_server(self) -> bool:
        try:
            with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.connect(("127.0.0.1", self._port))
            logger.error("[GeWe] Port %d already in use", self._port)
            return False
        except (ConnectionRefusedError, OSError):
            pass

        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_post(self._path, self._handle_callback)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()
        logger.info("[GeWe] Callback server listening on %s:%d%s", self._host, self._port, self._path)
        return True

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "platform": "gewe", "mode": self._inbound_mode})

    async def _handle_callback(self, request: web.Request) -> web.Response:
        if self._callback_secret and self._callback_secret != _INSECURE_NO_AUTH:
            supplied = request.headers.get("X-Hermes-Gewe-Secret") or request.headers.get("X-Gewe-Secret")
            if supplied != self._callback_secret:
                return web.json_response({"error": "invalid secret"}, status=401)
        try:
            payload = await request.json(loads=json.loads)
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)

        try:
            if self._inbound_mode == "relay-callback" or _looks_like_relay(payload):
                await self._process_relay_event(payload)
            else:
                await self._process_gewe_payload(payload)
        except Exception:
            logger.exception("[GeWe] Failed to process callback")
            return web.json_response({"status": "error"}, status=500)
        return web.json_response({"status": "ok"})

    async def _sse_loop(self) -> None:
        while self._running:
            try:
                await self._connect_sse_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[GeWe] relay-sse connection failed: %s", exc)
            await asyncio.sleep(3)

    async def _connect_sse_once(self) -> None:
        if not self._http_client:
            raise RuntimeError("GeWe HTTP client is not connected")
        url = self._relay_sse_url
        if not url:
            if not self._relay_app_id:
                raise RuntimeError("GEWE_RELAY_APP_ID or GEWE_RELAY_SSE_URL is required for relay-sse mode")
            url = f"{self._relay_base_url}/apps/{quote(self._relay_app_id, safe='')}/events"
            if self._relay_app_token:
                url = f"{url}?token={quote(self._relay_app_token, safe='')}"
        headers = {"Accept": "text/event-stream"}
        last_id = self._read_last_event_id()
        if last_id:
            headers["Last-Event-ID"] = last_id

        async with self._http_client.stream("GET", url, headers=headers, timeout=None) as response:
            response.raise_for_status()
            buffer = ""
            async for chunk in response.aiter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    raw, buffer = buffer.split("\n\n", 1)
                    await self._handle_sse_message(raw)

    async def _handle_sse_message(self, raw: str) -> None:
        message = _parse_sse(raw)
        if not message.get("data") or message.get("event") not in (None, "", "webhook"):
            return
        event = json.loads(message["data"])
        await self._process_relay_event(event)
        event_id = message.get("id") or str(event.get("id") or "")
        if event_id:
            self._write_last_event_id(event_id)

    async def _process_relay_event(self, event: Dict[str, Any]) -> None:
        if self._relay_channel and str(event.get("channel") or "") != self._relay_channel:
            return
        body = event.get("body")
        if body is None and event.get("body_base64"):
            import base64

            body = json.loads(base64.b64decode(event["body_base64"]).decode("utf-8"))
        normalized = normalize_gewe_callback(body)
        if normalized:
            normalized.relay = {
                "id": event.get("id"),
                "source_id": event.get("source_id"),
                "channel": event.get("channel"),
                "received_at": event.get("received_at"),
            }
            await self._dispatch_normalized(normalized)

    async def _process_gewe_payload(self, payload: Dict[str, Any]) -> None:
        normalized = normalize_gewe_callback(payload)
        if normalized:
            await self._dispatch_normalized(normalized)

    async def _dispatch_normalized(self, msg: NormalizedGeweMessage) -> None:
        if msg.is_self:
            return
        if msg.conversation_type == "group" and not self._should_process_group(msg):
            return
        dedupe_key = f"{msg.account_id}:{msg.provider_message_id}" if msg.provider_message_id else ""
        if dedupe_key and self._dedup.is_duplicate(dedupe_key):
            return

        media_urls, media_types = await self._cache_media(msg)
        text = self._message_text(msg)
        hermes_type = _to_hermes_type(msg.message_type)
        source = self.build_source(
            chat_id=msg.peer_id,
            chat_name=msg.peer_id,
            chat_type="group" if msg.conversation_type == "group" else "dm",
            user_id=msg.sender_id,
            user_name=msg.sender_id,
            message_id=msg.provider_message_id,
        )
        event = MessageEvent(
            text=text,
            message_type=hermes_type,
            source=source,
            raw_message=msg.raw,
            message_id=msg.provider_message_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=msg.reply_to_message_id or None,
            reply_to_text=msg.reply_to_text or None,
        )
        await self._dispatch_event_with_media_debounce(event)

    async def _dispatch_event_with_media_debounce(self, event: MessageEvent) -> None:
        key = self._media_followup_key(event)
        if not key or self._media_followup_grace_seconds <= 0:
            await self.handle_message(event)
            return

        if self._is_debounceable_media_event(event):
            pending = self._pending_media_followups.get(key)
            if pending:
                pending_event, pending_task = pending
                pending_task.cancel()
                merge_pending_message_event({key: pending_event}, key, event)
                self._pending_media_followups[key] = (
                    pending_event,
                    self._schedule_media_followup_flush(key),
                )
                return
            self._pending_media_followups[key] = (
                event,
                self._schedule_media_followup_flush(key),
            )
            return

        pending = self._pending_media_followups.pop(key, None)
        if pending:
            pending_event, pending_task = pending
            pending_task.cancel()
            if event.message_type == MessageType.TEXT and (event.text or "").strip():
                self._merge_text_into_pending_media_event(pending_event, event)
                await self.handle_message(pending_event)
                return
            await self.handle_message(pending_event)

        await self.handle_message(event)

    def _schedule_media_followup_flush(self, key: str) -> asyncio.Task:
        task = asyncio.create_task(self._flush_pending_media_followup_after_delay(key))
        task.add_done_callback(self._media_followup_task_done)
        return task

    async def _flush_pending_media_followup_after_delay(self, key: str) -> None:
        await asyncio.sleep(self._media_followup_grace_seconds)
        pending = self._pending_media_followups.pop(key, None)
        if not pending:
            return
        event, _task = pending
        await self.handle_message(event)

    def _media_followup_task_done(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning("[GeWe] Media follow-up debounce task failed", exc_info=True)

    async def _cancel_media_followup_tasks(self) -> None:
        pending = list(self._pending_media_followups.values())
        self._pending_media_followups.clear()
        for _event, task in pending:
            task.cancel()
        for _event, task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _media_followup_key(self, event: MessageEvent) -> str:
        source = event.source
        if not source:
            return ""
        return build_session_key(
            source,
            group_sessions_per_user=self.config.extra.get("group_sessions_per_user", True),
            thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
        )

    @staticmethod
    def _is_debounceable_media_event(event: MessageEvent) -> bool:
        if event.message_type != MessageType.PHOTO or not event.media_urls:
            return False
        return (event.text or "").strip() in _MEDIA_ONLY_PLACEHOLDERS

    @staticmethod
    def _merge_text_into_pending_media_event(media_event: MessageEvent, text_event: MessageEvent) -> None:
        followup_text = (text_event.text or "").strip()
        if not followup_text:
            return
        if (media_event.text or "").strip() in _MEDIA_ONLY_PLACEHOLDERS:
            media_event.text = followup_text
        else:
            media_event.text = BasePlatformAdapter._merge_caption(media_event.text, followup_text)
        media_event.message_id = text_event.message_id or media_event.message_id
        media_event.timestamp = text_event.timestamp

    def _should_process_group(self, msg: NormalizedGeweMessage) -> bool:
        """Apply GeWe group-level routing before gateway user auth.

        User authorization still happens in GatewayRunner.  This adapter-level
        gate only decides whether this group is eligible for agent handling at
        all.  ``paired`` means "allow the gateway auth layer to decide by
        sender wxid/pairing"; ``allowlist`` means the chatroom itself must be
        listed in GEWE_GROUP_ALLOWED_CHATS; ``open`` delegates everything to
        gateway auth; ``disabled`` ignores all group messages.
        """
        if self._group_policy in {"disabled", "off", "none"}:
            return False
        if self._group_allowed_chats and msg.peer_id not in self._group_allowed_chats and "*" not in self._group_allowed_chats:
            return False
        if self._group_policy == "allowlist":
            return bool(self._group_allowed_chats)
        if self._group_require_mention and not self._message_matches_mention(msg):
            return False
        return True

    def _message_matches_mention(self, msg: NormalizedGeweMessage) -> bool:
        return bool(self._bot_wxids and msg.mentioned_user_ids and self._bot_wxids & msg.mentioned_user_ids)

    async def _cache_media(self, msg: NormalizedGeweMessage) -> tuple[List[str], List[str]]:
        media_urls: List[str] = []
        media_types: List[str] = []
        if not self._download_media:
            return media_urls, media_types
        for attachment in _flatten_attachments(msg):
            path = ""
            tried_urls: set[str] = set()

            async def cache_candidate(candidate_url: str) -> bool:
                nonlocal path
                if not candidate_url or candidate_url in tried_urls:
                    return False
                tried_urls.add(candidate_url)
                if not _is_http_url(candidate_url):
                    return False
                try:
                    path = await self._cache_url(candidate_url, attachment)
                    return True
                except Exception:
                    logger.warning(
                        "[GeWe] Failed to cache media candidate url=%s kind=%s",
                        safe_url_for_log(candidate_url),
                        attachment.kind,
                        exc_info=True,
                    )
                    return False

            file_url = attachment.url
            if file_url:
                await cache_candidate(file_url)
            if not path and attachment.download_hint:
                async for downloaded_url in self._iter_download_media_urls(attachment.download_hint):
                    if await cache_candidate(downloaded_url):
                        break
            elif not path and file_url and not _is_http_url(file_url):
                logger.warning(
                    "[GeWe] Skipping media cache because attachment URL is not HTTP: kind=%s",
                    attachment.kind,
                )

            if not path:
                continue
            attachment.local_path = path
            media_urls.append(path)
            media_types.append(_media_type_for_attachment(attachment))
        return media_urls, media_types

    async def _download_media_url(self, hint: GeweDownloadHint) -> str:
        async for url in self._iter_download_media_urls(hint):
            return url
        return ""

    async def _iter_download_media_urls(self, hint: GeweDownloadHint) -> AsyncIterator[str]:
        seen_urls: set[str] = set()
        for candidate in [hint, *hint.fallbacks]:
            data = await self._api_post(f"/gewe/v2/api/message/{candidate.endpoint}", candidate.request_body)
            payload = data.get("data") if isinstance(data, dict) else data
            url = _find_http_url(payload, ("fileUrl", "url", "downloadUrl", "file_url"))
            if not url:
                url = _find_http_url(data, ("fileUrl", "url", "downloadUrl", "file_url"))
            if url:
                if url not in seen_urls:
                    seen_urls.add(url)
                    yield url
                continue
            logger.warning(
                "[GeWe] Media download returned no HTTP URL: endpoint=%s request=%s response=%s",
                candidate.endpoint,
                _download_request_summary(candidate.request_body),
                _download_response_summary(data),
            )

    async def _cache_url(self, url: str, attachment: GeweAttachment) -> str:
        if not self._http_client:
            raise RuntimeError("GeWe HTTP client is not connected")
        response = await self._http_client.get(url)
        response.raise_for_status()
        data = response.content
        if attachment.kind in {"image", "emoji"}:
            return cache_image_from_bytes(data, _ext(attachment, ".gif" if attachment.kind == "emoji" else ".jpg"))
        if attachment.kind == "voice":
            return await self._cache_voice_bytes(data, attachment)
        if attachment.kind == "video":
            return cache_video_from_bytes(data, _ext(attachment, ".mp4"))
        return cache_document_from_bytes(data, attachment.file_name or f"gewe-file{_ext(attachment, '')}")

    async def _cache_voice_bytes(self, data: bytes, attachment: GeweAttachment) -> str:
        cached = cache_audio_from_bytes(data, _voice_ext_for_data(data, attachment))
        converted = await asyncio.to_thread(_convert_silk_to_mp3, cached)
        return converted or cached

    def _message_text(self, msg: NormalizedGeweMessage) -> str:
        if msg.message_type == "text":
            return msg.text
        if msg.message_type == "chat_record":
            lines = [f"[聊天记录] {len(msg.items)} 条"]
            for item in msg.items:
                prefix = item.sender_id or "unknown"
                lines.append(f"- {prefix}: {self._message_text(item)}")
            return "\n".join(lines)
        if msg.text:
            return msg.text
        if msg.attachments:
            return _attachment_summary(msg.attachments[0])
        return f"[{msg.message_type}]"

    def _read_last_event_id(self) -> str:
        try:
            return self._last_event_id_file.read_text(encoding="utf-8").strip()
        except OSError:
            return ""

    def _write_last_event_id(self, value: str) -> None:
        self._last_event_id_file.parent.mkdir(parents=True, exist_ok=True)
        self._last_event_id_file.write_text(f"{value}\n", encoding="utf-8")


def normalize_gewe_callback(payload: Any) -> Optional[NormalizedGeweMessage]:
    if not isinstance(payload, dict):
        return None
    if "msgType" not in payload or ("fromUser" not in payload and "toUser" not in payload):
        return None
    device_id = _str(payload.get("appid") or payload.get("appId"))
    from_user = _str(payload.get("fromUser"))
    to_user = _str(payload.get("toUser"))
    account_id = _str(payload.get("wxid")) or to_user
    from_group = _str(payload.get("fromGroup") or payload.get("roomWxid") or payload.get("groupId"))
    msg_type_raw = _str(payload.get("msgType")).upper()
    content = _str(payload.get("content") or payload.get("text"))
    xml = content if _looks_like_xml(content) else ""
    message_type = _detect_app_message_type(xml) if msg_type_raw == "APP_MSG" and xml else _map_message_type(msg_type_raw)
    event_code = _str(payload.get("eventCode")).lower()
    conversation_type = "group" if from_group or from_user.endswith("@chatroom") or to_user.endswith("@chatroom") or event_code == "group_msg_event" else "direct"
    if conversation_type == "group":
        peer_id = from_group or _first_chatroom(from_user, to_user)
    else:
        peer_id = to_user if from_user == account_id else from_user
    msg = NormalizedGeweMessage(
        account_id=account_id,
        device_id=device_id,
        peer_id=peer_id,
        sender_id=from_user,
        conversation_type=conversation_type,
        message_type=message_type,
        provider_message_id=_str(payload.get("newMsgId") or payload.get("msgId") or payload.get("id")),
        text=content if message_type == "text" else "",
        content_xml=xml,
        create_time=_int(payload.get("createTime")),
        is_self=_bool(payload.get("isSelf")),
        mentioned_user_ids=_extract_mentioned_user_ids(payload),
        raw=payload,
    )
    if xml and message_type != "text":
        msg.attachments = _attachments_from_xml(message_type, xml, device_id or account_id)
    if message_type == "quote" and msg.attachments:
        quote = msg.attachments[0]
        msg.text = quote.title or "[引用消息]"
        msg.reply_to_message_id = quote.quoted_message_id
        msg.reply_to_text = quote.quoted_text
    if message_type == "chat_record" and xml:
        msg.items = _chat_record_items_from_xml(msg, xml)
    return msg


def _attachments_from_xml(message_type: str, xml: str, app_id: str) -> List[GeweAttachment]:
    root = _parse_xml(xml)
    if root is None:
        return [GeweAttachment(kind=message_type, raw=xml)]
    appmsg = root.find(".//appmsg")
    if appmsg is not None:
        return [_appmsg_attachment(appmsg, xml, app_id, message_type)]
    emoji = root.find(".//emoji")
    if emoji is not None:
        return [_emoji_attachment(emoji, xml, app_id)]
    img = root.find(".//img")
    if img is not None:
        return [_cdn_attachment("image", img, xml, app_id)]
    voice = root.find(".//voicemsg")
    if voice is not None:
        return [_cdn_attachment("voice", voice, xml, app_id)]
    video = root.find(".//videomsg")
    if video is not None:
        return [_cdn_attachment("video", video, xml, app_id)]
    return [GeweAttachment(kind=message_type, raw=xml)]


def _chat_record_items_from_xml(base: NormalizedGeweMessage, xml: str) -> List[NormalizedGeweMessage]:
    root = _parse_xml(xml)
    if root is None:
        return []
    record_text = _text(root.find(".//recorditem"))
    record_root = _parse_xml(unescape(record_text))
    if record_root is None:
        return []
    items: List[NormalizedGeweMessage] = []
    for index, item in enumerate(record_root.findall(".//dataitem")):
        data_type = _int(item.attrib.get("datatype") or _text(item.find("datatype"))) or 0
        message_type = _record_message_type(item, data_type)
        text = _text(item.find("datadesc")) or _text(item.find("sourcename"))
        sender = _text(item.find("sourcename")) or base.sender_id
        normalized = NormalizedGeweMessage(
            account_id=base.account_id,
            device_id=base.device_id,
            peer_id=base.peer_id,
            sender_id=sender,
            conversation_type=base.conversation_type,
            message_type=message_type,
            provider_message_id=_text(item.find("fromnewmsgid")) or f"{base.provider_message_id}:{index}",
            text=text if message_type == "text" or _is_text_only_voice_item(item, data_type) else "",
            raw={"record_item": ET.tostring(item, encoding="unicode")},
        )
        attachment = _record_item_attachment(item, message_type, base.device_id or base.account_id)
        if attachment:
            normalized.attachments = [attachment]
        items.append(normalized)
    return items


def _appmsg_attachment(appmsg: ET.Element, xml: str, app_id: str, fallback_type: str) -> GeweAttachment:
    app_type = _int(_text(appmsg.find("type"))) or 0
    kind = "file" if app_type == 6 else "link" if app_type == 5 else "mini_program" if app_type == 33 else fallback_type
    appattach = appmsg.find("appattach")
    refermsg = appmsg.find("refermsg")
    attachment = GeweAttachment(
        kind=kind,
        title=_text(appmsg.find("title")),
        description=_text(appmsg.find("des")),
        url=_text(appmsg.find("url")) or _text(appmsg.find("dataurl")) or _child_text(appattach, "tpurl"),
        thumb_url=_text(appmsg.find("thumburl")) or _child_text(appattach, "cdnthumburl"),
        file_name=_text(appmsg.find("title")),
        file_ext=_child_text(appattach, "fileext"),
        file_size=_int(_child_text(appattach, "totallen")),
        md5=_text(appmsg.find("md5")),
        aes_key=_child_text(appattach, "aeskey"),
        cdn_file_id=_child_text(appattach, "cdnattachurl") or _child_text(appattach, "attachid"),
        quoted_message_id=_child_text(refermsg, "svrid"),
        quoted_sender_id=_child_text(refermsg, "chatusr"),
        quoted_sender_name=_child_text(refermsg, "displayname"),
        quoted_text=_quote_text_from_refermsg(refermsg),
        quoted_message_type=_refermsg_type(_child_text(refermsg, "type")),
    )
    return _with_download_hint(attachment, xml, app_id)


def _cdn_attachment(kind: str, source: ET.Element, xml: str, app_id: str) -> GeweAttachment:
    attachment = GeweAttachment(
        kind=kind,
        file_size=_int(source.attrib.get("length") or source.attrib.get("cdnthumblength")),
        md5=_str(source.attrib.get("md5")),
        aes_key=_str(source.attrib.get("aeskey") or source.attrib.get("cdnthumbaeskey")),
        cdn_file_id=_str(source.attrib.get("cdnmidimgurl") or source.attrib.get("cdnvideourl") or source.attrib.get("voiceurl") or source.attrib.get("cdnthumburl")),
        cdn_file_ids=_voice_cdn_file_ids(source) if kind == "voice" else [],
        file_ext=_suffix_for_kind(kind) if kind == "voice" else "",
        thumb_url=_str(source.attrib.get("cdnthumburl")),
        duration_seconds=_duration_seconds(source.attrib.get("playlength") or source.attrib.get("voicelength")),
    )
    return _with_download_hint(attachment, xml, app_id)


def _emoji_attachment(source: ET.Element, xml: str, app_id: str) -> GeweAttachment:
    direct_url = _str(
        source.attrib.get("cdnurl")
        or source.attrib.get("thumburl")
        or source.attrib.get("externurl")
        or source.attrib.get("encrypturl")
    )
    attachment = GeweAttachment(
        kind="emoji",
        title="表情",
        file_ext=_emoji_file_ext(source),
        file_size=_int(
            source.attrib.get("len")
            or source.attrib.get("cdnthumblength")
            or source.attrib.get("length")
        ),
        url=direct_url if direct_url.startswith(("http://", "https://")) else "",
        thumb_url=_str(source.attrib.get("thumburl") or source.attrib.get("cdnthumburl")),
        md5=_str(source.attrib.get("md5") or source.attrib.get("externmd5")),
        aes_key=_str(source.attrib.get("aeskey") or source.attrib.get("cdnthumbaeskey")),
        cdn_file_id=_str(
            source.attrib.get("cdnurl")
            or source.attrib.get("encrypturl")
            or source.attrib.get("thumburl")
            or source.attrib.get("cdnthumburl")
            or source.attrib.get("md5")
        ),
        raw=xml,
    )
    if attachment.cdn_file_id and attachment.aes_key:
        attachment.needs_download = True
        attachment.download_hint = GeweDownloadHint("downloadCdn", {
            "appId": app_id,
            "aesKey": attachment.aes_key,
            "totalSize": str(attachment.file_size or ""),
            "type": _cdn_download_type(attachment.kind),
            "fileId": attachment.cdn_file_id,
            "suffix": attachment.file_ext or _suffix_for_kind(attachment.kind),
        })
    return attachment


def _quote_text_from_refermsg(refermsg: Optional[ET.Element]) -> str:
    if refermsg is None:
        return ""
    content = unescape(_child_text(refermsg, "content"))
    quoted_type = _refermsg_type(_child_text(refermsg, "type"))
    if _looks_like_xml(content):
        attachments = _attachments_from_xml(quoted_type, content, "")
        if attachments:
            content = _attachment_summary(attachments[0])
    elif not content and quoted_type != "unknown":
        content = f"[{quoted_type}]"
    sender = _child_text(refermsg, "displayname") or _child_text(refermsg, "chatusr")
    return f"{sender}: {content}" if sender and content else content


def _record_item_attachment(item: ET.Element, message_type: str, app_id: str) -> Optional[GeweAttachment]:
    if message_type == "text":
        return None
    kind = _attachment_kind(message_type)
    url = _first_text(item, "dataurl", "streamdataurl", "cdndataurl")
    thumb_url = _first_text(item, "thumburl", "cdnthumburl")
    cdn_file_id, aes_key, file_size = _record_cdn_download_fields(item)
    attachment = GeweAttachment(
        kind=kind,
        title=_first_text(item, "datatitle", "sourcename"),
        description=_text(item.find("datadesc")),
        file_name=_first_text(item, "datatitle", "filename"),
        file_ext=_record_file_ext(item, url or thumb_url, kind),
        file_size=file_size,
        url=url,
        thumb_url=thumb_url,
        md5=_first_text(item, "fullmd5", "dataitemmd5", "md5"),
        aes_key=aes_key,
        cdn_file_id=cdn_file_id,
        cdn_file_ids=[cdn_file_id] if cdn_file_id else [],
        duration_seconds=_first_int(item, "duration", "playlength", "voicelength"),
        raw=ET.tostring(item, encoding="unicode"),
    )
    if attachment.kind == "image":
        image_hints = _record_image_download_hints(item, app_id, attachment.file_ext or _suffix_for_kind(attachment.kind))
        if image_hints:
            attachment.needs_download = True
            attachment.download_hint = image_hints[0]
            attachment.download_hint.fallbacks.extend(image_hints[1:])
    elif attachment.cdn_file_id and attachment.aes_key:
        attachment.needs_download = True
        hints = _voice_cdn_download_hints(attachment, app_id) if attachment.kind == "voice" else []
        if hints:
            attachment.download_hint = hints[0]
            attachment.download_hint.fallbacks.extend(hints[1:])
        else:
            attachment.download_hint = GeweDownloadHint("downloadCdn", {
                "appId": app_id,
                "aesKey": attachment.aes_key,
                "totalSize": str(attachment.file_size or ""),
                "type": _cdn_download_type(attachment.kind),
                "fileId": attachment.cdn_file_id,
                "suffix": attachment.file_ext or _suffix_for_kind(attachment.kind),
            })
    return attachment


def _with_download_hint(attachment: GeweAttachment, xml: str, app_id: str) -> GeweAttachment:
    endpoint = {"image": "downloadImage", "voice": "downloadVoice", "video": "downloadVideo", "file": "downloadFile"}.get(attachment.kind)
    if endpoint:
        body: Dict[str, Any] = {"appId": app_id, "xml": xml}
        if attachment.kind == "image":
            body["type"] = 2
        attachment.needs_download = True
        attachment.download_hint = GeweDownloadHint(endpoint, body)
        if attachment.kind == "voice":
            attachment.download_hint.fallbacks.extend(_voice_cdn_download_hints(attachment, app_id))
    return attachment


def _parse_xml(value: str) -> Optional[ET.Element]:
    if not value or not _looks_like_xml(value):
        return None
    try:
        return ET.fromstring(value.strip())
    except ET.ParseError:
        return None


def _detect_app_message_type(xml: str) -> str:
    root = _parse_xml(xml)
    app_type = _int(_text(root.find(".//appmsg/type")) if root is not None else "") or 0
    return {19: "chat_record", 57: "quote", 6: "file", 5: "link", 33: "mini_program"}.get(app_type, "unknown")


def _map_message_type(value: str) -> str:
    return {
        "TEXT": "text",
        "IMAGE": "image",
        "VOICE": "voice",
        "VIDEO": "video",
        "FILE": "file",
        "LINK": "link",
        "CHAT_RECORD": "chat_record",
        "QUOTE": "quote",
        "MINI_PROGRAM": "mini_program",
        "EMOJI": "emoji",
        "SYSTEM": "system",
        "REVOKE_MSG": "system",
        "PAT_MSG": "system",
    }.get(value, "unknown")


def _to_hermes_type(value: str) -> MessageType:
    return {
        "image": MessageType.PHOTO,
        "emoji": MessageType.PHOTO,
        "voice": MessageType.VOICE,
        "video": MessageType.VIDEO,
        "file": MessageType.DOCUMENT,
    }.get(value, MessageType.TEXT)


def _record_message_type(item: ET.Element, data_type: int) -> str:
    if _is_text_only_voice_item(item, data_type):
        return "voice"
    return {1: "text", 2: "image", 3: "voice", 4: "video", 5: "link", 6: "file", 8: "file"}.get(data_type, "unknown")


def _refermsg_type(value: str) -> str:
    return {
        "1": "text",
        "3": "image",
        "34": "voice",
        "43": "video",
        "49": "app_msg",
    }.get(str(value or "").strip(), "unknown")


def _is_text_only_voice_item(item: ET.Element, data_type: int) -> bool:
    return data_type == 1 and _text(item.find("datadesc")).startswith("[语音]")


def _flatten_attachments(msg: NormalizedGeweMessage) -> List[GeweAttachment]:
    attachments = list(msg.attachments)
    for item in msg.items:
        attachments.extend(_flatten_attachments(item))
    return attachments


def _attachment_summary(attachment: GeweAttachment) -> str:
    local_path = f" 本地路径: {attachment.local_path}" if attachment.local_path else ""
    if attachment.kind == "image":
        return f"[图片]{local_path}"
    if attachment.kind == "emoji":
        return f"[表情]{local_path}"
    if attachment.kind == "voice":
        return f"[语音]{local_path}"
    if attachment.kind == "video":
        return f"[视频]{local_path}"
    if attachment.kind == "file":
        name = attachment.file_name or attachment.title or "文件"
        size = f" ({attachment.file_size} bytes)" if attachment.file_size else ""
        return f"[文件] {name}{size}{local_path}"
    if attachment.kind == "link":
        return f"[链接] {attachment.title or attachment.url}"
    return f"[{attachment.kind}]{local_path}"


def _media_type_for_attachment(attachment: GeweAttachment) -> str:
    return {
        "image": "image/jpeg",
        "emoji": "image/gif",
        "voice": "audio/mpeg" if attachment.local_path.lower().endswith(".mp3") else "audio/silk",
        "video": "video/mp4",
        "file": "application/octet-stream",
    }.get(attachment.kind, attachment.kind)


def _gewe_ok(data: Dict[str, Any]) -> bool:
    value = data.get("ret", data.get("code", 0))
    message = str(data.get("msg", "")).strip().lower()
    return value in (0, "0", 200, "200", None) or message in {"success", "操作成功"}


def _parse_sse(raw: str) -> Dict[str, str]:
    message: Dict[str, str] = {}
    data: List[str] = []
    for line in raw.splitlines():
        if not line or line.startswith(":"):
            continue
        key, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if key == "data":
            data.append(value)
        elif key in {"id", "event"}:
            message[key] = value
    if data:
        message["data"] = "\n".join(data)
    return message


def _extract_mentioned_user_ids(payload: Dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for key in (
        "atUserList",
        "atuserlist",
        "atUsers",
        "atWxidList",
        "atWxids",
        "atUserName",
        "mentionedUsers",
        "mentionUsers",
        "mentionWxids",
    ):
        values.update(_split_csv(payload.get(key)))
    msg_source = _str(payload.get("msgSource") or payload.get("msgsource"))
    if msg_source:
        root = _parse_xml(unescape(msg_source))
        if root is not None:
            values.update(_split_csv(_text(root.find(".//atuserlist"))))
    return values


def _looks_like_relay(value: Dict[str, Any]) -> bool:
    return "body" in value and ("source_id" in value or "channel" in value or "received_at" in value)


def _looks_like_xml(value: str) -> bool:
    return isinstance(value, str) and value.strip().startswith("<")


def _first_chatroom(a: str, b: str) -> str:
    return a if a.endswith("@chatroom") else b


def _attachment_kind(value: str) -> str:
    return value if value in {"image", "voice", "video", "file", "link", "mini_program", "emoji"} else "unknown"


def _cdn_download_type(kind: str) -> str:
    return {"image": "2", "emoji": "2", "voice": "3", "video": "4", "file": "5"}.get(kind, "5")


def _suffix_for_kind(kind: str) -> str:
    return {"image": "jpg", "emoji": "gif", "voice": "silk", "video": "mp4"}.get(kind, "")


def _record_cdn_download_fields(item: ET.Element) -> tuple[str, str, Optional[int]]:
    candidates = _record_cdn_download_field_candidates(item)
    if candidates:
        return candidates[0]
    return "", "", _first_int(item, "fullmd5size", "datasize", "totallen", "length")


def _record_cdn_download_field_candidates(item: ET.Element) -> List[tuple[str, str, Optional[int]]]:
    pairs = (
        (
            _first_text(item, "cdndataurl", "cdnmidimgurl", "cdnvideourl", "voiceurl", "cdnattachurl", "attachid"),
            _first_text(item, "cdndatakey", "dataurlkey", "aeskey"),
            _first_int(item, "fullmd5size", "datasize", "totallen", "length"),
        ),
        (
            _text(item.find("cdnthumburl")),
            _first_text(item, "cdnthumbkey", "cdnthumbaeskey"),
            _first_int(item, "cdnthumblength", "thumbsize", "thumbfullsize"),
        ),
    )
    candidates: List[tuple[str, str, Optional[int]]] = []
    seen: set[tuple[str, str]] = set()
    for file_id, aes_key, size in pairs:
        if not (file_id and aes_key):
            continue
        key = (file_id, aes_key)
        if key in seen:
            continue
        seen.add(key)
        candidates.append((file_id, aes_key, size))
    return candidates


def _voice_cdn_download_hints(attachment: GeweAttachment, app_id: str) -> List[GeweDownloadHint]:
    if not attachment.aes_key:
        return []
    file_ids = _voice_download_file_ids(attachment)
    if not file_ids:
        return []
    suffixes = _voice_download_suffixes(attachment)
    total_sizes = [str(attachment.file_size or "")]
    if attachment.file_size:
        total_sizes.append("")
    hints = []
    for file_id in file_ids:
        for download_type in ("5", _cdn_download_type("voice")):
            for suffix in suffixes:
                for total_size in total_sizes:
                    hints.append(GeweDownloadHint("downloadCdn", {
                        "appId": app_id,
                        "aesKey": attachment.aes_key,
                        "totalSize": total_size,
                        "type": download_type,
                        "fileId": file_id,
                        "suffix": suffix,
                    }))
    return _dedupe_download_hints(hints)


def _voice_download_file_ids(attachment: GeweAttachment) -> List[str]:
    file_ids: List[str] = []
    for candidate in [attachment.cdn_file_id, *attachment.cdn_file_ids]:
        value = str(candidate or "").strip()
        if value and value not in file_ids:
            file_ids.append(value)
    return file_ids


def _voice_cdn_file_ids(source: ET.Element) -> List[str]:
    file_ids: List[str] = []
    for name in ("voiceurl", "bufid", "clientmsgid", "voicemd5", "cdnvoiceurl", "cdnurl"):
        value = _str(source.attrib.get(name)).strip()
        if value and value not in file_ids:
            file_ids.append(value)
    return file_ids


def _voice_download_suffixes(attachment: GeweAttachment) -> List[str]:
    candidates = [
        attachment.file_ext,
        _suffix_for_kind(attachment.kind),
        "silk",
        "amr",
        "mp3",
        "",
    ]
    suffixes: List[str] = []
    for candidate in candidates:
        suffix = str(candidate or "").strip().lower().lstrip(".")
        if suffix not in suffixes:
            suffixes.append(suffix)
    return suffixes


def _record_image_download_hints(item: ET.Element, app_id: str, suffix: str) -> List[GeweDownloadHint]:
    hints: List[GeweDownloadHint] = []
    image_xml = _record_image_xml(item)
    if image_xml:
        for image_type in _image_download_types(item):
            hints.append(GeweDownloadHint("downloadImage", {"appId": app_id, "xml": image_xml, "type": image_type}))
    for file_id, aes_key, size in _record_cdn_download_field_candidates(item):
        for image_type in _image_download_types(item):
            hints.append(GeweDownloadHint("downloadCdn", {
                "appId": app_id,
                "aesKey": aes_key,
                "totalSize": str(size or ""),
                "type": str(image_type),
                "fileId": file_id,
                "suffix": suffix,
            }))
    return _dedupe_download_hints(hints)


def _image_download_types(item: ET.Element) -> List[int]:
    preferred = [2, 3, 1]
    if not _first_text(item, "cdndataurl", "cdnmidimgurl") and _text(item.find("cdnthumburl")):
        preferred = [3, 2, 1]
    return preferred


def _dedupe_download_hints(hints: List[GeweDownloadHint]) -> List[GeweDownloadHint]:
    deduped: List[GeweDownloadHint] = []
    seen: set[str] = set()
    for hint in hints:
        key = json.dumps({"endpoint": hint.endpoint, "request_body": hint.request_body}, sort_keys=True, ensure_ascii=True)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(hint)
    return deduped


def _record_image_xml(item: ET.Element) -> str:
    mid_url = _first_text(item, "cdndataurl", "cdnmidimgurl")
    thumb_url = _text(item.find("cdnthumburl")) or mid_url
    aes_key = _first_text(item, "cdndatakey", "dataurlkey", "aeskey", "cdnthumbkey", "cdnthumbaeskey")
    thumb_key = _first_text(item, "cdnthumbkey", "cdnthumbaeskey") or aes_key
    if not (mid_url or thumb_url) or not (aes_key or thumb_key):
        return ""

    attrs = {
        "aeskey": aes_key or thumb_key,
        "encryver": _first_text(item, "encryver", "dataitemsource") or "1",
        "cdnthumbaeskey": thumb_key or aes_key,
        "cdnthumburl": thumb_url or mid_url,
        "cdnthumblength": str(_first_int(item, "cdnthumblength", "thumbsize", "thumbfullsize") or ""),
        "cdnthumbheight": str(_first_int(item, "cdnthumbheight", "thumbheight") or 0),
        "cdnthumbwidth": str(_first_int(item, "cdnthumbwidth", "thumbwidth") or 0),
        "cdnmidheight": str(_first_int(item, "cdnmidheight", "height") or 0),
        "cdnmidwidth": str(_first_int(item, "cdnmidwidth", "width") or 0),
        "cdnhdheight": str(_first_int(item, "cdnhdheight") or 0),
        "cdnhdwidth": str(_first_int(item, "cdnhdwidth") or 0),
        "cdnmidimgurl": mid_url or thumb_url,
        "length": str(_first_int(item, "fullmd5size", "datasize", "length", "totallen") or ""),
        "md5": _first_text(item, "fullmd5", "dataitemmd5", "md5"),
    }
    img = ET.Element("img", {key: value for key, value in attrs.items() if value != ""})
    root = ET.Element("msg")
    root.append(img)
    ET.SubElement(root, "platform_signature")
    ET.SubElement(root, "imgdatahash")
    return ET.tostring(root, encoding="unicode")


def _record_file_ext(item: ET.Element, url: str, kind: str) -> str:
    if kind == "voice":
        return _first_text(item, "fileext", "datafmt").lower().strip().lstrip(".") or "silk"
    candidate = _first_text(item, "fileext", "datafmt").lower().strip().lstrip(".")
    if candidate:
        return candidate
    path = urlsplit(url).path.lower() if url else ""
    suffix = Path(path).suffix.lstrip(".")
    return suffix or _suffix_for_kind(kind)


def _emoji_file_ext(source: ET.Element) -> str:
    candidate = _str(source.attrib.get("type") or source.attrib.get("fileext") or "").lower().strip().lstrip(".")
    if candidate in {"gif", "png", "jpg", "jpeg", "webp"}:
        return candidate
    for attr in ("cdnurl", "thumburl", "externurl", "encrypturl"):
        value = _str(source.attrib.get(attr)).lower().split("?", 1)[0]
        for ext in (".gif", ".png", ".jpg", ".jpeg", ".webp"):
            if value.endswith(ext):
                return ext.lstrip(".")
    return "gif"



def _voice_ext_for_data(data: bytes, attachment: GeweAttachment) -> str:
    if _looks_like_silk(data):
        return ".silk"
    if data[:4] == b"RIFF":
        return ".wav"
    if data[:4] == b"OggS":
        return ".ogg"
    if data[:4] == b"fLaC":
        return ".flac"
    if data[:3] == b"ID3" or data[:2] in (b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"):
        return ".mp3"
    if data[:5] == b"#!AMR":
        return ".amr"
    if len(data) >= 12 and data[4:8] == b"ftyp":
        return ".m4a"
    return _ext(attachment, ".silk")


def _looks_like_silk(data: bytes) -> bool:
    return data.startswith(b"#!SILK") or data.startswith(b"\x02#!SILK") or data.startswith(b"\x02!")


def _silk_decoder_command() -> List[str]:
    configured = os.getenv(GEWE_SILK_DECODER_ENV, "").strip()
    if configured:
        return configured.split()
    for name in ("silk_v3_decoder", "silk-decoder", "decoder"):
        found = shutil.which(name)
        if found:
            return [found]
    return []


def _convert_silk_to_mp3(path: str) -> str:
    source = Path(path)
    if not source.exists():
        return ""
    if source.suffix.lower() == ".mp3":
        return str(source)

    ffmpeg = shutil.which("ffmpeg")
    if _looks_like_silk(source.read_bytes()[:16]):
        if ffmpeg:
            converted = _convert_silk_with_pilk(source, ffmpeg)
            if converted:
                return converted
        decoder = _silk_decoder_command()
        if decoder and ffmpeg:
            converted = _convert_silk_with_decoder(source, decoder, ffmpeg)
            if converted:
                return converted
        if not decoder:
            logger.info(
                "[GeWe] Silk decoder not found; install pilk or set %s to the kn007 decoder path for reliable voice transcription",
                GEWE_SILK_DECODER_ENV,
            )

    converted = _convert_audio_with_ffmpeg(source, ffmpeg)
    if converted:
        return converted
    if source.suffix.lower() == ".silk":
        logger.info("[GeWe] Silk voice cached without MP3 conversion")
    return ""


def _convert_silk_with_pilk(source: Path, ffmpeg: str) -> str:
    try:
        import pilk
    except ImportError:
        return ""

    wav_path = source.with_suffix(".wav")
    try:
        pilk.silk_to_wav(str(source), str(wav_path), rate=16000)
        converted = _convert_audio_with_ffmpeg(wav_path, ffmpeg)
        if converted:
            return converted
    except Exception as exc:
        logger.debug("[GeWe] pilk direct silk conversion failed: %s", exc)

    if source.suffix.lower() != ".silk":
        silk_path = source.with_suffix(".silk")
        try:
            shutil.copy2(source, silk_path)
            pilk.silk_to_wav(str(silk_path), str(wav_path), rate=16000)
            converted = _convert_audio_with_ffmpeg(wav_path, ffmpeg)
            if converted:
                return converted
        except Exception as exc:
            logger.debug("[GeWe] pilk .silk conversion failed: %s", exc)
        finally:
            try:
                silk_path.unlink(missing_ok=True)
            except OSError:
                pass
    try:
        wav_path.unlink(missing_ok=True)
    except OSError:
        pass
    return ""


def _convert_silk_with_decoder(source: Path, decoder: List[str], ffmpeg: str) -> str:
    pcm_path = source.with_suffix(".pcm")
    mp3_path = source.with_suffix(".mp3")
    try:
        subprocess.run([*decoder, str(source), str(pcm_path)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30)
        subprocess.run(
            [ffmpeg, "-y", "-f", "s16le", "-ar", "24000", "-ac", "1", "-i", str(pcm_path), str(mp3_path)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=30,
        )
        if mp3_path.exists() and mp3_path.stat().st_size > 0:
            return str(mp3_path)
    except Exception as exc:
        logger.warning("[GeWe] Failed to convert silk voice to mp3 with decoder: %s", exc)
    finally:
        try:
            pcm_path.unlink(missing_ok=True)
        except OSError:
            pass
    return ""


def _convert_audio_with_ffmpeg(source: Path, ffmpeg: str | None) -> str:
    if not ffmpeg:
        return ""
    mp3_path = source.with_suffix(".mp3")
    try:
        subprocess.run(
            [ffmpeg, "-y", "-i", str(source), "-vn", "-acodec", "libmp3lame", str(mp3_path)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=30,
        )
        if mp3_path.exists() and mp3_path.stat().st_size > 0:
            return str(mp3_path)
    except Exception as exc:
        logger.debug("[GeWe] ffmpeg direct audio conversion failed for %s: %s", source.suffix or "audio", exc)
    return ""

def _ext(attachment: GeweAttachment, default: str) -> str:
    ext = attachment.file_ext.strip().lstrip(".") if attachment.file_ext else ""
    return f".{ext}" if ext else default


def _text(node: Optional[ET.Element]) -> str:
    return "" if node is None or node.text is None else str(node.text).strip()


def _child_text(node: Optional[ET.Element], name: str) -> str:
    return _text(node.find(name)) if node is not None else ""


def _first_text(node: ET.Element, *names: str) -> str:
    for name in names:
        value = _text(node.find(name))
        if value:
            return value
    return ""


def _first_int(node: ET.Element, *names: str) -> Optional[int]:
    for name in names:
        value = _int(_text(node.find(name)))
        if value is not None:
            return value
    return None


def _download_request_summary(body: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    for key, value in body.items():
        if key == "xml":
            root = _parse_xml(_str(value))
            summary[key] = _xml_shape(root) if root is not None else {"chars": len(_str(value)), "valid_xml": False}
        elif key in {"aesKey", "fileId"}:
            summary[key] = _short_fingerprint(value)
        elif key == "appId":
            summary[key] = _safe_id(_str(value))
        else:
            summary[key] = value
    return summary


def _download_response_summary(value: Any) -> Any:
    if isinstance(value, dict):
        summary: Dict[str, Any] = {}
        for key, nested in value.items():
            if key in {"ret", "code", "msg", "message"}:
                summary[key] = _safe_response_text(nested)
            elif key == "data" and isinstance(nested, dict):
                summary[key] = _download_response_data_summary(nested)
            else:
                summary[key] = _value_shape(nested)
        return summary
    return _value_shape(value)


def _download_response_data_summary(value: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"type": "dict", "keys": sorted(str(key) for key in value.keys())[:30]}
    for key in ("code", "ret", "msg", "message", "detail", "error"):
        if key in value:
            summary[key] = _safe_response_text(value.get(key))
    return summary


def _safe_response_text(value: Any) -> str:
    text = _redact_text(_str(value))
    if _is_http_url(text):
        return "http-url"
    return text[:180]


def _value_shape(value: Any) -> Any:
    if isinstance(value, dict):
        return {"type": "dict", "keys": sorted(str(key) for key in value.keys())[:30]}
    if isinstance(value, list):
        return {"type": "list", "len": len(value), "items": [_value_shape(item) for item in value[:3]]}
    if isinstance(value, str):
        if _is_http_url(value):
            return "http-url"
        return {"type": "str", "chars": len(value), "sample": _redact_text(value[:80])}
    return {"type": type(value).__name__, "value": value}


def _xml_shape(root: ET.Element) -> Dict[str, Any]:
    tags = [elem.tag for elem in root.iter()][:20]
    attrs: Dict[str, List[str]] = {}
    for elem in root.iter():
        if elem.attrib:
            attrs[elem.tag] = sorted(elem.attrib.keys())
    return {"root": root.tag, "tags": tags, "attrs": attrs}


def _short_fingerprint(value: Any) -> str:
    text = _str(value)
    if not text:
        return ""
    if len(text) <= 12:
        return f"len={len(text)}"
    return f"len={len(text)} {text[:4]}...{text[-4:]}"


def _redact_text(value: str) -> str:
    text = _str(value)
    if text.startswith("sk-"):
        return "sk-***"
    return text


def _find_http_url(value: Any, preferred_keys: tuple[str, ...] = ()) -> str:
    if isinstance(value, str):
        return value if _is_http_url(value) else ""
    if isinstance(value, dict):
        for key in preferred_keys:
            url = _find_http_url(value.get(key), preferred_keys)
            if url:
                return url
        for nested in value.values():
            url = _find_http_url(nested, preferred_keys)
            if url:
                return url
    if isinstance(value, list):
        for nested in value:
            url = _find_http_url(nested, preferred_keys)
            if url:
                return url
    return ""


def _is_http_url(value: str) -> bool:
    return str(value or "").startswith(("http://", "https://"))

def _str(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _duration_seconds(value: Any) -> Optional[int]:
    number = _int(value)
    if number is None:
        return None
    return number // 1000 if number > 1000 else number


def _bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return None


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(value)


def _as_float(value: Any, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _split_csv(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    return {part.strip() for part in str(value or "").split(",") if part.strip()}


def _safe_id(value: str, keep: int = 8) -> str:
    return value if len(value) <= keep else f"{value[:keep]}..."
