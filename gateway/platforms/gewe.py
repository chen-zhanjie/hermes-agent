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
import os
import socket as _socket
import tempfile
import time
from dataclasses import dataclass, field
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin
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
)
from gateway.platforms.helpers import MessageDeduplicator
from hermes_constants import get_default_hermes_root, get_hermes_home

logger = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "https://api.geweapi.com"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8656
DEFAULT_DIRECT_PATH = "/gewe/callback"
DEFAULT_RELAY_PATH = "/gewe/relay"
DEFAULT_RELAY_BASE_URL = "https://hook.yunzxu.com"
SUPPORTED_INBOUND_MODES = {"direct-callback", "relay-callback", "relay-sse"}
_INSECURE_NO_AUTH = "INSECURE_NO_AUTH"
DEFAULT_PROFILE_ROUTER_STORE = "platforms/gewe/bindings.json"


def check_gewe_requirements() -> bool:
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE


@dataclass
class GeweDownloadHint:
    endpoint: str
    request_body: Dict[str, Any]


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
    duration_seconds: Optional[int] = None
    quoted_message_id: str = ""
    quoted_sender_id: str = ""
    quoted_sender_name: str = ""
    quoted_text: str = ""
    quoted_message_type: str = ""
    needs_download: bool = False
    download_hint: Optional[GeweDownloadHint] = None
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


@dataclass
class GeweProfileBinding:
    type: str
    identity: str
    profile: str
    name: str = ""
    listen_all: bool = False
    source: str = "manual"



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

        self._profile_router_store = Path(
            extra.get("profile_router_store")
            or os.getenv("GEWE_PROFILE_ROUTER_STORE")
            or (get_default_hermes_root() / DEFAULT_PROFILE_ROUTER_STORE)
        )
        self._current_profile = _current_profile_name()

        self._download_media = _as_bool(extra.get("download_media"), True)
        self._dedup = MessageDeduplicator(ttl_seconds=300)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._sse_task: Optional[asyncio.Task] = None
        self._last_event_id_file = Path(extra.get("last_event_id_file") or (get_hermes_home() / "gewe_last_event_id"))

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
        lock_identity = f"{self._api_base_url}:{self._app_id}:{self._token}"
        if not self._acquire_platform_lock("gewe-app", lock_identity, "GeWe app/token"):
            return False

        self._http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

        if self._inbound_mode in {"direct-callback", "relay-callback"}:
            if not await self._start_callback_server():
                await self._cleanup()
                self._release_platform_lock()
                return False
        self._mark_connected()
        if self._inbound_mode == "relay-sse":
            self._sse_task = asyncio.create_task(self._sse_loop())
        logger.info("[GeWe] Connected mode=%s app_id=%s", self._inbound_mode, _safe_id(self._app_id))
        return True

    async def disconnect(self) -> None:
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass
            self._sse_task = None
        await self._cleanup()
        self._release_platform_lock()
        self._mark_disconnected()
        logger.info("[GeWe] Disconnected")

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
            "imageUrl": image_url,
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
        if file_path.startswith(("http://", "https://")):
            return await self._post_message("/gewe/v2/api/message/postFile", {
                "appId": self._app_id,
                "toWxid": chat_id,
                "fileUrl": file_path,
                "fileName": file_name or Path(file_path).name or "file",
            })
        return await self.send(chat_id, f"{caption + chr(10) if caption else ''}[文件] {file_name or Path(file_path).name}: {file_path}")

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "group" if chat_id.endswith("@chatroom") else "dm"}

    async def _post_message(self, path: str, payload: Dict[str, Any]) -> SendResult:
        try:
            data = await self._api_post(path, payload)
            ok = _gewe_ok(data)
            return SendResult(
                success=ok,
                message_id=str((data.get("data") or {}).get("msgId") or (data.get("data") or {}).get("newMsgId") or "") if isinstance(data.get("data"), dict) else "",
                error=None if ok else str(data),
                raw_response=data,
                retryable=not ok,
            )
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

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

        routed = await self._route_profile_message(msg)
        if routed:
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
        await self.handle_message(event)

    async def _route_profile_message(self, msg: NormalizedGeweMessage) -> bool:
        store = _load_profile_router_store(self._profile_router_store)
        if not store:
            return False
        if _store_has_processed(store, msg):
            _save_profile_router_store(self._profile_router_store, store)
            return True
        _mark_store_processed(store, msg)

        binding = _route_binding_for_message(store, msg)
        if not binding:
            _save_profile_router_store(self._profile_router_store, store)
            return False
        if binding.profile == self._current_profile:
            _save_profile_router_store(self._profile_router_store, store)
            return False

        reply = await self._call_profile_gateway(binding.profile, msg)
        _save_profile_router_store(self._profile_router_store, store)
        if reply:
            await self.send(msg.peer_id, reply)
        return True

    async def _call_profile_gateway(self, profile: str, msg: NormalizedGeweMessage) -> str:
        if not self._http_client:
            raise RuntimeError("GeWe HTTP client is not connected")
        upstream = _profile_gateway_url(profile).rstrip("/")
        headers = {
            "Content-Type": "application/json",
            "X-Hermes-Session-Key": _profile_session_key(msg),
            "X-Hermes-GeWe-Profile": profile,
        }
        api_key = _profile_api_key(profile)
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        response = await self._http_client.post(
            f"{upstream}/v1/responses",
            headers=headers,
            json={
                "input": self._message_text(msg),
                "conversation": _profile_session_key(msg),
                "store": True,
                "metadata": _profile_message_metadata(msg),
            },
            timeout=300.0,
        )
        text = response.text
        response.raise_for_status()
        return _extract_output_text(json.loads(text) if text else {})

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
            file_url = attachment.url
            if not file_url and attachment.download_hint:
                file_url = await self._download_media_url(attachment.download_hint)
            if not file_url:
                continue
            try:
                path = await self._cache_url(file_url, attachment)
            except Exception:
                logger.warning("[GeWe] Failed to cache media url=%s", file_url, exc_info=True)
                continue
            media_urls.append(path)
            media_types.append(_media_type_for_attachment(attachment))
        return media_urls, media_types

    async def _download_media_url(self, hint: GeweDownloadHint) -> str:
        data = await self._api_post(f"/gewe/v2/api/message/{hint.endpoint}", hint.request_body)
        payload = data.get("data") if isinstance(data, dict) else None
        if isinstance(payload, dict):
            return str(payload.get("fileUrl") or payload.get("url") or "")
        return ""

    async def _cache_url(self, url: str, attachment: GeweAttachment) -> str:
        if not self._http_client:
            raise RuntimeError("GeWe HTTP client is not connected")
        response = await self._http_client.get(url)
        response.raise_for_status()
        data = response.content
        if attachment.kind == "image":
            return cache_image_from_bytes(data, _ext(attachment, ".jpg"))
        if attachment.kind == "voice":
            return cache_audio_from_bytes(data, _ext(attachment, ".amr"))
        if attachment.kind == "video":
            return cache_video_from_bytes(data, _ext(attachment, ".mp4"))
        return cache_document_from_bytes(data, attachment.file_name or f"gewe-file{_ext(attachment, '')}")

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
        attachment = _record_item_attachment(item, message_type, base.account_id)
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
        thumb_url=_str(source.attrib.get("cdnthumburl")),
        duration_seconds=_duration_seconds(source.attrib.get("playlength") or source.attrib.get("voicelength")),
    )
    return _with_download_hint(attachment, xml, app_id)


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
    attachment = GeweAttachment(
        kind=_attachment_kind(message_type),
        title=_text(item.find("datatitle")),
        description=_text(item.find("datadesc")),
        file_name=_text(item.find("datatitle")),
        file_ext=_text(item.find("fileext")) or _text(item.find("datafmt")),
        file_size=_int(_text(item.find("fullmd5size")) or _text(item.find("datasize"))),
        url=_text(item.find("dataurl")) or _text(item.find("streamdataurl")) or _text(item.find("cdndataurl")),
        thumb_url=_text(item.find("thumburl")) or _text(item.find("cdnthumburl")),
        md5=_text(item.find("fullmd5")) or _text(item.find("dataitemmd5")),
        aes_key=_text(item.find("dataurlkey")) or _text(item.find("cdndatakey")) or _text(item.find("cdnthumbkey")),
        cdn_file_id=_text(item.find("cdndataurl")) or _text(item.find("cdnthumburl")),
        duration_seconds=_int(_text(item.find("duration"))),
        raw=ET.tostring(item, encoding="unicode"),
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


def _with_download_hint(attachment: GeweAttachment, xml: str, app_id: str) -> GeweAttachment:
    endpoint = {"image": "downloadImage", "voice": "downloadVoice", "video": "downloadVideo", "file": "downloadFile"}.get(attachment.kind)
    if endpoint:
        body: Dict[str, Any] = {"appId": app_id, "xml": xml}
        if attachment.kind == "image":
            body["type"] = 2
        attachment.needs_download = True
        attachment.download_hint = GeweDownloadHint(endpoint, body)
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
    if attachment.kind == "image":
        return "[图片]"
    if attachment.kind == "voice":
        return "[语音]"
    if attachment.kind == "video":
        return "[视频]"
    if attachment.kind == "file":
        name = attachment.file_name or attachment.title or "文件"
        size = f" ({attachment.file_size} bytes)" if attachment.file_size else ""
        return f"[文件] {name}{size}"
    if attachment.kind == "link":
        return f"[链接] {attachment.title or attachment.url}"
    return f"[{attachment.kind}]"


def _media_type_for_attachment(attachment: GeweAttachment) -> str:
    return {"image": "image", "voice": "audio", "video": "video", "file": "document"}.get(attachment.kind, attachment.kind)


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


def _profile_root() -> Path:
    return get_default_hermes_root()


def _profile_dir(profile: str) -> Path:
    root = _profile_root()
    return root if not profile or profile == "default" else root / "profiles" / profile


def _current_profile_name() -> str:
    home = get_hermes_home().resolve()
    root = _profile_root().resolve()
    try:
        rel = home.relative_to(root / "profiles")
        return rel.parts[0] if rel.parts else "default"
    except ValueError:
        return "default"


def _load_profile_router_store(path: Path) -> Dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            data.setdefault("bindings", {})
            data.setdefault("processed", {})
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {"bindings": {}, "processed": {}}


def _save_profile_router_store(path: Path, store: Dict[str, Any]) -> None:
    _cleanup_profile_router_store(store)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(store, fh, ensure_ascii=False, indent=2)
            fh.write("\n")
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass


def _cleanup_profile_router_store(store: Dict[str, Any]) -> None:
    now = int(time.time())
    processed = store.get("processed") if isinstance(store.get("processed"), dict) else {}
    for key, seen_at in list(processed.items()):
        if now - int(seen_at or 0) > 600:
            processed.pop(key, None)
    store["processed"] = processed


def _store_processed_key(msg: NormalizedGeweMessage) -> str:
    return f"{msg.account_id}:{msg.provider_message_id}" if msg.account_id and msg.provider_message_id else ""


def _store_has_processed(store: Dict[str, Any], msg: NormalizedGeweMessage) -> bool:
    key = _store_processed_key(msg)
    processed = store.get("processed") if isinstance(store.get("processed"), dict) else {}
    return bool(key and processed.get(key))


def _mark_store_processed(store: Dict[str, Any], msg: NormalizedGeweMessage) -> None:
    key = _store_processed_key(msg)
    if not key:
        return
    processed = store.get("processed") if isinstance(store.get("processed"), dict) else {}
    processed[key] = int(time.time())
    store["processed"] = processed


def _binding_key(binding_type: str, identity: str) -> str:
    return f"{binding_type}:{identity}"


def _binding_from_raw(raw: Any, default_type: str, default_identity: str) -> Optional[GeweProfileBinding]:
    if not isinstance(raw, dict):
        return None
    profile = _str(raw.get("profile"))
    if not profile:
        return None
    binding_type = _str(raw.get("type") or default_type) or default_type
    identity = _str(raw.get("identity") or raw.get("user_id") or default_identity)
    if not identity:
        return None
    return GeweProfileBinding(
        type=binding_type,
        identity=identity,
        profile=profile,
        name=_str(raw.get("name") or raw.get("user_name")),
        listen_all=_as_bool(raw.get("listen_all"), False),
        source=_str(raw.get("source") or "manual"),
    )


def _binding_for_identity(store: Dict[str, Any], binding_type: str, identity: str) -> Optional[GeweProfileBinding]:
    bindings = store.get("bindings") if isinstance(store.get("bindings"), dict) else {}
    key = _binding_key(binding_type, identity)
    binding = _binding_from_raw(bindings.get(key), binding_type, identity)
    if binding:
        return binding
    if binding_type == "user":
        return _binding_from_raw(bindings.get(identity), binding_type, identity)
    return None


def _route_binding_for_message(store: Dict[str, Any], msg: NormalizedGeweMessage) -> Optional[GeweProfileBinding]:
    if msg.conversation_type == "direct":
        return _binding_for_identity(store, "user", msg.sender_id)

    for mentioned_wxid in sorted(msg.mentioned_user_ids):
        binding = _binding_for_identity(store, "user", mentioned_wxid)
        if binding:
            return binding

    group_binding = _binding_for_identity(store, "group", msg.peer_id)
    if group_binding and group_binding.listen_all:
        return group_binding
    return _binding_for_identity(store, "user", msg.sender_id)


def _profile_session_key(msg: NormalizedGeweMessage) -> str:
    if msg.conversation_type == "group":
        return f"gewe:group:{msg.peer_id}:{msg.sender_id}"
    return f"gewe:dm:{msg.sender_id}"


def _profile_message_metadata(msg: NormalizedGeweMessage) -> Dict[str, Any]:
    return {
        "platform": "gewe",
        "conversation_type": msg.conversation_type,
        "sender_wxid": msg.sender_id,
        "chat_wxid": msg.peer_id,
        "chatroom_id": msg.peer_id if msg.conversation_type == "group" else "",
        "mentioned_wxids": sorted(msg.mentioned_user_ids),
        "provider_message_id": msg.provider_message_id,
        "reply_to_message_id": msg.reply_to_message_id,
        "reply_to_text": msg.reply_to_text,
    }


def _profile_api_key(profile: str) -> str:
    try:
        raw = (_profile_dir(profile) / ".env").read_text(encoding="utf-8")
    except OSError:
        return ""
    for line in raw.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == "API_SERVER_KEY":
            return value.strip().strip('"').strip("'")
    return ""


def _profile_gateway_url(profile: str) -> str:
    cfg = _read_profile_config(profile)
    extra = ((cfg.get("platforms") or {}).get("api_server") or {}).get("extra") or {}
    host = str(extra.get("host") or "127.0.0.1")
    if host in {"0.0.0.0", "::", "[::]"}:
        host = "127.0.0.1"
    port = _int(extra.get("port")) or 8642
    return f"http://{_format_host_for_url(host)}:{port}"


def _read_profile_config(profile: str) -> Dict[str, Any]:
    path = _profile_dir(profile) / "config.yaml"
    try:
        import yaml

        with path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _format_host_for_url(host: str) -> str:
    if host.startswith("[") and host.endswith("]"):
        return host
    return f"[{host}]" if ":" in host else host


def _extract_output_text(value: Any) -> str:
    parts: List[str] = []

    def visit(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if not isinstance(node, dict):
            return
        node_type = str(node.get("type") or "")
        if isinstance(node.get("text"), str) and node_type in {"output_text", "text"}:
            parts.append(node["text"])
        if isinstance(node.get("output_text"), str):
            parts.append(node["output_text"])
        visit(node.get("content"))
        visit(node.get("output"))

    visit(value.get("output") if isinstance(value, dict) and "output" in value else value)
    return "".join(parts).strip()


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
    return {"image": "2", "voice": "3", "video": "4", "file": "5"}.get(kind, "5")


def _suffix_for_kind(kind: str) -> str:
    return {"image": "jpg", "voice": "amr", "video": "mp4"}.get(kind, "")


def _ext(attachment: GeweAttachment, default: str) -> str:
    ext = attachment.file_ext.strip().lstrip(".") if attachment.file_ext else ""
    return f".{ext}" if ext else default


def _text(node: Optional[ET.Element]) -> str:
    return "" if node is None or node.text is None else str(node.text).strip()


def _child_text(node: Optional[ET.Element], name: str) -> str:
    return _text(node.find(name)) if node is not None else ""


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


def _split_csv(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    return {part.strip() for part in str(value or "").split(",") if part.strip()}


def _safe_id(value: str, keep: int = 8) -> str:
    return value if len(value) <= keep else f"{value[:keep]}..."
