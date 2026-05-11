"""Tests for the native GeWe v2 callback adapter."""

import asyncio
from html import escape
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource
from gateway.platforms.gewe import (
    GeweAdapter,
    GeweDownloadHint,
    _gewe_ok,
    _media_type_for_attachment,
    _to_hermes_type,
    normalize_gewe_callback,
)


def _gewe_payload(**overrides):
    payload = {
        "appid": "wx_app",
        "wxid": "wxid_bot",
        "toUser": "wxid_bot",
        "fromUser": "wxid_sender",
        "content": "hello",
        "eventCode": "private_msg_event",
        "msgType": "TEXT",
        "newMsgId": 123,
        "isSelf": False,
    }
    payload.update(overrides)
    return payload


def _adapter(**extra):
    return GeweAdapter(
        PlatformConfig(
            enabled=True,
            token="token",
            extra={"app_id": "wx_app", "bot_wxid": "wxid_bot", **extra},
        )
    )




@pytest.mark.asyncio
async def test_media_followup_debounce_merges_text_into_single_photo_event():
    adapter = _adapter(media_followup_grace_seconds=0.05)
    adapter.handle_message = AsyncMock()
    source = SessionSource(
        platform=Platform.GEWE,
        chat_id="wxid_sender",
        chat_type="dm",
        user_id="wxid_sender",
        user_name="wxid_sender",
    )
    media_event = MessageEvent(
        text="[表情]",
        message_type=MessageType.PHOTO,
        source=source,
        message_id="m1",
        media_urls=["/tmp/emoji.gif"],
        media_types=["image/gif"],
    )
    text_event = MessageEvent(
        text="看看这个",
        message_type=MessageType.TEXT,
        source=source,
        message_id="m2",
    )

    await adapter._dispatch_event_with_media_debounce(media_event)
    adapter.handle_message.assert_not_called()

    await adapter._dispatch_event_with_media_debounce(text_event)
    await asyncio.sleep(0.08)

    adapter.handle_message.assert_awaited_once()
    dispatched = adapter.handle_message.await_args.args[0]
    assert dispatched is media_event
    assert dispatched.text == "看看这个"
    assert dispatched.message_type == MessageType.PHOTO
    assert dispatched.media_urls == ["/tmp/emoji.gif"]
    assert dispatched.media_types == ["image/gif"]
    assert dispatched.message_id == "m2"


@pytest.mark.asyncio
async def test_media_followup_debounce_flushes_media_without_followup():
    adapter = _adapter(media_followup_grace_seconds=0.01)
    adapter.handle_message = AsyncMock()
    source = SessionSource(
        platform=Platform.GEWE,
        chat_id="wxid_sender",
        chat_type="dm",
        user_id="wxid_sender",
        user_name="wxid_sender",
    )
    media_event = MessageEvent(
        text="[图片]",
        message_type=MessageType.PHOTO,
        source=source,
        message_id="m1",
        media_urls=["/tmp/image.jpg"],
        media_types=["image/jpeg"],
    )

    await adapter._dispatch_event_with_media_debounce(media_event)
    adapter.handle_message.assert_not_called()

    await asyncio.sleep(0.05)

    adapter.handle_message.assert_awaited_once_with(media_event)


def test_direct_mode_acquires_distinct_gewe_app_and_token_locks(monkeypatch):
    acquired = []
    released = []

    def acquire(scope, identity, metadata=None):
        acquired.append((scope, identity, metadata))
        return True, None

    monkeypatch.setattr("gateway.status.acquire_scoped_lock", acquire)
    monkeypatch.setattr("gateway.status.release_scoped_lock", lambda scope, identity: released.append((scope, identity)))

    adapter = _adapter()

    assert adapter._acquire_gewe_locks() is True
    assert [(scope, identity) for scope, identity, _ in acquired] == [
        ("gewe-app-id", "wx_app"),
        ("gewe-token", "token"),
    ]

    adapter._release_gewe_locks()
    assert released == [("gewe-token", "token"), ("gewe-app-id", "wx_app")]


def test_relay_mode_acquires_distinct_gewe_and_webhook_router_locks(monkeypatch):
    acquired = []

    def acquire(scope, identity, metadata=None):
        acquired.append((scope, identity))
        return True, None

    monkeypatch.setattr("gateway.status.acquire_scoped_lock", acquire)
    monkeypatch.setattr("gateway.status.release_scoped_lock", lambda scope, identity: None)

    adapter = _adapter(inbound_mode="relay-sse", relay_app_id="router-app", relay_app_token="router-token")

    assert adapter._acquire_gewe_locks() is True
    assert acquired == [
        ("gewe-app-id", "wx_app"),
        ("gewe-token", "token"),
        ("gewe-relay-app-id", "router-app"),
        ("gewe-relay-app-token", "router-token"),
    ]
    adapter._release_gewe_locks()


def test_lock_failure_releases_previously_acquired_gewe_locks(monkeypatch):
    released = []

    def acquire(scope, identity, metadata=None):
        if scope == "gewe-relay-app-id":
            return False, {"pid": 42}
        return True, None

    monkeypatch.setattr("gateway.status.acquire_scoped_lock", acquire)
    monkeypatch.setattr("gateway.status.release_scoped_lock", lambda scope, identity: released.append((scope, identity)))

    adapter = _adapter(inbound_mode="relay-sse", relay_app_id="router-app", relay_app_token="router-token")

    assert adapter._acquire_gewe_locks() is False
    assert released == [("gewe-token", "token"), ("gewe-app-id", "wx_app")]


def test_gewe_send_success_accepts_ret_200_operation_success():
    assert _gewe_ok({"ret": 200, "msg": "操作成功", "data": {"msgId": 123}}) is True


@pytest.mark.asyncio
async def test_send_image_uses_gewe_img_url_field():
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": 456}})

    result = await adapter.send_image("wxid_friend", "https://cdn.example.com/pic.jpg")

    assert result.success is True
    adapter._api_post.assert_awaited_once_with(
        "/gewe/v2/api/message/postImage",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "imgUrl": "https://cdn.example.com/pic.jpg",
        },
    )


@pytest.mark.asyncio
async def test_upload_local_file_posts_multipart_to_webhook_router(tmp_path):
    source = tmp_path / "example.jpg"
    source.write_bytes(b"jpeg-data")
    adapter = _adapter(relay_base_url="https://hook.yunzxu.com", relay_app_id="macmini-hremes", relay_app_token="secret-token")

    class FakeResponse:
        text = '{"ok": true, "path": "/files/01j/example.jpg", "size": 9, "filename": "example.jpg"}'

        def raise_for_status(self):
            pass

    captured = {}

    async def fake_post(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        uploaded = kwargs["files"]["file"]
        captured["filename"] = uploaded[0]
        captured["mime"] = uploaded[2]
        captured["content"] = uploaded[1].read()
        return FakeResponse()

    adapter._http_client = SimpleNamespace(post=fake_post)

    public_url = await adapter._upload_local_file(str(source))

    assert public_url == "https://hook.yunzxu.com/files/01j/example.jpg"
    assert captured["url"] == "https://hook.yunzxu.com/apps/macmini-hremes/files?token=secret-token"
    assert captured["filename"] == "example.jpg"
    assert captured["mime"] == "image/jpeg"
    assert captured["content"] == b"jpeg-data"


@pytest.mark.asyncio
async def test_send_image_file_uploads_local_file_then_posts_img_url(tmp_path):
    source = tmp_path / "photo.jpg"
    source.write_bytes(b"jpeg-data")
    adapter = _adapter(relay_base_url="https://hook.yunzxu.com", relay_app_id="app", relay_app_token="token")
    adapter._upload_local_file = AsyncMock(return_value="https://hook.yunzxu.com/files/01j/photo.jpg")
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": 459}})

    result = await adapter.send_image_file("wxid_friend", str(source))

    assert result.success is True
    adapter._upload_local_file.assert_awaited_once_with(str(source))
    adapter._api_post.assert_awaited_once_with(
        "/gewe/v2/api/message/postImage",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "imgUrl": "https://hook.yunzxu.com/files/01j/photo.jpg",
        },
    )


@pytest.mark.asyncio
async def test_send_document_uploads_local_file_then_posts_file_url(tmp_path):
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"xlsx-data")
    adapter = _adapter(relay_base_url="https://hook.yunzxu.com", relay_app_id="app", relay_app_token="token")
    adapter._upload_local_file = AsyncMock(return_value="https://hook.yunzxu.com/files/01j/report.xlsx")
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": 460}})

    result = await adapter.send_document("wxid_friend", str(source))

    assert result.success is True
    adapter._upload_local_file.assert_awaited_once_with(str(source))
    adapter._api_post.assert_awaited_once_with(
        "/gewe/v2/api/message/postFile",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "fileUrl": "https://hook.yunzxu.com/files/01j/report.xlsx",
            "fileName": "report.xlsx",
        },
    )


@pytest.mark.asyncio
async def test_send_voice_uploads_local_silk_then_posts_voice_url(tmp_path):
    source = tmp_path / "voice.silk"
    source.write_bytes(b"silk-data")
    adapter = _adapter(relay_base_url="https://hook.yunzxu.com", relay_app_id="app", relay_app_token="token")
    adapter._upload_local_file = AsyncMock(return_value="https://hook.yunzxu.com/files/01j/voice.silk")
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": 461}})

    result = await adapter.send_voice("wxid_friend", str(source), metadata={"voice_duration_ms": 1200})

    assert result.success is True
    adapter._upload_local_file.assert_awaited_once_with(str(source))
    adapter._api_post.assert_awaited_once_with(
        "/gewe/v2/api/message/postVoice",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "voiceUrl": "https://hook.yunzxu.com/files/01j/voice.silk",
            "voiceDuration": 1200,
        },
    )


@pytest.mark.asyncio
async def test_upload_local_file_requires_relay_credentials(tmp_path):
    source = tmp_path / "example.jpg"
    source.write_bytes(b"jpeg-data")
    adapter = _adapter(relay_base_url="https://hook.yunzxu.com")
    adapter._http_client = SimpleNamespace()

    with pytest.raises(RuntimeError, match="GEWE_RELAY_BASE_URL"):
        await adapter._upload_local_file(str(source))


@pytest.mark.asyncio
async def test_send_document_uses_gewe_post_file_fields():
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": 457}})

    result = await adapter.send_document(
        "wxid_friend",
        "https://cdn.example.com/pkg/a909.xls?q-signature=abc",
    )

    assert result.success is True
    adapter._api_post.assert_awaited_once_with(
        "/gewe/v2/api/message/postFile",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "fileUrl": "https://cdn.example.com/pkg/a909.xls?q-signature=abc",
            "fileName": "a909.xls",
        },
    )


@pytest.mark.asyncio
async def test_send_document_honors_explicit_file_name():
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": 458}})

    result = await adapter.send_document(
        "wxid_friend",
        "https://cdn.example.com/download?id=123",
        file_name="report.xlsx",
    )

    assert result.success is True
    adapter._api_post.assert_awaited_once_with(
        "/gewe/v2/api/message/postFile",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "fileUrl": "https://cdn.example.com/download?id=123",
            "fileName": "report.xlsx",
        },
    )


@pytest.mark.asyncio
async def test_send_document_local_path_reports_upload_configuration_error(tmp_path):
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"xlsx-data")
    adapter = _adapter(relay_base_url="https://hook.yunzxu.com")
    adapter._http_client = SimpleNamespace()

    result = await adapter.send_document("wxid_friend", str(source))

    assert result.success is False
    assert "GEWE_RELAY_BASE_URL" in result.error


@pytest.mark.asyncio
async def test_delete_message_uses_cached_gewe_revoke_payload():
    adapter = _adapter()
    adapter._api_post = AsyncMock(
        side_effect=[
            {
                "ret": 200,
                "msg": "操作成功",
                "data": {
                    "toWxid": "wxid_friend",
                    "createTime": 1704163145,
                    "msgId": 769533801,
                    "newMsgId": 5271007655758710001,
                },
            },
            {"ret": 200, "msg": "操作成功"},
        ]
    )

    send_result = await adapter.send("wxid_friend", "hello")
    deleted = await adapter.delete_message("wxid_friend", send_result.message_id)

    assert deleted is True
    assert adapter._api_post.await_args_list[1].args == (
        "/gewe/v2/api/message/revokeMsg",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "msgId": "769533801",
            "newMsgId": "5271007655758710001",
            "createTime": "1704163145",
        },
    )
    assert adapter._sent_message_revoke_payloads == {}


@pytest.mark.asyncio
async def test_delete_message_accepts_new_msg_id_alias():
    adapter = _adapter()
    adapter._api_post = AsyncMock(
        side_effect=[
            {
                "ret": 200,
                "msg": "操作成功",
                "data": {
                    "toWxid": "wxid_friend",
                    "createTime": "1704163145",
                    "msgId": "769533801",
                    "newMsgId": "5271007655758710001",
                },
            },
            {"ret": 200, "msg": "操作成功"},
        ]
    )

    await adapter.send("wxid_friend", "hello")
    deleted = await adapter.delete_message("wxid_friend", "5271007655758710001")

    assert deleted is True
    assert adapter._api_post.await_args_list[1].args[0] == "/gewe/v2/api/message/revokeMsg"


@pytest.mark.asyncio
async def test_delete_message_without_cached_revoke_payload_returns_false():
    adapter = _adapter()
    adapter._api_post = AsyncMock()

    deleted = await adapter.delete_message("wxid_friend", "missing")

    assert deleted is False
    adapter._api_post.assert_not_called()


@pytest.mark.asyncio
async def test_delete_message_rejects_chat_mismatch():
    adapter = _adapter()
    adapter._api_post = AsyncMock(
        return_value={
            "ret": 200,
            "msg": "操作成功",
            "data": {
                "toWxid": "wxid_friend",
                "createTime": "1704163145",
                "msgId": "769533801",
                "newMsgId": "5271007655758710001",
            },
        }
    )

    await adapter.send("wxid_friend", "hello")
    deleted = await adapter.delete_message("other_chat", "769533801")

    assert deleted is False
    assert adapter._api_post.await_count == 1


@pytest.mark.asyncio
async def test_incomplete_send_response_is_not_revokeable():
    adapter = _adapter()
    adapter._api_post = AsyncMock(
        return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": "769533801"}}
    )

    await adapter.send("wxid_friend", "hello")

    assert adapter._sent_message_revoke_payloads == {}


@pytest.mark.asyncio
async def test_send_voice_requires_http_silk_url_and_posts_voice_duration():
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"msgId": 789}})

    result = await adapter.send_voice(
        "wxid_friend",
        "https://cdn.example.com/voice.silk",
        metadata={"voice_duration_ms": "2000"},
    )

    assert result.success is True
    adapter._api_post.assert_awaited_once_with(
        "/gewe/v2/api/message/postVoice",
        {
            "appId": "wx_app",
            "toWxid": "wxid_friend",
            "voiceUrl": "https://cdn.example.com/voice.silk",
            "voiceDuration": 2000,
        },
    )


@pytest.mark.asyncio
async def test_send_voice_rejects_non_silk_url():
    adapter = _adapter()

    result = await adapter.send_voice("wxid_friend", "https://cdn.example.com/voice.mp3")

    assert result.success is False
    assert ".silk" in result.error


def test_v2_private_text_normalizes_sender_and_peer():
    msg = normalize_gewe_callback(_gewe_payload(content="你好"))

    assert msg is not None
    assert msg.conversation_type == "direct"
    assert msg.peer_id == "wxid_sender"
    assert msg.sender_id == "wxid_sender"
    assert msg.account_id == "wxid_bot"
    assert msg.text == "你好"


def test_v2_group_text_uses_from_group_as_chat_and_from_user_as_sender():
    msg = normalize_gewe_callback(
        _gewe_payload(
            eventCode="group_msg_event",
            fromGroup="25500496398@chatroom",
            fromUser="wxid_sender_a",
            content="测试1",
        )
    )

    assert msg is not None
    assert msg.conversation_type == "group"
    assert msg.peer_id == "25500496398@chatroom"
    assert msg.sender_id == "wxid_sender_a"


def test_group_require_mention_matches_only_bot_wxid_from_at_list():
    adapter = _adapter(group_require_mention=True)
    msg = normalize_gewe_callback(
        _gewe_payload(
            eventCode="group_msg_event",
            fromGroup="25500496398@chatroom",
            atUserList="wxid_bot,wxid_other",
        )
    )

    assert msg is not None
    assert adapter._should_process_group(msg) is True


def test_group_require_mention_matches_bot_wxid_from_msg_source():
    adapter = _adapter(group_require_mention=True)
    msg = normalize_gewe_callback(
        _gewe_payload(
            eventCode="group_msg_event",
            fromGroup="25500496398@chatroom",
            msgSource="<msgsource><atuserlist>wxid_bot</atuserlist></msgsource>",
        )
    )

    assert msg is not None
    assert msg.mentioned_user_ids == {"wxid_bot"}
    assert adapter._should_process_group(msg) is True


def test_group_require_mention_rejects_display_text_without_at_wxid():
    adapter = _adapter(group_require_mention=True)
    msg = normalize_gewe_callback(
        _gewe_payload(
            eventCode="group_msg_event",
            fromGroup="25500496398@chatroom",
            content="@陳可乐 你好",
        )
    )

    assert msg is not None
    assert adapter._should_process_group(msg) is False


def test_quote_message_populates_reply_context():
    quote_xml = """<?xml version="1.0"?>
    <msg><appmsg><title>我测试下引用消息</title><type>57</type><refermsg>
      <type>1</type><svrid>7810092927857443194</svrid><chatusr>wxid_sender</chatusr>
      <displayname>陈可乐</displayname><content>发个消息</content>
    </refermsg></appmsg></msg>
    """
    msg = normalize_gewe_callback(_gewe_payload(msgType="QUOTE", content=quote_xml))

    assert msg is not None
    assert msg.message_type == "quote"
    assert msg.text == "我测试下引用消息"
    assert msg.reply_to_message_id == "7810092927857443194"
    assert msg.reply_to_text == "陈可乐: 发个消息"


def _chat_record_xml(*items):
    record = "<recordinfo>" + "".join(items) + "</recordinfo>"
    return (
        "<msg><appmsg><type>19</type><title>聊天记录</title>"
        f"<recorditem>{escape(record)}</recorditem></appmsg></msg>"
    )


def _record_dataitem(datatype, **fields):
    body = "".join(f"<{name}>{escape(str(value))}</{name}>" for name, value in fields.items())
    return f'<dataitem datatype="{datatype}">{body}</dataitem>'


def test_chat_record_file_item_uses_device_app_id_for_download_hint():
    record_xml = _chat_record_xml(
        _record_dataitem(
            6,
            sourcename="陈可乐",
            datatitle="报价.xlsx",
            datafmt="xlsx",
            datasize="1234",
            cdndataurl="cdn-file-id",
            cdndatakey="cdn-aes",
        )
    )

    msg = normalize_gewe_callback(_gewe_payload(msgType="APP_MSG", content=record_xml))

    assert msg is not None
    assert msg.message_type == "chat_record"
    assert len(msg.items) == 1
    attachment = msg.items[0].attachments[0]
    assert attachment.kind == "file"
    assert attachment.file_name == "报价.xlsx"
    assert attachment.download_hint is not None
    assert attachment.download_hint.request_body["appId"] == "wx_app"
    assert attachment.download_hint.request_body["fileId"] == "cdn-file-id"


@pytest.mark.asyncio
async def test_chat_record_cached_file_path_is_injected_into_record_text():
    record_xml = _chat_record_xml(
        _record_dataitem(
            6,
            sourcename="陈可乐",
            datatitle="报价.xlsx",
            datafmt="xlsx",
            datasize="1234",
            cdndataurl="cdn-file-id",
            cdndatakey="cdn-aes",
        )
    )
    msg = normalize_gewe_callback(_gewe_payload(msgType="APP_MSG", content=record_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/报价.xlsx"}})
    adapter._cache_url = AsyncMock(return_value="/tmp/hermes/cache/documents/doc_abc_报价.xlsx")

    media_urls, media_types = await adapter._cache_media(msg)
    text = adapter._message_text(msg)

    assert media_urls == ["/tmp/hermes/cache/documents/doc_abc_报价.xlsx"]
    assert media_types == ["application/octet-stream"]
    assert "[聊天记录] 1 条" in text
    assert "陈可乐" in text
    assert "[文件] 报价.xlsx (1234 bytes)" in text
    assert "本地路径: /tmp/hermes/cache/documents/doc_abc_报价.xlsx" in text



def test_chat_record_image_item_builds_download_image_hint_from_cdn_fields():
    record_xml = _chat_record_xml(
        _record_dataitem(
            2,
            sourcename="陈可乐",
            datadesc="[图片]",
            cdndataurl="full-cdn-file-id",
            cdndatakey="full-aes",
            cdnthumburl="thumb-cdn-file-id",
            cdnthumbaeskey="thumb-aes",
            thumbfullsize="4567",
        )
    )

    msg = normalize_gewe_callback(_gewe_payload(msgType="APP_MSG", content=record_xml))

    assert msg is not None
    assert msg.message_type == "chat_record"
    assert len(msg.items) == 1
    attachment = msg.items[0].attachments[0]
    assert attachment.kind == "image"
    assert attachment.file_ext == "jpg"
    assert attachment.download_hint is not None
    assert attachment.download_hint.endpoint == "downloadImage"
    assert attachment.download_hint.request_body["appId"] == "wx_app"
    assert attachment.download_hint.request_body["type"] == 2
    hint_xml = attachment.download_hint.request_body["xml"]
    assert "<img" in hint_xml
    assert 'cdnmidimgurl="full-cdn-file-id"' in hint_xml
    assert 'cdnthumburl="thumb-cdn-file-id"' in hint_xml
    assert 'cdnthumbaeskey="thumb-aes"' in hint_xml
    fallback_bodies = [hint.request_body for hint in attachment.download_hint.fallbacks]
    assert {body["type"] for body in fallback_bodies if "xml" in body} >= {1, 3}
    assert {body["type"] for body in fallback_bodies if body.get("fileId") == "full-cdn-file-id"} == {"1", "2", "3"}
    assert {body["type"] for body in fallback_bodies if body.get("fileId") == "thumb-cdn-file-id"} == {"1", "2", "3"}


@pytest.mark.asyncio
async def test_download_media_url_tries_fallbacks_and_top_level_url():
    adapter = _adapter()
    adapter._api_post = AsyncMock(side_effect=[
        {"ret": 200, "msg": "操作成功", "data": {}},
        {"ret": 200, "msg": "操作成功", "fileUrl": "https://cdn.example.com/fallback.jpg"},
    ])
    hint = GeweDownloadHint(
        "downloadImage",
        {"appId": "wx_app", "xml": "<msg><img aeskey=\"x\" /></msg>", "type": 2},
        fallbacks=[GeweDownloadHint("downloadCdn", {"appId": "wx_app", "fileId": "file", "aesKey": "key"})],
    )

    assert await adapter._download_media_url(hint) == "https://cdn.example.com/fallback.jpg"
    assert adapter._api_post.await_count == 2
    assert adapter._api_post.await_args_list[0].args[0].endswith("/downloadImage")
    assert adapter._api_post.await_args_list[1].args[0].endswith("/downloadCdn")


@pytest.mark.asyncio
async def test_chat_record_image_cache_uses_download_fallback_url():
    record_xml = _chat_record_xml(
        _record_dataitem(
            2,
            sourcename="陈可乐",
            datadesc="[图片]",
            cdndataurl="image-cdn-file-id",
            cdndatakey="image-aes",
            fullmd5size="1234",
        )
    )
    msg = normalize_gewe_callback(_gewe_payload(msgType="APP_MSG", content=record_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(side_effect=[
        {"ret": 200, "msg": "操作成功", "data": {}},
        {"ret": 200, "msg": "操作成功", "data": {}},
        {"ret": 200, "msg": "操作成功", "data": {}},
        {"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/image.jpg"}},
    ])
    adapter._cache_url = AsyncMock(return_value="/tmp/hermes/cache/images/img_abc.jpg")

    media_urls, media_types = await adapter._cache_media(msg)

    assert media_urls == ["/tmp/hermes/cache/images/img_abc.jpg"]
    assert media_types == ["image/jpeg"]
    adapter._cache_url.assert_awaited_once_with("https://cdn.example.com/image.jpg", msg.items[0].attachments[0])


@pytest.mark.asyncio
async def test_chat_record_image_cache_continues_after_bad_download_url():
    record_xml = _chat_record_xml(
        _record_dataitem(
            2,
            sourcename="陈可乐",
            datadesc="[图片]",
            cdndataurl="image-cdn-file-id",
            cdndatakey="image-aes",
            fullmd5size="1234",
        )
    )
    msg = normalize_gewe_callback(_gewe_payload(msgType="APP_MSG", content=record_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(side_effect=[
        {"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/bad.jpg"}},
        {"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/good.jpg"}},
    ])
    adapter._cache_url = AsyncMock(side_effect=[
        ValueError("not an image"),
        "/tmp/hermes/cache/images/img_good.jpg",
    ])

    media_urls, media_types = await adapter._cache_media(msg)

    assert media_urls == ["/tmp/hermes/cache/images/img_good.jpg"]
    assert media_types == ["image/jpeg"]
    assert adapter._cache_url.await_args_list[0].args[0] == "https://cdn.example.com/bad.jpg"
    assert adapter._cache_url.await_args_list[1].args[0] == "https://cdn.example.com/good.jpg"


@pytest.mark.asyncio
async def test_chat_record_cached_image_path_is_injected_into_record_text():
    record_xml = _chat_record_xml(
        _record_dataitem(
            2,
            sourcename="陈可乐",
            datadesc="[图片]",
            cdndataurl="image-cdn-file-id",
            cdndatakey="image-aes",
            fullmd5size="1234",
        )
    )
    msg = normalize_gewe_callback(_gewe_payload(msgType="APP_MSG", content=record_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/image.jpg"}})
    adapter._cache_url = AsyncMock(return_value="/tmp/hermes/cache/images/img_abc.jpg")

    attachment = msg.items[0].attachments[0]
    assert attachment.download_hint.endpoint == "downloadImage"

    media_urls, media_types = await adapter._cache_media(msg)
    text = adapter._message_text(msg)

    assert media_urls == ["/tmp/hermes/cache/images/img_abc.jpg"]
    assert media_types == ["image/jpeg"]
    assert "[聊天记录] 1 条" in text
    assert "陈可乐" in text
    assert "[图片] 本地路径: /tmp/hermes/cache/images/img_abc.jpg" in text


@pytest.mark.asyncio
async def test_cache_media_skips_non_http_cdn_id_when_download_returns_no_url():
    record_xml = _chat_record_xml(
        _record_dataitem(
            2,
            sourcename="陈可乐",
            datadesc="[图片]",
            cdndataurl="image-cdn-file-id",
            cdndatakey="image-aes",
        )
    )
    msg = normalize_gewe_callback(_gewe_payload(msgType="APP_MSG", content=record_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 500, "msg": "下载图片失败"})
    adapter._cache_url = AsyncMock()

    media_urls, media_types = await adapter._cache_media(msg)

    assert media_urls == []
    assert media_types == []
    adapter._cache_url.assert_not_called()


def test_emoji_message_builds_image_attachment_from_emoji_xml():
    emoji_xml = """<msg><emoji md5="emoji-md5" len="2048"
      cdnurl="https://emoji.example.com/e.webp" thumburl="https://emoji.example.com/t.png"
      aeskey="emoji-aes" /></msg>"""
    msg = normalize_gewe_callback(_gewe_payload(msgType="EMOJI", content=emoji_xml))

    assert msg is not None
    assert msg.message_type == "emoji"
    assert len(msg.attachments) == 1
    attachment = msg.attachments[0]
    assert attachment.kind == "emoji"
    assert attachment.url == "https://emoji.example.com/e.webp"
    assert attachment.thumb_url == "https://emoji.example.com/t.png"
    assert attachment.md5 == "emoji-md5"
    assert attachment.file_size == 2048
    assert attachment.file_ext == "webp"
    assert attachment.download_hint is not None
    assert attachment.download_hint.endpoint == "downloadCdn"
    assert attachment.download_hint.request_body["type"] == "2"
    assert _to_hermes_type(msg.message_type) == MessageType.PHOTO
    assert _media_type_for_attachment(attachment).startswith("image/")


def test_emoji_summary_is_human_readable():
    msg = normalize_gewe_callback(_gewe_payload(msgType="EMOJI", content='<msg><emoji md5="x" /></msg>'))
    adapter = _adapter()

    assert msg is not None
    assert adapter._message_text(msg) == "[表情]"


def test_voice_message_builds_download_hint_from_voiceurl():
    voice_xml = """<msg><voicemsg voicelength="1039" length="1267"
      aeskey="voice-aes" voiceurl="voice-file-id" fromusername="wxid_sender" /></msg>"""
    msg = normalize_gewe_callback(_gewe_payload(msgType="VOICE", content=voice_xml))

    assert msg is not None
    assert msg.message_type == "voice"
    assert len(msg.attachments) == 1
    attachment = msg.attachments[0]
    assert attachment.kind == "voice"
    assert attachment.cdn_file_id == "voice-file-id"
    assert attachment.duration_seconds == 1
    assert attachment.file_ext == "silk"
    assert attachment.download_hint is not None
    assert attachment.download_hint.endpoint == "downloadVoice"
    assert any(hint.endpoint == "downloadCdn" for hint in attachment.download_hint.fallbacks)


@pytest.mark.asyncio
async def test_voice_message_download_caches_silk_for_stt_path():
    voice_xml = """<msg><voicemsg voicelength="1039" length="1267"
      aeskey="voice-aes" voiceurl="voice-file-id" fromusername="wxid_sender" /></msg>"""
    msg = normalize_gewe_callback(_gewe_payload(msgType="VOICE", content=voice_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/voice.silk"}})
    adapter._cache_url = AsyncMock(return_value="/tmp/hermes/cache/audio/audio_abc.silk")

    media_urls, media_types = await adapter._cache_media(msg)
    text = adapter._message_text(msg)

    assert media_urls == ["/tmp/hermes/cache/audio/audio_abc.silk"]
    assert media_types == ["audio/silk"]
    assert "[语音] 本地路径: /tmp/hermes/cache/audio/audio_abc.silk" in text


@pytest.mark.asyncio
async def test_voice_message_download_uses_cdn_fallback_after_download_voice_failure():
    voice_xml = """<msg><voicemsg voicelength="1039" length="1267"
      aeskey="voice-aes" voiceurl="voice-file-id" fromusername="wxid_sender" /></msg>"""
    msg = normalize_gewe_callback(_gewe_payload(msgType="VOICE", content=voice_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(side_effect=[
        {"ret": 500, "msg": "语音下载失败", "data": {"code": "500"}},
        {"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/voice.silk"}},
    ])
    adapter._cache_url = AsyncMock(return_value="/tmp/hermes/cache/audio/audio_abc.silk")

    media_urls, media_types = await adapter._cache_media(msg)

    assert media_urls == ["/tmp/hermes/cache/audio/audio_abc.silk"]
    assert media_types == ["audio/silk"]
    assert adapter._api_post.await_args_list[0].args[0].endswith("/downloadVoice")
    assert adapter._api_post.await_args_list[1].args[0].endswith("/downloadCdn")
    assert adapter._api_post.await_args_list[1].args[1]["type"] == "5"


@pytest.mark.asyncio
async def test_voice_message_download_continues_across_cdn_suffix_fallbacks():
    voice_xml = """<msg><voicemsg voicelength="1039" length="1267"
      aeskey="voice-aes" voiceurl="voice-file-id" fromusername="wxid_sender" /></msg>"""
    msg = normalize_gewe_callback(_gewe_payload(msgType="VOICE", content=voice_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(side_effect=[
        {"ret": 500, "msg": "语音下载失败", "data": {"code": "500"}},
        {"ret": 500, "msg": "cdn下载失败"},
        {"ret": 500, "msg": "cdn下载失败"},
        {"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/voice.amr"}},
    ])
    adapter._cache_url = AsyncMock(return_value="/tmp/hermes/cache/audio/audio_abc.mp3")

    media_urls, media_types = await adapter._cache_media(msg)

    assert media_urls == ["/tmp/hermes/cache/audio/audio_abc.mp3"]
    assert media_types == ["audio/mpeg"]
    requests = [call.args[1] for call in adapter._api_post.await_args_list[1:]]
    assert requests[0]["type"] == "5"
    assert requests[0]["suffix"] == "silk"
    assert requests[0]["totalSize"] == "1267"
    assert requests[1]["suffix"] == "silk"
    assert requests[1]["totalSize"] == ""
    assert requests[2]["suffix"] == "amr"


@pytest.mark.asyncio
async def test_voice_message_cache_reports_mp3_after_silk_conversion():
    voice_xml = """<msg><voicemsg voicelength="1039" length="1267"
      aeskey="voice-aes" voiceurl="voice-file-id" fromusername="wxid_sender" /></msg>"""
    msg = normalize_gewe_callback(_gewe_payload(msgType="VOICE", content=voice_xml))
    adapter = _adapter()
    adapter._api_post = AsyncMock(return_value={"ret": 200, "msg": "操作成功", "data": {"fileUrl": "https://cdn.example.com/voice.silk"}})
    adapter._cache_url = AsyncMock(return_value="/tmp/hermes/cache/audio/audio_abc.mp3")

    media_urls, media_types = await adapter._cache_media(msg)

    assert media_urls == ["/tmp/hermes/cache/audio/audio_abc.mp3"]
    assert media_types == ["audio/mpeg"]


def test_gewe_silk_voice_conversion_uses_decoder_and_ffmpeg(tmp_path):
    from gateway.platforms import gewe

    silk = tmp_path / "voice.silk"
    silk.write_bytes(b"#!SILK_V3\x00payload")

    def fake_run(cmd, **kwargs):
        output = Path(cmd[-1])
        output.write_bytes(b"mp3" if output.suffix == ".mp3" else b"pcm")
        return SimpleNamespace(returncode=0)

    with patch("gateway.platforms.gewe._silk_decoder_command", return_value=["decoder"]), \
         patch("gateway.platforms.gewe.shutil.which", return_value="ffmpeg"), \
         patch("gateway.platforms.gewe.subprocess.run", side_effect=fake_run):
        converted = gewe._convert_silk_to_mp3(str(silk))

    assert converted.endswith(".mp3")
    assert Path(converted).read_bytes() == b"mp3"
