"""Tests for the native GeWe v2 callback adapter."""

from gateway.config import PlatformConfig
from gateway.platforms.gewe import GeweAdapter, _gewe_ok, normalize_gewe_callback


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
    assert attachment.download_hint is not None
    assert attachment.download_hint.endpoint == "downloadVoice"
