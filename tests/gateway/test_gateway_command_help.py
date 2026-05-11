"""Gateway command help rendering tests."""

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource


def _make_event(text: str, platform: Platform) -> MessageEvent:
    return MessageEvent(
        text=text,
        source=SessionSource(
            platform=platform,
            chat_id="chat-1",
            user_id="user-1",
            user_name="tester",
            chat_type="dm",
        ),
    )


def _make_runner():
    from gateway.run import GatewayRunner

    return object.__new__(GatewayRunner)


@pytest.mark.asyncio
async def test_help_sanitizes_slash_command_mentions_for_telegram(monkeypatch):
    """Telegram help output must not expose invalid uppercase/hyphenated slashes."""
    monkeypatch.setattr(
        "agent.skill_commands.get_skill_commands",
        lambda: {
            "/Linear": {"description": "Open Linear"},
            "/Custom-Thing": {"description": "Run a custom thing"},
        },
    )

    result = await _make_runner()._handle_help_command(
        _make_event("/help", Platform.TELEGRAM)
    )

    assert "`/linear`" in result
    assert "`/custom_thing`" in result
    assert "`/Linear`" not in result
    assert "`/Custom-Thing`" not in result


@pytest.mark.asyncio
async def test_commands_sanitizes_slash_command_mentions_for_telegram(monkeypatch):
    """Paginated Telegram /commands output uses Telegram-valid slash mentions."""
    monkeypatch.setattr(
        "agent.skill_commands.get_skill_commands",
        lambda: {"/Linear": {"description": "Open Linear"}},
    )

    result = await _make_runner()._handle_commands_command(
        _make_event("/commands 999", Platform.TELEGRAM)
    )

    assert "`/linear`" in result
    assert "`/Linear`" not in result


@pytest.mark.asyncio
async def test_help_keeps_non_telegram_slash_command_mentions_unchanged(monkeypatch):
    """Only Telegram needs slash mentions rewritten to Telegram command names."""
    monkeypatch.setattr(
        "agent.skill_commands.get_skill_commands",
        lambda: {"/Linear": {"description": "Open Linear"}},
    )

    result = await _make_runner()._handle_help_command(
        _make_event("/help", Platform.DISCORD)
    )

    assert "`/Linear`" in result


@pytest.mark.asyncio
async def test_commands_respects_chinese_language(monkeypatch):
    monkeypatch.setenv("HERMES_LANGUAGE", "zh")
    from agent import i18n

    i18n.reset_language_cache()
    monkeypatch.setattr("agent.skill_commands.get_skill_commands", lambda: {})

    result = await _make_runner()._handle_commands_command(
        _make_event("/commands", Platform.GEWE)
    )

    assert "📚 **命令**" in result
    assert "开始一个新会话" in result
    assert "别名" in result
    assert "Start a new session" not in result


def test_gateway_help_lines_can_render_chinese(monkeypatch):
    monkeypatch.setenv("HERMES_LANGUAGE", "zh")
    from agent import i18n
    from hermes_cli.commands import gateway_help_lines

    i18n.reset_language_cache()
    lines = gateway_help_lines(lang="zh")

    assert any("开始一个新会话" in line for line in lines)
    assert any("别名" in line for line in lines)
    assert not any("Start a new session" in line for line in lines)
