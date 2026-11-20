# tests/test_telegram_handlers.py
# Tsclp_v2.2_volatile — изолированные тесты Telegram-обработчиков.
#
# ВАЖНО:
#   - TelegramBot.__new__ (без __init__) — не создаётся Application.
#   - app_controls — MagicMock (не реальные start_trading/stop_trading).
#   - Нет реального бота, API, торгового цикла.
#   - Не создаёт trading.flag.
#   - Не меняет рабочие файлы.
#
# Тесты:
#   1. _authorized — разрешённый user -> True
#   2. _authorized — неизвестный user -> False
#   3. _authorized — allowed_user_id=None -> False
#   4. _authorized — effective_user=None -> False
#   5. start_command — авторизованный -> вызывает start_trading
#   6. stop_command — авторизованный -> вызывает stop_trading
#   7. status_command — авторизованный -> вызывает get_status
#   8. start_command — неавторизованный -> _deny, НЕ вызывает start_trading
#   9. stop_command — неавторизованный -> _deny, НЕ вызывает stop_trading
#   10. status_command — неавторизованный -> _deny, НЕ вызывает get_status

import pytest
from unittest.mock import AsyncMock, MagicMock

import telegram_bot
from telegram_bot import TelegramBot


# ------------------------------------------------------------
# Вспомогательные
# ------------------------------------------------------------

class FakeUser:
    def __init__(self, uid, username="user"):
        self.id = uid
        self.username = username


class FakeMessage:
    def __init__(self, text="/start"):
        self.text = text
        self.reply_text = AsyncMock()


class FakeUpdate:
    def __init__(self, uid=None, text="/start", user_exists=True):
        if user_exists:
            self.effective_user = FakeUser(uid) if uid is not None else None
        else:
            self.effective_user = None
        self.message = FakeMessage(text)


def make_bot(allowed_user_id=12345):
    """TelegramBot.__new__ — без __init__, без Application."""
    bot = TelegramBot.__new__(TelegramBot)
    bot.allowed_user_id = allowed_user_id
    bot.app_controls = {
        'start_trading': MagicMock(),
        'stop_trading': MagicMock(),
        'get_status': MagicMock(return_value="status text"),
    }
    return bot


# ------------------------------------------------------------
# 1. _authorized — разрешённый user
# ------------------------------------------------------------

def test_authorized_allowed_user():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=12345)
    assert bot._authorized(update) is True


# ------------------------------------------------------------
# 2. _authorized — неизвестный user
# ------------------------------------------------------------

def test_authorized_unknown_user():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=99999)
    assert bot._authorized(update) is False


# ------------------------------------------------------------
# 3. _authorized — allowed_user_id=None
# ------------------------------------------------------------

def test_authorized_allowed_none():
    bot = make_bot(allowed_user_id=None)
    update = FakeUpdate(uid=12345)
    assert bot._authorized(update) is False


# ------------------------------------------------------------
# 4. _authorized — effective_user=None
# ------------------------------------------------------------

def test_authorized_no_user():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=None, user_exists=False)
    assert bot._authorized(update) is False


# ------------------------------------------------------------
# 5. start_command — авторизованный
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_command_authorized():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=12345, text="/start")
    context = MagicMock()

    await bot.start_command(update, context)

    bot.app_controls['start_trading'].assert_called_once()
    update.message.reply_text.assert_awaited_once()


# ------------------------------------------------------------
# 6. stop_command — авторизованный
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_command_authorized():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=12345, text="/stop")
    context = MagicMock()

    await bot.stop_command(update, context)

    bot.app_controls['stop_trading'].assert_called_once()
    update.message.reply_text.assert_awaited_once()


# ------------------------------------------------------------
# 7. status_command — авторизованный
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_status_command_authorized():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=12345, text="/status")
    context = MagicMock()

    await bot.status_command(update, context)

    bot.app_controls['get_status'].assert_called_once()
    update.message.reply_text.assert_awaited_once()


# ------------------------------------------------------------
# 8. start_command — неавторизованный
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_command_unauthorized():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=99999, text="/start")
    context = MagicMock()

    await bot.start_command(update, context)

    bot.app_controls['start_trading'].assert_not_called()
    update.message.reply_text.assert_awaited_once()  # _deny


# ------------------------------------------------------------
# 9. stop_command — неавторизованный
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_command_unauthorized():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=99999, text="/stop")
    context = MagicMock()

    await bot.stop_command(update, context)

    bot.app_controls['stop_trading'].assert_not_called()
    update.message.reply_text.assert_awaited_once()


# ------------------------------------------------------------
# 10. status_command — неавторизованный
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_status_command_unauthorized():
    bot = make_bot(allowed_user_id=12345)
    update = FakeUpdate(uid=99999, text="/status")
    context = MagicMock()

    await bot.status_command(update, context)

    bot.app_controls['get_status'].assert_not_called()
    update.message.reply_text.assert_awaited_once()