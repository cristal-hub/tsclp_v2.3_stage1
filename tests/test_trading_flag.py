# tests/test_trading_flag.py
# Tsclp_v2.2_volatile — изолированные тесты trading.flag.
#
# ВАЖНО:
#   - Импортирует main, но НЕ вызывает main().
#   - monkeypatch TRADING_FLAG -> tmp_path.
#   - Настоящий trading.flag в корне НЕ создаётся.
#   - Не подключается к Sandbox, не создаёт сетевые клиенты.
#   - Не меняет рабочие файлы.
#
# Тесты:
#   1. start_trading создаёт флаг в tmp_path.
#   2. stop_trading удаляет флаг.
#   3. is_trading_enabled True при наличии флага.
#   4. is_trading_enabled False при отсутствии флага.
#   5. Ошибка создания флага (недоступная директория) -> не падает.
#   6. stop_trading не трогает monitor.positions (мониторинг продолжается).
#   7. Отсутствие флага -> контракт блокировки новых входов (is_trading_enabled=False).

import os

import pytest

import main
from execution import ExecutionMonitor, PositionState
from tinkoff_client import PositionData
from decimal import Decimal


# ------------------------------------------------------------
# Вспомогательные
# ------------------------------------------------------------

class FakeClient:
    """Минимальный клиент: не подключается к сети."""
    def __init__(self):
        self.uncertain_state = False
        self.account_id = "fake"

    async def get_positions(self):
        return {}

    def is_uncertain(self):
        return self.uncertain_state


def register_buy(mon, figi="FIGI", qty=1):
    return mon.register(
        figi=figi, ticker="T", direction="BUY",
        entry_price=100.0, quantity=qty, sl_price=99.0, tp_price=102.0,
        lot_size=1, trade_id="tid-" + figi,
        entry_requested_lots=qty,
    )


@pytest.fixture
def flag_path(tmp_path, monkeypatch):
    """Перенаправляет TRADING_FLAG в tmp_path."""
    p = tmp_path / "trading.flag"
    monkeypatch.setattr(main, "TRADING_FLAG", str(p))
    return p


# ------------------------------------------------------------
# 1. start_trading создаёт флаг
# ------------------------------------------------------------

def test_start_trading_creates_flag(flag_path):
    assert not os.path.isfile(flag_path)
    main.start_trading()
    assert os.path.isfile(flag_path)
    with open(flag_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert content  # содержит ISO-время


# ------------------------------------------------------------
# 2. stop_trading удаляет флаг
# ------------------------------------------------------------

def test_stop_trading_removes_flag(flag_path):
    main.start_trading()
    assert os.path.isfile(flag_path)
    main.stop_trading()
    assert not os.path.isfile(flag_path)


# ------------------------------------------------------------
# 3. is_trading_enabled True при наличии флага
# ------------------------------------------------------------

def test_is_trading_enabled_true(flag_path):
    main.start_trading()
    assert main.is_trading_enabled() is True


# ------------------------------------------------------------
# 4. is_trading_enabled False при отсутствии флага
# ------------------------------------------------------------

def test_is_trading_enabled_false(flag_path):
    assert not os.path.isfile(flag_path)
    assert main.is_trading_enabled() is False


# ------------------------------------------------------------
# 5. Ошибка создания флага -> не падает
# ------------------------------------------------------------

def test_start_trading_error_does_not_raise(tmp_path, monkeypatch):
    """Недоступная директория -> start_trading не падает, флаг не создан."""
    bad_dir = tmp_path / "no_such_dir"
    bad_flag = bad_dir / "trading.flag"
    monkeypatch.setattr(main, "TRADING_FLAG", str(bad_flag))

    # Не должно быть исключения
    main.start_trading()
    assert not os.path.isfile(bad_flag)


# ------------------------------------------------------------
# 6. stop_trading не останавливает мониторинг и не закрывает позиции
# ------------------------------------------------------------

def test_stop_trading_does_not_touch_positions(flag_path):
    """
    stop_trading удаляет флаг, но не трогает monitor.positions.
    Позиции остаются в своих состояниях.
    """
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, figi="F1", qty=3)

    main.start_trading()
    assert main.is_trading_enabled() is True
    assert mon.get_position("F1") is not None
    assert pos.state == PositionState.OPEN

    main.stop_trading()
    assert main.is_trading_enabled() is False

    # Позиция не закрыта, состояние не изменено
    assert mon.get_position("F1") is not None
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 3


# ------------------------------------------------------------
# 7. Отсутствие флага -> контракт блокировки новых входов
# ------------------------------------------------------------

def test_no_flag_blocks_new_entries(flag_path):
    """
    is_trading_enabled() = False -> контракт: новые входы запрещены.
    Проверяем: флага нет, is_trading_enabled() = False.
    В main() это означает возврат до входов (не вызываем main()).
    """
    assert not os.path.isfile(flag_path)
    assert main.is_trading_enabled() is False
    # Контракт: main.trading_cycle при is_trading_enabled()=False
    # возвращается до входов (проверено в test_fail_closed.py)