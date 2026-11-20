# tests/test_main.py
# Tsclp_v2.2_volatile — тесты main.py (mock, без реального API).

import json
import os
import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import main
from execution import ExecutionMonitor, PositionState
from risk_manager import RiskManager
from tinkoff_client import PositionData


class FakeClient:
    def __init__(self, positions=None):
        self._positions = positions if positions is not None else {}
        self.uncertain_state = False
        self.order_states = {}

    async def get_positions(self):
        return self._positions

    async def get_sandbox_order_state(self, order_id):
        return self.order_states.get(order_id)

    def is_uncertain(self):
        return self.uncertain_state


def make_position(lots, direction="LONG"):
    return PositionData(lots=lots, direction=direction, raw_shares=Decimal(lots))


def register_buy(mon, figi="FIGI", qty=1, direction="BUY"):
    return mon.register(
        figi=figi, ticker="T", direction=direction,
        entry_price=100.0, quantity=qty, sl_price=99.0, tp_price=102.0,
        lot_size=1, trade_id="tid-" + figi,
        entry_requested_lots=qty,
    )


# --- reconcile ---

@pytest.mark.asyncio
async def test_reconcile_ok():
    client = FakeClient(positions={"F1": make_position(1)})
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=1)
    await main.reconcile(client, mon)
    assert main.reconcile_done is True
    assert client.uncertain_state is False


@pytest.mark.asyncio
async def test_reconcile_empty_broker_local_open_mismatch():
    """Локальная OPEN, брокер пуст -> расхождение."""
    client = FakeClient(positions={})
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=1)
    await main.reconcile(client, mon)
    assert main.reconcile_done is False
    assert client.uncertain_state is True


@pytest.mark.asyncio
async def test_reconcile_quantity_mismatch():
    client = FakeClient(positions={"F1": make_position(2)})
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=1)
    await main.reconcile(client, mon)
    assert main.reconcile_done is False
    assert client.uncertain_state is True


@pytest.mark.asyncio
async def test_reconcile_direction_mismatch():
    client = FakeClient(positions={"F1": make_position(1, direction="SHORT")})
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=1)  # LONG
    await main.reconcile(client, mon)
    assert main.reconcile_done is False
    assert client.uncertain_state is True


@pytest.mark.asyncio
async def test_reconcile_api_error():
    class ClientNone(FakeClient):
        async def get_positions(self):
            return None

    client = ClientNone()
    mon = ExecutionMonitor(client)
    await main.reconcile(client, mon)
    assert main.reconcile_done is False
    assert client.uncertain_state is True


@pytest.mark.asyncio
async def test_reconcile_currency_skipped():
    client = FakeClient(positions={"RUB000UTSTOM": make_position(100000)})
    mon = ExecutionMonitor(client)
    await main.reconcile(client, mon)
    assert main.reconcile_done is True


# --- state.json ---

def test_save_load_state_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(tmp_path / "state.json.tmp"))

    client = FakeClient()
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=3)
    rm = RiskManager(client)
    rm.daily_trade_counts = {"F1": 2}

    assert main._save_state(mon, rm) is True
    assert os.path.isfile(main.STATE_FILE)

    mon2 = ExecutionMonitor(client)
    rm2 = RiskManager(client)
    assert main._load_state(mon2, rm2) is True
    assert mon2.get_position("F1").quantity == 3
    assert rm2.daily_trade_counts == {"F1": 2}


def test_load_state_missing_returns_false(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "STATE_FILE", str(tmp_path / "missing.json"))
    client = FakeClient()
    mon = ExecutionMonitor(client)
    rm = RiskManager(client)
    assert main._load_state(mon, rm) is False


def test_load_state_corrupted_returns_false(tmp_path, monkeypatch):
    bad = tmp_path / "bad.json"
    bad.write_text("{not valid json", encoding="utf-8")
    monkeypatch.setattr(main, "STATE_FILE", str(bad))
    client = FakeClient()
    mon = ExecutionMonitor(client)
    rm = RiskManager(client)
    assert main._load_state(mon, rm) is False


# --- process_closing: UNCERTAIN ---

@pytest.mark.asyncio
async def test_process_closing_resolves_uncertain():
    client = FakeClient(positions={"F1": make_position(1)})
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, figi="F1", qty=1)
    pos.state = PositionState.UNCERTAIN
    await main.process_closing(client, mon)
    assert pos.state == PositionState.OPEN
# Дополнение к tests/test_main.py

@pytest.mark.asyncio
async def test_trading_cycle_not_fill_but_executed_creates_position(monkeypatch):
    """
    status=NEW (не FILL), но get_sandbox_order_state показал lots_executed > 0
    -> создаётся TrackedPosition (этап 4.1).
    """
    from t_tech.invest import OrderExecutionReportStatus
    from unittest.mock import AsyncMock, MagicMock
    from decimal import Decimal
    from execution import ExecutionMonitor
    from risk_manager import RiskManager
    from tinkoff_client import PositionData

    # Мок-клиент
    fake = MagicMock()
    fake.get_last_prices = AsyncMock(return_value={"F1": 54.25})
    fake.get_positions = AsyncMock(return_value={})
    fake.get_historical_candles = AsyncMock(return_value=[MagicMock()] * 100)
    fake.get_instrument_info = AsyncMock(return_value={"lot": 1, "ticker": "MVID"})
    fake.post_market_order = AsyncMock(return_value=MagicMock(
        order_id="ord-1",
        state=MagicMock(execution_report_status=OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW),
        error=None,
    ))
    # get_sandbox_order_state: lots_executed=359
    fake.get_sandbox_order_state = AsyncMock(return_value=MagicMock(
        lots_executed=359,
        executed_order_price=MagicMock(units=54, nano=150000000),
        executed_commission=None,
    ))
    fake.is_uncertain = MagicMock(return_value=False)

    monitor = ExecutionMonitor(fake)
    rm = RiskManager(fake)
    sm = MagicMock()
    sm.get_signal = MagicMock(return_value=("BUY", "MA(9/21) ↑"))
    sm.last_signal_candle_time = None
    sm.last_adx_value = None
    sm._last_regime = None
    sm._last_regime_candidate = None
    sm._regime_streak = None

    # Мок pandas.DataFrame
    import pandas as pd
    df = pd.DataFrame({"close": [54.25], "volume": [1000]})
    fake.get_historical_candles = AsyncMock(return_value=[
        MagicMock(time=pd.Timestamp("2026-11-17"), open=MagicMock(units=54, nano=0),
                  high=MagicMock(units=54, nano=0), low=MagicMock(units=54, nano=0),
                  close=MagicMock(units=54, nano=0), volume=1000, is_complete=True)
    ] * 100)

    rm.calculate_position_size = AsyncMock(return_value=359)
    rm.calculate_sl_tp = MagicMock(return_value=(53.0, 56.0))
    rm.record_trade = MagicMock()
    rm.export_state = MagicMock(return_value={})
    rm.import_state = MagicMock()

    main.top_instruments = [{"figi": "F1", "ticker": "MVID"}]
    main.reconcile_done = True
    main.is_trading_enabled = MagicMock(return_value=True)
    main._save_state = MagicMock(return_value=True)

    await main.trading_cycle(fake, rm, sm, monitor)

    # Проверка: TrackedPosition создан
    assert monitor.get_position("F1") is not None
    assert monitor.get_position("F1").quantity == 359