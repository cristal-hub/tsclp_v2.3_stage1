# tests/test_close_all_positions.py
# Tsclp_v2.2_volatile — тесты close_all_positions.py (mock, без реального API).

import os
import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import close_all_positions as cap
from tinkoff_client import PositionData


def make_position(lots, direction="LONG"):
    return PositionData(lots=lots, direction=direction, raw_shares=Decimal(lots))


@pytest.fixture(autouse=True)
def _clean_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(cap, "LOCK_FILE", str(tmp_path / "close.lock"))
    yield
    # cleanup
    try:
        if os.path.isfile(cap.LOCK_FILE):
            os.remove(cap.LOCK_FILE)
    except Exception:
        pass


def test_lock_exclusive(tmp_path, monkeypatch):
    monkeypatch.setattr(cap, "LOCK_FILE", str(tmp_path / "close.lock"))
    assert cap._acquire_lock() is True
    assert cap._acquire_lock() is False  # второй раз — False
    cap._release_lock()
    assert cap._acquire_lock() is True
    cap._release_lock()


# --- _cancel_active_orders ---

@pytest.mark.asyncio
async def test_cancel_orders_our_figi():
    client = MagicMock()
    client.account_id = "acc"
    client.client.sandbox.get_sandbox_orders = AsyncMock(
        return_value=MagicMock(orders=[MagicMock(order_id="o1", figi="F1")])
    )
    client.cancel_order = AsyncMock(return_value=MagicMock())
    client.get_sandbox_order_state = AsyncMock(
        return_value=MagicMock(execution_report_status=3, lots_executed=0, lots_left=0)
    )
    ok = await cap._cancel_active_orders(client, {"F1"})
    assert ok is True
    client.cancel_order.assert_awaited_once_with("o1")


@pytest.mark.asyncio
async def test_cancel_orders_foreign_figi_stops():
    client = MagicMock()
    client.account_id = "acc"
    client.client.sandbox.get_sandbox_orders = AsyncMock(
        return_value=MagicMock(orders=[MagicMock(order_id="o1", figi="OTHER")])
    )
    ok = await cap._cancel_active_orders(client, {"F1"})
    assert ok is False


@pytest.mark.asyncio
async def test_cancel_orders_partial_fill_warns_but_continues():
    client = MagicMock()
    client.account_id = "acc"
    client.client.sandbox.get_sandbox_orders = AsyncMock(
        return_value=MagicMock(orders=[MagicMock(order_id="o1", figi="F1")])
    )
    client.cancel_order = AsyncMock(return_value=MagicMock())
    client.get_sandbox_order_state = AsyncMock(
        return_value=MagicMock(execution_report_status=3, lots_executed=2, lots_left=0)
    )
    ok = await cap._cancel_active_orders(client, {"F1"})
    assert ok is True


# --- main: SHORT not sent ---

@pytest.mark.asyncio
async def test_main_short_not_sent(monkeypatch):
    positions = {
        "F1": make_position(5, "LONG"),
        "F2": make_position(-3, "SHORT"),
    }
    fake_client = MagicMock()
    fake_client.get_positions = AsyncMock(return_value=positions)
    fake_client.get_instrument_info = AsyncMock(return_value={"lot": 1, "ticker": "T"})
    fake_client.get_sandbox_max_lots = AsyncMock(
        return_value={"sell_max_lots": 5, "buy_max_lots": 3}
    )
    fake_client.post_market_order = AsyncMock(
        return_value=MagicMock(state=MagicMock(execution_report_status=1, lots_executed=5))
    )
    fake_client.cancel_order = AsyncMock()
    fake_client.client.sandbox.get_sandbox_orders = AsyncMock(
        return_value=MagicMock(orders=[])
    )

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(cap, "TinkoffClient", lambda *a, **kw: FakeCtx())
    monkeypatch.setattr(cap.config, "validate", lambda: None)
    monkeypatch.setattr(cap.config, "USE_SANDBOX", True)
    monkeypatch.setattr("builtins.input", lambda *a, **kw: "YES")

    # Финал: только SHORT остался
    fake_client.get_positions = AsyncMock(side_effect=[positions, positions, {"F2": make_position(-3, "SHORT")}])

    rc = await cap.main()
    # LONG отправлен, SHORT остался -> rc=1
    assert rc == 1
    # BUY не отправлялся (только SELL для F1)
    calls = fake_client.post_market_order.await_args_list
    assert all(c.args[2] != 1 for c in calls)  # direction != BUY (1)