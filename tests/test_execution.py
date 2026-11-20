import asyncio
import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.modules.setdefault("config", types.ModuleType("config"))
spec = importlib.util.spec_from_file_location("execution_under_test", ROOT / "execution.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

PS = mod.PositionState
class Client:
    async def get_sandbox_order_state(self, oid):
        return None

def make_position(qty=10):
    m = mod.ExecutionMonitor(Client())
    p = m.register(
        figi="FIGI1", ticker="TEST", direction="BUY", entry_price=100.0,
        quantity=qty, sl_price=95.0, tp_price=110.0, lot_size=1,
        trade_id="T1", entry_order_id="BUY1", entry_requested_lots=qty,
        entry_applied_lots=qty, executed_lots=qty,
    )
    return m, p

def test_partial_sell_then_second_sell_uses_own_cursor():
    _, p = make_position(10)
    p.close_order_id = "SELL1"
    p.close_requested_lots = 10
    p.state = PS.CLOSING
    assert p.close_orders == {}
    assert mod.ExecutionMonitor.add_close_order
    # first SELL cumulative 4
    assert mod.ExecutionMonitor.apply_close_execution(
        mod.ExecutionMonitor(None), p, "SELL1", 4, "PARTIALLYFILL",
        requested_lots=10
    )
    assert p.quantity == 6
    assert p.close_orders["SELL1"]["applied_lots"] == 4
    # terminal cancellation; leftover 6 shares stay open
    mod.ExecutionMonitor.apply_close_execution(
        mod.ExecutionMonitor(None), p, "SELL1", 4, "CANCELLED",
        requested_lots=10
    )
    assert p.quantity == 6
    assert p.state == PS.OPEN
    # second order has a fresh cursor
    p.close_order_id = "SELL2"
    p.close_requested_lots = 6
    mod.ExecutionMonitor.add_close_order(mod.ExecutionMonitor(None), p, "SELL2", 6)
    mod.ExecutionMonitor.apply_close_execution(
        mod.ExecutionMonitor(None), p, "SELL2", 2, "PARTIALLYFILL",
        requested_lots=6
    )
    assert p.quantity == 4
    assert p.close_orders["SELL1"]["applied_lots"] == 4
    assert p.close_orders["SELL2"]["applied_lots"] == 2

def test_duplicate_cumulative_report_does_not_double_apply():
    _, p = make_position(8)
    m = mod.ExecutionMonitor(None)
    m.add_close_order(p, "S1", 8)
    assert m.apply_close_execution(p, "S1", 3, "PARTIALLYFILL", requested_lots=8)
    assert p.quantity == 5
    assert m.apply_close_execution(p, "S1", 3, "PARTIALLYFILL", requested_lots=8)
    assert p.quantity == 5

def test_cumulative_growth_applies_only_delta():
    _, p = make_position(9)
    m = mod.ExecutionMonitor(None)
    m.add_close_order(p, "S1", 9)
    m.apply_close_execution(p, "S1", 2, "PARTIALLYFILL", requested_lots=9)
    m.apply_close_execution(p, "S1", 5, "PARTIALLYFILL", requested_lots=9)
    assert p.quantity == 4
    assert p.close_orders["S1"]["applied_lots"] == 5

def test_zero_remaining_closes_position():
    _, p = make_position(4)
    m = mod.ExecutionMonitor(None)
    m.add_close_order(p, "S1", 4)
    assert m.apply_close_execution(p, "S1", 4, "FILL", requested_lots=4)
    assert p.quantity == 0
    assert p.state == PS.CLOSED

def test_terminal_partial_sell_reopens_remaining_position():
    _, p = make_position(7)
    m = mod.ExecutionMonitor(None)
    m.add_close_order(p, "S1", 7)
    m.apply_close_execution(p, "S1", 3, "CANCELLED", requested_lots=7)
    assert p.quantity == 4
    assert p.state == PS.OPEN

def test_lower_cumulative_is_uncertain_without_quantity_change():
    _, p = make_position(7)
    m = mod.ExecutionMonitor(None)
    m.add_close_order(p, "S1", 7)
    m.apply_close_execution(p, "S1", 4, "PARTIALLYFILL", requested_lots=7)
    before = p.quantity
    assert not m.apply_close_execution(p, "S1", 2, "PARTIALLYFILL", requested_lots=7)
    assert p.quantity == before
    assert p.state == PS.UNCERTAIN

def test_execution_above_requested_is_uncertain():
    _, p = make_position(7)
    m = mod.ExecutionMonitor(None)
    m.add_close_order(p, "S1", 5)
    assert not m.apply_close_execution(p, "S1", 6, "FILL", requested_lots=5)
    assert p.quantity == 7
    assert p.state == PS.UNCERTAIN

def test_sell_delta_cannot_exceed_remaining_position():
    _, p = make_position(3)
    m = mod.ExecutionMonitor(None)
    m.add_close_order(p, "S1", 5)
    assert not m.apply_close_execution(p, "S1", 4, "PARTIALLYFILL", requested_lots=5)
    assert p.quantity == 3
    assert p.state == PS.UNCERTAIN

def test_state_roundtrip_preserves_multiple_sell_orders():
    m, p = make_position(10)
    m.add_close_order(p, "S1", 10)
    m.apply_close_execution(p, "S1", 3, "CANCELLED", requested_lots=10)
    m.add_close_order(p, "S2", 7)
    m.apply_close_execution(p, "S2", 2, "PARTIALLYFILL", requested_lots=7)
    exported = m.export_state()
    m2 = mod.ExecutionMonitor(None)
    m2.import_state(exported)
    p2 = m2.get_position("FIGI1")
    assert p2.quantity == 5
    assert set(p2.close_orders) == {"S1", "S2"}
    assert p2.close_orders["S1"]["applied_lots"] == 3
    assert p2.close_orders["S2"]["applied_lots"] == 2

def test_legacy_state_with_ambiguous_cursor_not_silently_reused():
    data = {
        "figi": "FIGI1", "ticker": "TEST", "direction": "BUY",
        "entry_price": 100, "quantity": 5, "sl_price": 95, "tp_price": 110,
        "close_order_id": "OLDSELL", "close_requested_lots": 10,
        "close_applied_lots": 3, "state": "CLOSING",
    }
    p = mod.TrackedPosition.from_dict(data)
    assert p.close_orders == {}
    assert p.close_applied_lots == 3
