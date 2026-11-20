# tests/test_stage1_packet_v.py
# Тесты v2.3.0-stage1, пакет В: самовосстановление учёта.
#   В2 — закрытие учёта по своей SELL-заявке из реестра;
#   В3 — внешнее (ручное) закрытие с двойным подтверждением;
#   В4 — автопривязка адоптированной RECOVERY-позиции к единственной BUY.
#   (В1 — сохранение state.json в конце цикла — проверяется чек-листом поля,
#    см. README_ИЗМЕНЕНИЯ.md.)
#
# Запуск из корня репозитория:
#   pytest tests/test_stage1_packet_v.py -v
#
# Вне окружения репозитория (нет SDK t_tech.invest / telegram) ставится
# минимальная заглушка, чтобы логика пакета В всё равно проверялась.

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import logger as research_logger
from execution import ExecutionMonitor, PositionState, TrackedPosition
from order_ledger import OrderLedger

UTC = timezone.utc

_IMPORT_ERROR = None


def _install_sdk_stubs():
    """Минимальные заглушки t_tech.invest / telegram_bot для запуска тестов
    вне окружения репозитория. В реальном окружении не используются."""
    import types

    class _EnumVal:
        def __init__(self, name):
            self.name = name
            self.value = name

    invest = types.ModuleType("t_tech.invest")
    invest.CandleInterval = types.SimpleNamespace(
        CANDLE_INTERVAL_5_MIN=_EnumVal("5min"))
    invest.OrderDirection = types.SimpleNamespace(
        ORDER_DIRECTION_BUY=_EnumVal("BUY"),
        ORDER_DIRECTION_SELL=_EnumVal("SELL"))
    invest.OrderExecutionReportStatus = types.SimpleNamespace(
        EXECUTION_REPORT_STATUS_FILL=_EnumVal("FILL"),
        EXECUTION_REPORT_STATUS_PARTIALLYFILL=_EnumVal("PARTIALLYFILL"),
        EXECUTION_REPORT_STATUS_NEW=_EnumVal("NEW"),
        EXECUTION_REPORT_STATUS_CANCELLED=_EnumVal("CANCELLED"),
        EXECUTION_REPORT_STATUS_REJECTED=_EnumVal("REJECTED"),
        EXECUTION_REPORT_STATUS_EXPIRED=_EnumVal("EXPIRED"))
    ttech = types.ModuleType("t_tech")
    ttech.invest = invest
    telegram_bot = types.ModuleType("telegram_bot")
    telegram_bot.TelegramBot = object

    sys.modules.setdefault("t_tech", ttech)
    sys.modules.setdefault("t_tech.invest", invest)
    sys.modules.setdefault("telegram_bot", telegram_bot)


try:
    import main
except Exception as exc:  # noqa: BLE001
    _IMPORT_ERROR = exc
    _install_sdk_stubs()
    try:
        import main  # noqa: F811
        _IMPORT_ERROR = None
    except Exception as exc2:  # noqa: BLE001
        _IMPORT_ERROR = exc2

pytestmark = pytest.mark.skipif(
    _IMPORT_ERROR is not None,
    reason=f"main.py недоступен: {_IMPORT_ERROR}",
)


@pytest.fixture()
def isolated_paths(tmp_path, monkeypatch):
    """Все файловые записи тестов — в tmp_path (не трогаем рабочие файлы)."""
    monkeypatch.setattr(research_logger, "EVENTS_FILE", str(tmp_path / "research_events.jsonl"))
    monkeypatch.setattr(research_logger, "TRADES_FILE", str(tmp_path / "research_trades.csv"))
    monkeypatch.setattr(main, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(tmp_path / "state.json.tmp"))
    main._external_close_candidates.clear()
    yield tmp_path
    main._external_close_candidates.clear()


def make_buy_pos(**over):
    base = dict(
        figi="F1", ticker="TEST", direction="BUY", entry_price=100.0,
        quantity=10, sl_price=98.0, tp_price=104.0, lot_size=1,
        state=PositionState.OPEN, trade_id="t1",
        order_send_time=(datetime.now(UTC) - timedelta(hours=1)).isoformat(),
    )
    base.update(over)
    return TrackedPosition(**base)


def run(coro):
    return asyncio.run(coro)


class _MV:
    def __init__(self, units, nano=0):
        self.units = units
        self.nano = nano


class FakeState:
    """Минимальный аналог отчёта заявки брокера (FILL=1, CANCELLED=3)."""
    def __init__(self, status_code=1, lots_executed=10, total_price_units=1000):
        self.execution_report_status = status_code
        self.lots_executed = lots_executed
        self.executed_order_price = _MV(total_price_units)
        self.executed_commission = None


class FakeClient:
    def __init__(self, order_state=None, prices=None, lot=1):
        self._order_state = order_state
        self._prices = prices or {}
        self._lot = lot

    async def get_sandbox_order_state(self, oid):
        return self._order_state

    async def get_instrument_info(self, figi):
        return {"lot": self._lot}

    async def get_last_prices(self, figis):
        return self._prices


def fresh_ledger(tmp_path):
    ledger = OrderLedger(str(tmp_path / "order_ledger.jsonl"))
    ledger.load()
    return ledger


def backdate(ledger, order_id, dt):
    """Сдвигает created_at_utc записи реестра (для проверки временных границ)."""
    rec = ledger.latest(order_id)
    rec["created_at_utc"] = dt.isoformat()
    ledger._latest[order_id] = rec


# ---------- В2: закрытие по своей SELL-заявке из реестра ----------

def test_v2_ledger_close_resolves_absent_position(isolated_paths):
    ledger = fresh_ledger(isolated_paths)
    pos = make_buy_pos(quantity=10)
    entry_dt = datetime.now(UTC) - timedelta(hours=1)
    pos.order_send_time = entry_dt.isoformat()
    sell_id = "sell-1"
    assert ledger.record_intent(sell_id, trade_id="t1", figi="F1", ticker="TEST",
                                side="SELL", requested_lots=10, reason="close:SL")
    backdate(ledger, sell_id, entry_dt + timedelta(seconds=5))
    assert ledger.record_sent(sell_id, figi="F1", ticker="TEST", side="SELL",
                              requested_lots=10, executed_lots=10,
                              avg_exec_price=None, status_hint="SENT",
                              reason="close:SL", source="post_response")
    client = FakeClient(order_state=FakeState(status_code=1, lots_executed=10,
                                              total_price_units=1000))
    action = run(main._try_resolve_absent_position(client, ledger, pos))
    assert action == "ledger_close"
    assert pos.state == PositionState.CLOSED
    assert pos.quantity == 0
    assert pos.executed_lots == 10
    assert pos.close_reason == "SL"                     # распарсено из "close:SL"
    assert pos.execution_price == pytest.approx(100.0)  # 1000 / (10 * lot=1)
    assert pos.logged is False                          # CSV запишет process_closing


def test_v2_old_sell_before_entry_is_not_matched(isolated_paths):
    """SELL-заявка СТАРЕЕ входа не должна закрывать новую позицию."""
    ledger = fresh_ledger(isolated_paths)
    pos = make_buy_pos(quantity=10)
    entry_dt = datetime.now(UTC) - timedelta(hours=1)
    pos.order_send_time = entry_dt.isoformat()
    old_sell = "sell-old"
    assert ledger.record_intent(old_sell, trade_id="old", figi="F1", ticker="TEST",
                                side="SELL", requested_lots=10, reason="close:SL")
    backdate(ledger, old_sell, entry_dt - timedelta(hours=2))
    assert ledger.record_sent(old_sell, figi="F1", ticker="TEST", side="SELL",
                              requested_lots=10, executed_lots=10,
                              status_hint="FILL", reason="close:SL")
    client = FakeClient(order_state=FakeState(status_code=1, lots_executed=10))
    action = run(main._try_resolve_absent_position(client, ledger, pos))
    assert action == "candidate"            # не ledger_close: старый SELL отфильтрован
    assert pos.state == PositionState.OPEN  # учёт не тронут


def test_v2_direct_close_order_id_synthesizes_record(isolated_paths):
    """CLOSING-позиция с close_order_id, но без записи в реестре (старый
    формат state.json): принадлежность роботу — по построению."""
    ledger = fresh_ledger(isolated_paths)
    pos = make_buy_pos(state=PositionState.CLOSING)
    pos.close_order_id = "sell-direct"
    pos.close_requested_lots = 10
    pos.close_reason = "SL"
    client = FakeClient(order_state=FakeState(status_code=1, lots_executed=10,
                                              total_price_units=990))
    action = run(main._try_resolve_absent_position(client, ledger, pos))
    assert action == "ledger_close"
    assert pos.state == PositionState.CLOSED
    assert pos.execution_price == pytest.approx(99.0)


def test_v2_cancelled_sell_without_execution_falls_to_v3(isolated_paths):
    """Отменённый SELL без исполнения не доказывает закрытие — уходим в В3."""
    ledger = fresh_ledger(isolated_paths)
    pos = make_buy_pos(quantity=10)
    entry_dt = datetime.now(UTC) - timedelta(hours=1)
    pos.order_send_time = entry_dt.isoformat()
    sell_id = "sell-x"
    ledger.record_intent(sell_id, trade_id="t1", figi="F1", ticker="TEST",
                         side="SELL", requested_lots=10, reason="close:SL")
    backdate(ledger, sell_id, entry_dt + timedelta(seconds=5))
    ledger.record_sent(sell_id, figi="F1", ticker="TEST", side="SELL",
                       requested_lots=10, executed_lots=0, status_hint="FILL")
    # брокер: заявка отменена без исполнения, цены нет
    client = FakeClient(order_state=FakeState(status_code=3, lots_executed=0,
                                              total_price_units=0))
    action = run(main._try_resolve_absent_position(client, ledger, pos))
    assert action == "candidate"
    assert pos.state == PositionState.OPEN


# ---------- В3: внешнее закрытие с двойным подтверждением ----------

def test_v3_external_close_requires_double_confirmation(isolated_paths):
    ledger = fresh_ledger(isolated_paths)
    pos = make_buy_pos(quantity=10)
    client = FakeClient(prices={"F1": 101.5})
    first = run(main._try_resolve_absent_position(client, ledger, pos))
    assert first == "candidate"
    assert pos.state == PositionState.OPEN   # первое обнаружение ничего не меняет
    second = run(main._try_resolve_absent_position(client, ledger, pos))
    assert second == "external_close"
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"
    assert pos.execution_price == pytest.approx(101.5)
    assert pos.executed_lots == 10
    assert "EXTERNAL_CLOSE" in pos.error_note
    assert pos.figi not in main._external_close_candidates


def test_v3_fallback_price_when_no_market_data(isolated_paths):
    """Нет текущей цены — берётся price_before_order, учёт всё равно закрывается."""
    ledger = fresh_ledger(isolated_paths)
    pos = make_buy_pos(quantity=10, price_before_order=100.5)
    client = FakeClient(prices={})           # get_last_prices пуст
    assert run(main._try_resolve_absent_position(client, ledger, pos)) == "candidate"
    assert run(main._try_resolve_absent_position(client, ledger, pos)) == "external_close"
    assert pos.execution_price == pytest.approx(100.5)


# ---------- В4: автопривязка RECOVERY ----------

def test_v4_binds_adopted_recovery_to_single_buy(isolated_paths):
    ledger = fresh_ledger(isolated_paths)
    buy_id = "buy-1"
    assert ledger.record_intent(buy_id, trade_id="wush", figi="F1", ticker="TEST",
                                side="BUY", requested_lots=495, reason="")
    assert ledger.record_sent(buy_id, figi="F1", ticker="TEST", side="BUY",
                              requested_lots=495, executed_lots=495,
                              status_hint="FILL", reason="")
    # имитируем «застрявшую» UNCERTAIN-запись (кейс WUSH/TRMK)
    ledger.record_uncertain(buy_id, figi="F1", ticker="TEST", side="BUY",
                            requested_lots=495, executed_lots=495,
                            reason="отчёт заявки НЕ привязан к RECOVERY-позиции")
    monitor = ExecutionMonitor(None)
    pos = monitor.register_recovery(figi="F1", ticker="TEST", direction="BUY",
                                    quantity=495, lot_size=1,
                                    recovery_source="adopted")
    rm = main.RiskManager(None)
    assert main._try_bind_recovery_entry(monitor, rm, ledger, pos) is True
    assert pos.entry_order_id == buy_id
    latest = ledger.latest(buy_id)
    # P0-5: действующая UNCERTAIN-запись НЕ перезаписывается привязкой —
    # она снимется штатным путём после подтверждённой сверки портфеля
    assert latest["status"] == "UNCERTAIN"
    assert "НЕ привязан" in (latest["reason"] or "")


def test_v4_refuses_ambiguous_or_unsafe_cases(isolated_paths):
    ledger = fresh_ledger(isolated_paths)
    monitor = ExecutionMonitor(None)
    pos = monitor.register_recovery(figi="F1", ticker="TEST", direction="BUY",
                                    quantity=495, lot_size=1,
                                    recovery_source="adopted")
    rm = main.RiskManager(None)
    # две BUY с исполнением — неоднозначность
    for oid, executed in (("buy-a", 300), ("buy-b", 195)):
        ledger.record_intent(oid, trade_id="x", figi="F1", ticker="TEST",
                             side="BUY", requested_lots=executed)
        ledger.record_sent(oid, figi="F1", ticker="TEST", side="BUY",
                           requested_lots=executed, executed_lots=executed,
                           status_hint="FILL")
    assert main._try_bind_recovery_entry(monitor, rm, ledger, pos) is False
    assert pos.entry_order_id is None
    # лоты не совпадают
    ledger2 = fresh_ledger(isolated_paths)
    ledger2.record_intent("buy-c", trade_id="x", figi="F1", ticker="TEST",
                          side="BUY", requested_lots=100)
    ledger2.record_sent("buy-c", figi="F1", ticker="TEST", side="BUY",
                        requested_lots=100, executed_lots=100, status_hint="FILL")
    assert main._try_bind_recovery_entry(monitor, rm, ledger2, pos) is False
    # заявка из архива закрытых сделок — «воскрешение» запрещено
    ledger3 = fresh_ledger(isolated_paths)
    ledger3.record_intent("buy-old", trade_id="x", figi="F1", ticker="TEST",
                          side="BUY", requested_lots=495)
    ledger3.record_sent("buy-old", figi="F1", ticker="TEST", side="BUY",
                        requested_lots=495, executed_lots=495, status_hint="FILL")
    monitor.closed_entry_order_ids.append("buy-old")
    assert main._try_bind_recovery_entry(monitor, rm, ledger3, pos) is False
