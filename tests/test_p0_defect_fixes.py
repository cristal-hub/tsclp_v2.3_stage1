# tests/test_p0_defect_fixes.py
# Tsclp_v2.2_volatile — тесты точечного исправления дефектов первой поставки P0.
#
# Соответствие дефект ↔ сценарий (обязательный набор из постановки):
#   Дефект №1 (потеря частичного исполнения после отмены остатка):
#       сценарий 1  — test_scenario_1_partial_buy_cancelled_portfolio_silent
#       (плюс принцип в poll: test_scenario_4_*, test_poll_registers_before_cancel)
#   Дефект №2 (неверный статус частичного исполнения):
#       сценарий 2  — test_scenario_2_cancelled_with_executed_lots
#       сценарий 3  — test_scenario_3_expired_with_executed_lots
#       сценарий 4  — test_scenario_4_repeated_poll_no_quantity_doubling
#       сценарий 5  — test_scenario_5_late_execution_after_cancel
#   Дефект №3 (сброс возраста заявки):
#       сценарий 6  — test_scenario_6_new_order_regularly_polled_reaches_timeout
#       сценарий 7  — test_scenario_7_age_restored_after_restart
#   Дефект №4 (неизвестная заявка не блокирует BUY):
#       сценарий 8  — test_scenario_8_unknown_side_order_blocks_buy_by_figi
#       сценарий 9  — test_scenario_9_unknown_figi_order_blocks_buy_globally
#   Дефект №5 (повреждение реестра -> пустой достоверный реестр):
#       сценарий 10 — test_scenario_10_corrupted_ledger_blocks_buy
#       сценарий 11 — test_scenario_11_ledger_read_error_blocks_buy
#   Дефект №6 (знак количества SHORT в RECOVERY):
#       сценарий 12 — test_scenario_12_short_recovery_quantity_canonical
#   Дефект №7 (исключение calculate_sl_tp срывает регистрацию FILL):
#       сценарий 13 — test_scenario_13_sltp_exception_still_registers_recovery
#   Дефект №8 (противоречивый FILL с нулём лотов):
#       сценарий 14 — test_scenario_14_fill_with_zero_lots_not_confirmed
#       сценарий 15 — test_scenario_15_repeated_recovery_same_order_id_no_duplicate
#       сценарий 16 — test_scenario_16_registered_partial_not_added_again
#
# Тесты проверяют ПОСЛЕДСТВИЯ, а не только внутренние статусы: появилась ли
# позиция, корректно ли количество, заблокирован ли BUY, осталась ли заявка
# доступной для разрешения, есть ли дубликаты, не закрыта ли RECOVERY
# автоматически.

import builtins
import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import main
import logger as research_logger
import config
from order_ledger import OrderLedger
from execution import ExecutionMonitor, PositionState
from tinkoff_client import PositionData

FIGI = "BBG00TESTF0"
TICKER = "TEST"
OTHER_FIGI = "BBG00OTHERF1"


# ============================================================
# Фейки (mock, без реального API; SANDBOX-клиент не используется)
# ============================================================

class _Q:
    def __init__(self, units=0, nano=0):
        self.units = units
        self.nano = nano


def fake_state(status_code, lots_executed=0, price_units=0, price_nano=0, commission_units=0):
    """Объект OrderState/PostOrderResponse-подобный (поля как в реальном SDK:
    execution_report_status int, lots_executed int, MoneyValue units/nano)."""
    st = SimpleNamespace(
        execution_report_status=status_code,
        lots_executed=lots_executed,
        executed_order_price=_Q(price_units, price_nano),
        executed_commission=_Q(commission_units, 0) if commission_units else None,
    )
    return st


# Коды статусов (маппинг T-Invest, проверен по реальному SDK):
FILL, REJECTED, CANCELLED, NEW, PARTIALLYFILL, EXPIRED = 1, 2, 3, 4, 5, 6


class FakeClient:
    def __init__(self, positions=None):
        self.uncertain_state = False
        self.order_states = {}            # order_id -> состояние заявки
        self.positions_value = positions  # dict | None (None = снимок недоступен)
        self.positions_after_first = None # опционально: снимок при ПОВТОРНЫХ вызовах
        self.positions_calls = 0
        self.prices_value = {}
        self.orders_list = []             # активные заявки брокера (startup)
        self.post_state = None            # ответ post_sandbox_order
        self.final_state = None           # состояние заявки после отправки
        self.post_error = None
        self.cancel_response = object()   # не None — отмена подтверждена
        self.last_post = None
        self.cancel_calls = []
        self.instrument_info = {
            "lot": 1, "ticker": TICKER, "name": TICKER,
            "min_price_increment": 0.1, "instrument_type": "share",
        }

    async def get_positions(self):
        self.positions_calls += 1
        if self.positions_after_first is not None and self.positions_calls > 1:
            return self.positions_after_first
        return self.positions_value

    async def get_sandbox_order_state(self, order_id):
        return self.order_states.get(order_id)

    async def get_instrument_info(self, figi):
        return dict(self.instrument_info)

    async def get_last_prices(self, figi_list):
        return dict(self.prices_value)

    async def get_sandbox_orders(self):
        return list(self.orders_list)

    async def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        return self.cancel_response

    async def post_market_order(self, figi, quantity, direction, order_id=None):
        self.last_post = {
            "figi": figi, "quantity": quantity,
            "direction": str(direction), "order_id": order_id,
        }
        if self.final_state is not None and order_id:
            self.order_states[order_id] = self.final_state
        return SimpleNamespace(order_id=order_id, state=self.post_state, error=self.post_error)

    def is_uncertain(self):
        return self.uncertain_state


class FakeRM:
    def __init__(self, sl=95.0, tp=110.0, size=5, raise_sltp=False):
        self.sl = sl
        self.tp = tp
        self.size = size
        self.raise_sltp = raise_sltp
        self.recorded = []

    def calculate_sl_tp(self, price, direction, df):
        if self.raise_sltp:
            raise RuntimeError("simulated sl/tp calculation failure")
        return self.sl, self.tp

    async def calculate_position_size(self, figi, price, df):
        return self.size

    def record_trade(self, figi):
        self.recorded.append(figi)

    def export_state(self):
        return {"fake": True}


class FakeSM:
    def __init__(self, signal="BUY", reason="unit-test-signal"):
        self.signal = signal
        self.reason = reason
        self.calls = 0

    def get_signal(self, df, price, drop_incomplete=True):
        self.calls += 1
        return self.signal, self.reason


def small_df():
    import pandas as pd
    return pd.DataFrame({"close": [100.0, 101.0, 99.0, 100.5, 100.2]})


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Изоляция файловых побочных эффектов main/logger + включённая торговля."""
    monkeypatch.setattr(main, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(tmp_path / "state.json.tmp"))
    monkeypatch.setattr(main, "TRADING_FLAG", str(tmp_path / "trading.flag"))
    monkeypatch.setattr(research_logger, "EVENTS_FILE", str(tmp_path / "events.jsonl"))
    monkeypatch.setattr(research_logger, "TRADES_FILE", str(tmp_path / "trades.csv"))
    monkeypatch.setattr(main, "reconcile_done", True)
    monkeypatch.setattr(main, "state_save_failed", False)
    monkeypatch.setattr(main, "top_instruments", [])
    # свечи для расчёта SL/TP — mocked (стратегия не тестируется здесь)
    async def fake_candles(client, figi):
        return small_df()
    monkeypatch.setattr(main, "_build_candles_df", fake_candles)
    # торговля включена (флаг в tmp)
    with open(tmp_path / "trading.flag", "w", encoding="utf-8") as f:
        f.write(datetime.now(timezone.utc).isoformat())
    return SimpleNamespace(tmp_path=tmp_path)


def ledger_line(oid, status="INTENT", figi=FIGI, side="BUY", requested=10, executed=0,
                created_at=None, updated_at=None, reason="", trade_id="t-1"):
    ts = (updated_at or created_at or datetime.now(timezone.utc)).isoformat()
    rec = {
        "timestamp_utc": ts, "status": status, "order_id": oid,
        "trade_id": trade_id, "figi": figi, "ticker": TICKER, "side": side,
        "requested_lots": requested, "executed_lots": executed, "lots_left": None,
        "avg_exec_price": None, "reason": reason, "source": "test", "error": None,
        "created_at_utc": (created_at or ts).isoformat() if isinstance(created_at or ts, datetime) else (created_at or ts),
        "updated_at_utc": (updated_at or ts).isoformat() if isinstance(updated_at or ts, datetime) else (updated_at or ts),
    }
    return json.dumps(rec, ensure_ascii=False)


def make_ledger(tmp_path):
    return OrderLedger(str(tmp_path / "order_ledger.jsonl"))


def make_instruments(figi=FIGI):
    return [{"figi": figi, "ticker": TICKER}]


async def run_entry_cycle(client, rm, sm, monitor, ledger, figi=FIGI):
    monkey_free = SimpleNamespace()  # placeholder, не используется
    main.top_instruments.clear()
    main.top_instruments.extend(make_instruments(figi))
    client.prices_value = {figi: 100.0}
    await main.trading_cycle(client, rm, sm, monitor, ledger)


# ============================================================
# Дефект №1: потеря частичного исполнения после отмены остатка
# ============================================================

@pytest.mark.asyncio
async def test_scenario_1_partial_buy_cancelled_portfolio_silent(env):
    """Сценарий 1 / Дефект №1: частичный BUY, остаток CANCELLED, портфель
    ВРЕМЕННО не показывает позицию — исполнение НЕ теряется, позиция
    регистрируется ровно в подтверждённом количестве."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})          # портфель временно пуст
    client.post_state = fake_state(PARTIALLYFILL, lots_executed=2)
    client.final_state = fake_state(CANCELLED, lots_executed=3, price_units=300)  # итог 300 = 3 лота * 100
    monitor = ExecutionMonitor(client)
    rm = FakeRM(sl=95.0, tp=110.0, size=5)
    sm = FakeSM("BUY")
    main.monitor_ref = monitor
    main.ledger_ref = ledger

    await run_entry_cycle(client, rm, sm, monitor, ledger)

    # ПОСЛЕДСТВИЯ: позиция появилась, ровно исполненное количество (3 из 5)
    assert client.last_post is not None
    oid = client.last_post["order_id"]
    pos = monitor.get_position(FIGI)
    assert pos is not None, "подтверждённое исполнение потеряно"
    assert pos.quantity == 3, f"в позицию попало неверное количество: {pos.quantity}"
    assert pos.state == PositionState.OPEN     # цена и SL/TP были доступны
    assert pos.entry_price == 100.0
    assert pos.sl_price == 95.0 and pos.tp_price == 110.0
    assert pos.partial is True
    # Статус заявки и факт исполнения раздельно: заявка CANCELLED с исполнением
    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED"
    assert rec["executed_lots"] == 3
    assert len(client.cancel_calls) == 1
    # Дубликат не создан
    assert len(monitor.positions) == 1


@pytest.mark.asyncio
async def test_defect1_unavailable_portfolio_with_confirmed_lots_registers(env):
    """Дефект №1 (снимок недоступен): брокер подтвердил lots_executed>0, но
    свежий снимок портфеля ВТОРОЙ запрос вернул недостоверным — исполнение
    всё равно регистрируется (отсутствие снимка не отменяет исполнение)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})         # первый снимок (шаг 6) корректен
    client.positions_after_first = None       # свежий снимок в ветке partial недостоверен
    client.post_state = fake_state(PARTIALLYFILL, lots_executed=1)
    client.final_state = fake_state(CANCELLED, lots_executed=2, price_units=200)  # итог 200 = 2 лота * 100
    monitor = ExecutionMonitor(client)
    rm = FakeRM(sl=95.0, tp=110.0, size=5)
    sm = FakeSM("BUY")
    main.monitor_ref = monitor
    main.ledger_ref = ledger

    await run_entry_cycle(client, rm, sm, monitor, ledger)

    oid = client.last_post["order_id"]
    pos = monitor.get_position(FIGI)
    assert pos is not None, "исполнение потеряно при недоступном снимке"
    assert pos.quantity == 2
    assert pos.state == PositionState.OPEN     # df доступен — SL/TP рассчитаны
    rec = ledger.latest(oid)
    assert rec["executed_lots"] == 2


# ============================================================
# Дефект №2: CANCELLED/EXPIRED с исполненными лотами
# ============================================================

async def _poll_cancelled_with_execution(env, status_code):
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-terminal-exec"
    ledger.record_intent(oid, trade_id="t-1", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    client.order_states[oid] = fake_state(status_code, lots_executed=3, price_units=100)
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    await main.poll_open_orders(client, monitor, rm, ledger)
    return ledger, client, monitor, oid


@pytest.mark.asyncio
async def test_scenario_2_cancelled_with_executed_lots(env):
    """Сценарий 2 / Дефект №2: CANCELLED с lots_executed=3 — отменённая заявка
    С ЧАСТИЧНЫМ ИСПОЛНЕНИЕМ; в позиции ровно 3 лота (не 10), статус заявки
    в реестре CANCELLED (не FILL), partial=True."""
    ledger, client, monitor, oid = await _poll_cancelled_with_execution(env, CANCELLED)
    pos = monitor.get_position(FIGI)
    assert pos is not None, "исполненная часть не зарегистрирована"
    assert pos.quantity == 3, "неисполненный остаток попал в позицию"
    assert pos.partial is True
    assert pos.state == PositionState.RECOVERY  # свечей нет — RECOVERY, не фиктивные данные
    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED", f"статус заявки искажён: {rec['status']}"
    assert rec["executed_lots"] == 3


@pytest.mark.asyncio
async def test_scenario_3_expired_with_executed_lots(env):
    """Сценарий 3 / Дефект №2: EXPIRED с lots_executed=3 — истёкшая заявка
    с частичным исполнением; позиция = 3 лота, статус EXPIRED."""
    ledger, client, monitor, oid = await _poll_cancelled_with_execution(env, EXPIRED)
    pos = monitor.get_position(FIGI)
    assert pos is not None
    assert pos.quantity == 3
    assert pos.partial is True
    rec = ledger.latest(oid)
    assert rec["status"] == "EXPIRED"
    assert rec["executed_lots"] == 3


@pytest.mark.asyncio
async def test_scenario_4_repeated_poll_no_quantity_doubling(env):
    """Сценарий 4 / Дефект №2: повторный опрос частично исполненной заявки
    не дублирует позицию и не прибавляет уже учтённые лоты."""
    ledger, client, monitor, oid = await _poll_cancelled_with_execution(env, CANCELLED)
    pos = monitor.get_position(FIGI)
    assert pos is not None and pos.quantity == 3

    # повторный опрос той же заявки
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert len(monitor.positions) == 1, "создан дубликат позиции"
    assert monitor.get_position(FIGI).quantity == 3, "лоты прибавлены повторно"


@pytest.mark.asyncio
async def test_scenario_5_late_execution_after_cancel(env):
    """Сценарий 5 / Дефект №2: позднее исполнение — брокер обновляет состояние
    ПОСЛЕ отмены (2 -> 5 лотов): исполненная часть растёт до 5, дубликата нет,
    уже учтённые лоты не прибавляются повторно (синхронизация RECOVERY)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-late"
    ledger.record_intent(oid, trade_id="t-2", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    monitor = ExecutionMonitor(client)

    # Фаза 1: частичное исполнение 2 лота, отмена остатка
    # (исполненная часть регистрируется ДО отмены — дефект №1)
    client.order_states[oid] = fake_state(PARTIALLYFILL, lots_executed=2, price_units=100)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert monitor.get_position(FIGI).quantity == 2
    assert client.cancel_calls == [oid]

    # Фаза 2: брокер обновил состояние после отмены — исполнено 5 лотов,
    # заявка переведена в терминальный CANCELLED
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=5, price_units=100)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert len(monitor.positions) == 1, "дубликат при позднем исполнении"
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 5, "позднее исполнение не учтено"
    assert pos.state == PositionState.RECOVERY
    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED"
    assert rec["executed_lots"] == 5


@pytest.mark.asyncio
async def test_poll_registers_before_cancel(env):
    """Дефект №1 (принцип в poll): подтверждённые лоты регистрируются ДО
    отмены остатка — отменённая часть не теряет уже исполненное."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-partial-live"
    ledger.record_intent(oid, trade_id="t-3", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    client.order_states[oid] = fake_state(PARTIALLYFILL, lots_executed=4, price_units=100)
    monitor = ExecutionMonitor(client)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    # исполненная часть зарегистрирована сразу, остаток отменён
    assert client.cancel_calls == [oid]
    pos = monitor.get_position(FIGI)
    assert pos is not None
    assert pos.quantity == 4
    rec = ledger.latest(oid)
    assert "cancel_requested" in (rec.get("reason") or "")
    assert rec["executed_lots"] == 4


# ============================================================
# Дефект №3: возраст заявки не сбрасывается
# ============================================================

def test_defect3_created_at_not_reset_by_status_updates(env, tmp_path):
    """Сценарий 6 (юнит-часть) / Дефект №3: record_status (NEW) НЕ сбрасывает
    created_at_utc; обновляется только updated_at_utc/timestamp_utc."""
    ledger = make_ledger(tmp_path)
    oid = "oid-age"
    ledger.record_intent(oid, trade_id="t-4", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    rec0 = ledger.latest(oid)
    created0 = rec0["created_at_utc"]
    assert created0 is not None

    # несколько опросов подряд (как в цикле)
    for _ in range(3):
        ledger.record_status(oid, "NEW", executed_lots=0,
                             reason="активна, под наблюдением опроса", source="poll")
    rec = ledger.latest(oid)
    assert rec["created_at_utc"] == created0, "возраст сброшен обновлением статуса"
    assert rec["timestamp_utc"] > rec["created_at_utc"]
    # возраст в циклах положительный и не обнулён
    age = main._age_in_cycles(rec)
    assert age > 0.0


@pytest.mark.asyncio
async def test_scenario_6_new_order_regularly_polled_reaches_timeout(env):
    """Сценарий 6 / Дефект №3: заявка NEW регулярно опрашивается и всё равно
    достигает таймаута — остаток отменяется (возраст считается от INTENT)."""
    ledger = make_ledger(env.tmp_path)
    oid = "oid-aged"
    created = datetime.now(timezone.utc) - timedelta(
        seconds=(main.ORDER_POLL_MAX_AGE_CYCLES + 5) * config.TRADING_CYCLE_SECONDS)
    # запись с давним created_at (как после долгой жизни заявки), статус NEW
    path = ledger.path
    with open(path, "a", encoding="utf-8") as f:
        f.write(ledger_line(oid, status="NEW", created_at=created, updated_at=created) + "\n")
    ledger.load()

    client = FakeClient(positions={})
    client.order_states[oid] = fake_state(NEW, lots_executed=0)
    monitor = ExecutionMonitor(client)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert client.cancel_calls == [oid], "таймаут не достигается при регулярном опросе"
    rec = ledger.latest(oid)
    assert "cancel_requested" in (rec.get("reason") or "")


def test_scenario_7_age_restored_after_restart(env, tmp_path):
    """Сценарий 7 / Дефект №3: после перезапуска робота возраст восстанавливается
    из реестра (created_at_utc хранится в журнале), а не начинается заново."""
    path = str(tmp_path / "order_ledger.jsonl")
    oid = "oid-restart"
    created = datetime.now(timezone.utc) - timedelta(
        seconds=(main.ORDER_POLL_MAX_AGE_CYCLES + 10) * config.TRADING_CYCLE_SECONDS)
    with open(path, "a", encoding="utf-8") as f:
        f.write(ledger_line(oid, status="INTENT", created_at=created) + "\n")

    # «перезапуск»: новый экземпляр реестра
    ledger = OrderLedger(path)
    ledger.load()
    assert ledger.latest(oid) is not None
    # опрос обновил статус — created_at сохранился
    ledger.record_status(oid, "NEW", executed_lots=0, source="poll")
    rec = ledger.latest(oid)
    assert main._age_in_cycles(rec) >= main.ORDER_POLL_MAX_AGE_CYCLES, \
        "возраст не восстановлен из реестра после перезапуска"


# ============================================================
# Дефект №4: неизвестная заявка блокирует BUY
# ============================================================

@pytest.mark.asyncio
async def test_scenario_8_unknown_side_order_blocks_buy_by_figi(env):
    """Сценарий 8 / Дефект №4: неизвестная заявка с side=None (направление не
    распознано) блокирует новые BUY по своему FIGI; существующие позиции
    продолжают мониториться."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    client.orders_list = [
        SimpleNamespace(order_id="u1", figi=FIGI, direction=None,
                        lots_requested=5, lots_executed=0),
    ]
    monitor = ExecutionMonitor(client)
    # существующая позиция по другому инструменту — должна остаться под мониторингом
    monitor.register(figi=OTHER_FIGI, ticker="OTHER", direction="BUY",
                     entry_price=100.0, quantity=1, sl_price=99.0, tp_price=102.0,
                     lot_size=1, trade_id="t-other", entry_requested_lots=1)

    await main.startup_recovery(client, monitor, FakeRM(), ledger)

    rec = ledger.latest("u1")
    assert rec is not None and rec["status"] == "UNCERTAIN"
    assert rec.get("side") is None
    assert ledger.figi_blocks_buy(FIGI) is True, "side=None не блокирует BUY по FIGI"
    assert ledger.figi_blocks_buy(OTHER_FIGI) is False
    assert ledger.has_unknown_figi_open_orders() is False
    # существующие позиции продолжают мониториться
    assert monitor.get_position(OTHER_FIGI).state == PositionState.OPEN
    assert monitor.active_count() == 1

    # и торговый цикл не входит по этому FIGI
    rm, sm = FakeRM(), FakeSM("BUY")
    await run_entry_cycle(client, rm, sm, monitor, ledger)
    assert sm.calls == 0, "BUY разрешён при неизвестной заявке по FIGI"
    assert client.last_post is None


@pytest.mark.asyncio
async def test_scenario_9_unknown_figi_order_blocks_buy_globally(env):
    """Сценарий 9 / Дефект №4: неизвестная заявка БЕЗ FIGI блокирует ВСЕ новые
    BUY глобально до разрешения неопределённости."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    client.orders_list = [
        SimpleNamespace(order_id="u2", figi=None, direction=1,
                        lots_requested=5, lots_executed=0),
    ]
    monitor = ExecutionMonitor(client)
    await main.startup_recovery(client, monitor, FakeRM(), ledger)

    rec = ledger.latest("u2")
    assert rec is not None and rec["status"] == "UNCERTAIN"
    assert ledger.has_unknown_figi_open_orders() is True

    rm, sm = FakeRM(), FakeSM("BUY")
    await run_entry_cycle(client, rm, sm, monitor, ledger)
    assert sm.calls == 0, "глобальная блокировка не сработала"
    assert client.last_post is None


@pytest.mark.asyncio
async def test_defect4_unknown_side_resolves_only_without_execution(env):
    """Дефект №4 (разрешение): заявка с side=None разрешается ТОЛЬКО при
    достоверном завершении без исполнения; при признаках исполнения остаётся
    UNCERTAIN (BUY не разблокируются)."""
    ledger = make_ledger(env.tmp_path)
    oid = "u3"
    ledger.record_uncertain(oid, figi=FIGI, ticker=TICKER, side=None,
                            requested_lots=5, source="startup",
                            reason="активная заявка брокера без записи в реестре")
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)

    # (а) признаки исполнения при неизвестной стороне — остаётся UNCERTAIN
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=2)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN", "неизвестная сторона не должна авторазрешаться при исполнении"
    assert ledger.figi_blocks_buy(FIGI) is True
    # позиция НЕ регистрировалась автоматически
    assert monitor.get_position(FIGI) is None

    # (б) достоверное завершение без исполнения — блокировка снимается
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=0)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED"
    assert ledger.figi_blocks_buy(FIGI) is False


# ============================================================
# Дефект №5: повреждение OrderLedger не превращается в пустой реестр
# ============================================================

async def _assert_buy_blocked(env, ledger, client, monitor):
    rm, sm = FakeRM(), FakeSM("BUY")
    main.ledger_ref = ledger
    await run_entry_cycle(client, rm, sm, monitor, ledger)
    assert sm.calls == 0, "новые BUY не заблокированы"
    assert client.last_post is None


@pytest.mark.asyncio
async def test_scenario_10_corrupted_ledger_blocks_buy(env):
    """Сценарий 10 / Дефект №5: повреждённая строка журнала (в середине) —
    реестр недостоверен, новые BUY запрещены, журнал НЕ перезаписывается,
    целые записи продолжают читаться (best effort)."""
    path = str(env.tmp_path / "order_ledger.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        f.write(ledger_line("oidA", status="INTENT", figi=FIGI) + "\n")
        f.write("{этот json повреждён\n")
        f.write(ledger_line("oidB", status="INTENT", figi=OTHER_FIGI) + "\n")

    ledger = OrderLedger(path)
    ledger.load()
    assert ledger.is_reliable() is False, "повреждение не обнаружено"
    assert "повреждён" in ledger.unreliable_reason
    # best effort: целые записи загружены, не выброшены
    assert ledger.latest("oidA") is not None
    assert ledger.latest("oidB") is not None
    # исходный журнал не удалён и не перезаписан
    raw = open(path, "r", encoding="utf-8").read()
    assert "{этот json повреждён" in raw

    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    await _assert_buy_blocked(env, ledger, client, monitor)


@pytest.mark.asyncio
async def test_scenario_11_ledger_read_error_blocks_buy(env, monkeypatch):
    """Сценарий 11 / Дефект №5: ошибка чтения реестра — явное состояние
    недостоверности (не «пустой достоверный реестр»), новые BUY запрещены."""
    path = str(env.tmp_path / "order_ledger.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        f.write(ledger_line("oidC", status="INTENT", figi=FIGI) + "\n")

    ledger = OrderLedger(path)
    real_open = builtins.open

    def broken_open(file, mode="r", *a, **kwargs):
        try:
            same = os.path.abspath(str(file)) == os.path.abspath(path)
        except Exception:
            same = False
        if same and "r" in str(mode):
            raise PermissionError(13, "simulated read failure")
        return real_open(file, mode, *a, **kwargs)

    monkeypatch.setattr(builtins, "open", broken_open)
    ledger.load()
    assert ledger.is_reliable() is False, "ошибка чтения не перевела реестр в недостоверность"
    assert ledger.unreliable_reason
    monkeypatch.undo()

    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    await _assert_buy_blocked(env, ledger, client, monitor)


@pytest.mark.asyncio
async def test_defect5_tail_corruption_no_truncate_blocks_buy(env):
    """Дефект №5: повреждённый ХВОСТ журнала не обрезается, не молчит и
    запрещает новые BUY до восстановления целостности."""
    path = str(env.tmp_path / "order_ledger.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        f.write(ledger_line("oidD", status="INTENT", figi=FIGI) + "\n")
        f.write('{"status": "INTENT", "order_id": "oidE", "trunc')  # обрыв append

    ledger = OrderLedger(path)
    ledger.load()
    assert ledger.is_reliable() is False
    raw = open(path, "r", encoding="utf-8").read()
    assert '"trunc' in raw, "журнал был перезаписан/обрезан"
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    await _assert_buy_blocked(env, ledger, client, monitor)


@pytest.mark.asyncio
async def test_defect5_missing_file_is_valid_empty_registry(env):
    """Дефект №5 (граница): ОТСУТСТВУЮЩИЙ журнал — валидный пустой реестр
    (заявок не было), торговля не блокируется."""
    ledger = OrderLedger(str(env.tmp_path / "not_existing.jsonl"))
    ledger.load()
    assert ledger.is_reliable() is True


# ============================================================
# Дефект №6: знак количества SHORT в RECOVERY
# ============================================================

def test_scenario_12_short_recovery_quantity_canonical(env):
    """Сценарий 12 / Дефект №6: SHORT RECOVERY хранит канонически ПОЛОЖИТЕЛЬНОЕ
    количество; знак брокера не попадает в pos.quantity; противоречащий знак
    не синхронизируется; ноль не закрывает RECOVERY автоматически. LONG
    проверен отдельно."""
    client = FakeClient()
    mon = ExecutionMonitor(client)

    # --- SHORT ---
    short = mon.register_recovery(figi=FIGI, ticker=TICKER, direction="SELL",
                                  quantity=5, lot_size=1, recovery_source="adopted")
    assert short.quantity == 5
    # брокер сообщает -7 (SHORT со знаком) — локально 7 положительных лотов
    assert mon.sync_recovery_quantity(short, -7) is True
    assert short.quantity == 7, "знак брокера попал в pos.quantity"
    assert short.executed_lots == 7 and short.entry_applied_lots == 7
    assert short.direction == "SELL"
    # знак, противоречащий направлению, НЕ синхронизируется
    assert mon.sync_recovery_quantity(short, 3) is False
    assert short.quantity == 7
    # ноль не синхронизируется и НЕ закрывает RECOVERY
    assert mon.sync_recovery_quantity(short, 0) is False
    assert short.state == PositionState.RECOVERY
    # то же значение — без изменений
    assert mon.sync_recovery_quantity(short, -7) is False

    # --- LONG отдельно ---
    long_pos = mon.register_recovery(figi=OTHER_FIGI, ticker="OTHER", direction="BUY",
                                     quantity=3, lot_size=1, recovery_source="adopted")
    assert mon.sync_recovery_quantity(long_pos, 6) is True
    assert long_pos.quantity == 6
    assert mon.sync_recovery_quantity(long_pos, -2) is False  # знак против LONG
    assert long_pos.quantity == 6


def test_defect6_resolve_uncertain_short_uses_abs(env):
    """Дефект №6: resolve_uncertain для SHORT сравнивает количество по модулю
    (знак проверен через направление) — SHORT не застревает в UNCERTAIN."""
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = mon.register_recovery(figi=FIGI, ticker=TICKER, direction="SELL",
                                quantity=5, lot_size=1, recovery_source="adopted")
    pos.state = PositionState.UNCERTAIN

    broker = {FIGI: PositionData(lots=-5, direction="SHORT", raw_shares=Decimal(-5))}
    mon.resolve_uncertain(pos, broker)
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 5, "отрицательное количество попало в позицию"

    # расхождение по количеству — остаётся UNCERTAIN
    pos2 = mon.register_recovery(figi=OTHER_FIGI, ticker="OTHER", direction="SELL",
                                 quantity=5, lot_size=1, recovery_source="adopted")
    pos2.state = PositionState.UNCERTAIN
    broker2 = {OTHER_FIGI: PositionData(lots=-3, direction="SHORT", raw_shares=Decimal(-3))}
    mon.resolve_uncertain(pos2, broker2)
    assert pos2.state == PositionState.UNCERTAIN


# ============================================================
# Дефект №7: исключение calculate_sl_tp не срывает регистрацию
# ============================================================

@pytest.mark.asyncio
async def test_scenario_13_sltp_exception_still_registers_recovery(env):
    """Сценарий 13 / Дефект №7: calculate_sl_tp() выбросил исключение ПОСЛЕ
    подтверждённого исполнения — позиция создаётся как RECOVERY с известными
    данными, без фиктивных цены/SL/TP, и НЕ закрывается автоматически."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient()
    monitor = ExecutionMonitor(client)
    rm = FakeRM(raise_sltp=True)

    pos = await main.register_executed_buy(
        client, monitor, rm, ledger,
        figi=FIGI, ticker=TICKER, order_id="oid-sltp", trade_id="t-5",
        reason="unit", signal_time="now", price_before_order=100.0,
        order_send_time="now", requested_lots=4, executed_lots=4,
        exec_price=100.0, lot_size=1, partial=False,
        df=small_df(), source="test",
    )
    # ПОСЛЕДСТВИЯ: позиция существует, исполнение не потеряно
    assert pos is not None
    assert pos.state == PositionState.RECOVERY
    assert pos.quantity == 4
    assert pos.entry_order_id == "oid-sltp"
    assert pos.direction == "BUY"
    assert pos.execution_price == 100.0      # известная цена исполнения сохранена
    assert pos.entry_price == 0.0            # фиктивная цена входа НЕ установлена
    assert pos.sl_price == 0.0 and pos.tp_price == 0.0  # фиктивные SL/TP НЕ созданы

    # RECOVERY не закрывается автоматически при SL/TP-мониторинге
    to_close = await monitor.check_all({FIGI: 50.0})
    assert to_close == []
    assert monitor.get_position(FIGI).state == PositionState.RECOVERY

    # запись в реестре связана с заявкой
    rec = ledger.latest("oid-sltp")
    assert rec is not None
    assert rec["executed_lots"] == 4


# ============================================================
# Дефект №8: противоречивый FILL с нулём исполненных лотов
# ============================================================

def test_scenario_14_record_sent_fill_zero_lots_not_confirmed(env, tmp_path):
    """Сценарий 14 / Дефект №8: ответ FILL с lots_executed=0 НЕ фиксируется
    как терминальный FILL: заявка остаётся UNCERTAIN, доступна для разрешения,
    FIGI заблокирован для новых BUY."""
    ledger = make_ledger(tmp_path)
    oid = "oid-fill0"
    assert ledger.record_intent(oid, trade_id="t-6", figi=FIGI, ticker=TICKER,
                                side="BUY", requested_lots=5) is True
    ok = ledger.record_sent(oid, figi=FIGI, ticker=TICKER, side="BUY",
                            requested_lots=5, executed_lots=0, status_hint="FILL",
                            source="post_response")
    assert ok is True
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN", f"FILL с нулём лотов записан как {rec['status']}"
    # заявка доступна для дальнейшего разрешения
    assert ledger.has_order(oid)
    assert any(r["order_id"] == oid for r in ledger.open_orders(side="BUY"))
    assert ledger.figi_blocks_buy(FIGI) is True

    # executed_lots=None — тоже не подтверждённый FILL
    oid2 = "oid-fill-null"
    ledger.record_intent(oid2, trade_id="t-7", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=5)
    ledger.record_sent(oid2, figi=FIGI, side="BUY", executed_lots=None, status_hint="FILL")
    assert ledger.latest(oid2)["status"] == "UNCERTAIN"

    # FILL с подтверждёнными лотами остаётся FILL (норма не сломана)
    oid3 = "oid-fill-ok"
    ledger.record_intent(oid3, trade_id="t-8", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=5)
    ledger.record_sent(oid3, figi=FIGI, side="BUY", executed_lots=5, status_hint="FILL")
    assert ledger.latest(oid3)["status"] == "FILL"


@pytest.mark.asyncio
async def test_scenario_14_fill_zero_lots_resolves_after_reconnect(env):
    """Дефект №8 (продолжение): после восстановления связи робот повторно
    запрашивает состояние у брокера и разрешает заявку (позиция 5 лотов)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-fill0-reconnect"
    ledger.record_intent(oid, trade_id="t-9", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=5)
    # противоречивый ответ post: FILL с 0 лотов
    ledger.record_sent(oid, figi=FIGI, side="BUY", executed_lots=0, status_hint="FILL")
    assert ledger.latest(oid)["status"] == "UNCERTAIN"

    monitor = ExecutionMonitor(client)
    # связь недоступна: заявка остаётся в реестре
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert ledger.latest(oid)["status"] == "UNCERTAIN"

    # связь восстановлена — повторный запрос состояния у брокера
    client.order_states[oid] = fake_state(FILL, lots_executed=5, price_units=100)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos is not None and pos.quantity == 5
    rec = ledger.latest(oid)
    assert rec["status"] == "FILL"
    assert rec["executed_lots"] == 5


@pytest.mark.asyncio
async def test_defect8_poll_fill_zero_lots_stays_uncertain(env):
    """Дефект №8 в poll: FILL с lots_executed=0 — UNCERTAIN, НЕ терминальный,
    НЕ отказ; FIGI остаётся заблокированным."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-poll-fill0"
    ledger.record_intent(oid, trade_id="t-10", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=5)
    client.order_states[oid] = fake_state(FILL, lots_executed=0)
    monitor = ExecutionMonitor(client)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN"
    assert monitor.get_position(FIGI) is None
    assert ledger.figi_blocks_buy(FIGI) is True


# ============================================================
# Идемпотентность восстановления (сценарии 15, 16)
# ============================================================

@pytest.mark.asyncio
async def test_scenario_15_repeated_recovery_same_order_id_no_duplicate(env):
    """Сценарий 15: повторное восстановление одного и того же order_id не
    создаёт дубликат позиции и не удваивает количество."""
    client = FakeClient()
    monitor = ExecutionMonitor(client)
    ledger = make_ledger(env.tmp_path)
    rm = FakeRM()
    kwargs = dict(
        figi=FIGI, ticker=TICKER, order_id="oid-same", trade_id="t-11",
        reason="unit", signal_time="now", price_before_order=100.0,
        order_send_time="now", requested_lots=4, executed_lots=4,
        exec_price=100.0, lot_size=1, partial=False, df=None, source="test",
    )
    pos1 = await main.register_executed_buy(client, monitor, rm, ledger, **kwargs)
    pos2 = await main.register_executed_buy(client, monitor, rm, ledger, **kwargs)
    assert pos1 is pos2, "создан второй объект позиции"
    assert len(monitor.positions) == 1
    assert monitor.get_position(FIGI).quantity == 4, "количество удвоено"


@pytest.mark.asyncio
async def test_scenario_16_registered_partial_not_added_again(env):
    """Сценарий 16: уже зарегистрированное частичное исполнение не прибавляется
    повторно при повторном опросе той же заявки."""
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    ledger = make_ledger(env.tmp_path)
    oid = "oid-partial-reg"
    ledger.record_intent(oid, trade_id="t-12", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    # прямая регистрация частичного исполнения (3 из 10)
    await main.register_executed_buy(
        client, monitor, FakeRM(), ledger,
        figi=FIGI, ticker=TICKER, order_id=oid, trade_id="t-12",
        reason="unit", signal_time="now", price_before_order=100.0,
        order_send_time="now", requested_lots=10, executed_lots=3,
        exec_price=100.0, lot_size=1, partial=True, df=None, source="test",
    )
    assert monitor.get_position(FIGI).quantity == 3

    # повторный опрос заявки с тем же исполнением
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=3, price_units=100)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert len(monitor.positions) == 1
    assert monitor.get_position(FIGI).quantity == 3, "частичное исполнение прибавлено повторно"


@pytest.mark.asyncio
async def test_defect2_only_executed_quantity_registered(env):
    """Дефект №2: в позицию регистрируется только реально исполненное
    количество; неисполненный остаток (10-2=8) не попадает в позицию."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-exec-only"
    ledger.record_intent(oid, trade_id="t-13", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=2, price_units=100)
    monitor = ExecutionMonitor(client)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos is not None
    assert pos.quantity == 2
    assert pos.entry_requested_lots in (10, 0)  # запрошенное количество не смешано с исполненным
    assert pos.entry_applied_lots == 2


# ============================================================
# Дополнительно: согласованность блокировок и менеджмента позиций
# ============================================================

@pytest.mark.asyncio
async def test_uncertain_buy_order_does_not_block_position_management(env):
    """Неопределённая BUY-заявка блокирует только НОВЫЕ входы; управление уже
    открытой позицией (SL/TP) продолжается."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={FIGI: PositionData(lots=1, direction="LONG",
                                                      raw_shares=Decimal(1))})
    ledger.record_intent("oid-block", trade_id="t-14", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=1)
    monitor = ExecutionMonitor(client)
    monitor.register(figi=FIGI, ticker=TICKER, direction="BUY",
                     entry_price=100.0, quantity=1, sl_price=99.0, tp_price=102.0,
                     lot_size=1, trade_id="t-15", entry_requested_lots=1)
    # цена ниже SL — позиция должна уйти в CLOSING несмотря на неопределённость
    prices = {FIGI: 98.0}
    to_close = await monitor.check_all(prices)
    assert len(to_close) == 1 and to_close[0].close_reason == "SL"
    assert monitor.get_position(FIGI).state == PositionState.CLOSING
