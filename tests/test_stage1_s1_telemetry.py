# tests/test_stage1_s1_telemetry.py
# Тесты v2.3.0-stage1, часть С1: телеметрия позиции и CSV, безубыток/трейлинг.
# Не требует SDK — выполняется в любом окружении.
#
# Запуск из корня репозитория:
#   pytest tests/test_stage1_s1_telemetry.py -v

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import logger as research_logger
from execution import ExecutionMonitor, PositionState, TrackedPosition

UTC = timezone.utc


@pytest.fixture()
def isolated_paths(tmp_path, monkeypatch):
    """Все файловые записи тестов — в tmp_path (не трогаем рабочие файлы)."""
    monkeypatch.setattr(research_logger, "EVENTS_FILE", str(tmp_path / "research_events.jsonl"))
    monkeypatch.setattr(research_logger, "TRADES_FILE", str(tmp_path / "research_trades.csv"))
    yield tmp_path


@pytest.fixture()
def be_enabled(monkeypatch):
    monkeypatch.setattr(config, "BE_TRAIL_ENABLED", True)
    return True


def make_buy_pos(**over):
    base = dict(
        figi="F1", ticker="TEST", direction="BUY", entry_price=100.0,
        quantity=10, sl_price=98.0, tp_price=104.0, lot_size=1,
        state=PositionState.OPEN, trade_id="t1",
        order_send_time=(datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        # Прямая конструкция (не через monitor.register) — фиксируем С1-поля,
        # как это делает register() в проде.
        initial_sl_price=98.0, initial_tp_price=104.0,
    )
    base.update(over)
    return TrackedPosition(**base)


def run(coro):
    return asyncio.run(coro)


# ---------- TrackedPosition: обратная совместимость и инициализация ----------

def test_from_dict_legacy_state_restores_initials():
    """Старый state.json без телеметрии С1: initial_* <- sl/tp, экстремумы 0."""
    raw = {
        "figi": "F1", "ticker": "TEST", "direction": "BUY", "entry_price": 100.0,
        "quantity": 10, "sl_price": 98.0, "tp_price": 104.0, "lot_size": 1,
        "state": "OPEN",
    }
    pos = TrackedPosition.from_dict(raw)
    assert pos.initial_sl_price == 98.0
    assert pos.initial_tp_price == 104.0
    assert pos.mae_price == 0.0 and pos.mfe_price == 0.0
    assert pos.breakeven_moved is False


def test_roundtrip_to_dict_from_dict():
    pos = make_buy_pos(mae_price=95.0, mfe_price=103.0, breakeven_moved=True,
                       initial_sl_price=98.0, initial_tp_price=104.0)
    restored = TrackedPosition.from_dict(pos.to_dict())
    assert restored.mae_price == 95.0 and restored.mfe_price == 103.0
    assert restored.breakeven_moved is True


def test_register_sets_initial_sl_tp():
    monitor = ExecutionMonitor(None)
    pos = monitor.register(
        figi="F1", ticker="TEST", direction="BUY", entry_price=100.0,
        quantity=10, sl_price=98.0, tp_price=104.0, lot_size=1,
    )
    assert pos.initial_sl_price == 98.0
    assert pos.initial_tp_price == 104.0


def test_promote_recovery_sets_initials_and_resets_extremes():
    monitor = ExecutionMonitor(None)
    pos = monitor.register_recovery(figi="F1", ticker="TEST", direction="BUY",
                                    quantity=10, lot_size=1)
    pos.mae_price, pos.mfe_price, pos.breakeven_moved = 90.0, 110.0, True
    assert monitor.promote_recovery(pos, 100.0, 98.0, 104.0)
    assert pos.initial_sl_price == 98.0
    assert pos.initial_tp_price == 104.0
    assert pos.mae_price == 100.0 and pos.mfe_price == 100.0
    assert pos.breakeven_moved is False


def test_update_extremes_buy_and_sell():
    monitor = ExecutionMonitor(None)
    buy = make_buy_pos()
    assert monitor.update_extremes(buy, high=103.0, low=97.0)
    assert buy.mfe_price == 103.0 and buy.mae_price == 97.0
    sell = make_buy_pos(direction="SELL", entry_price=100.0, sl_price=102.0)
    assert monitor.update_extremes(sell, high=103.0, low=97.0)
    assert sell.mfe_price == 97.0 and sell.mae_price == 103.0
    # повтор с меньшим high не ухудшает mfe BUY
    monitor.update_extremes(buy, high=101.0, low=99.0)
    assert buy.mfe_price == 103.0 and buy.mae_price == 97.0


# ---------- триггер безубытка и трейлинг ----------

def test_be_trigger_and_trail_buy(be_enabled):
    monitor = ExecutionMonitor(None)
    pos = make_buy_pos()  # risk = 100 - 98 = 2.0
    monitor._update_sl(pos, atr=None)          # mfe неизвестен — ничего
    assert pos.sl_price == 98.0 and not pos.breakeven_moved
    pos.mfe_price = 101.0                       # r_peak = 0.5 < 1.0
    monitor._update_sl(pos, atr=None)
    assert not pos.breakeven_moved and pos.sl_price == 98.0
    pos.mfe_price = 102.0                       # r_peak = 1.0 -> BE
    monitor._update_sl(pos, atr=None)
    buffer = 100.0 * (2 * config.COMMISSION_PER_SIDE + config.SLIPPAGE_PERCENT)
    assert pos.breakeven_moved
    assert pos.sl_price == pytest.approx(100.0 + buffer)
    # трейлинг: mfe=103, atr=0.5 -> кандидат 103-1.5=101.5 выше BE-уровня
    pos.mfe_price = 103.0
    monitor._update_sl(pos, atr=0.5)
    assert pos.sl_price == pytest.approx(101.5)
    # трейлинг не тянет SL вниз
    monitor._update_sl(pos, atr=2.0)            # кандидат 103-6=97 — хуже
    assert pos.sl_price == pytest.approx(101.5)


def test_be_trigger_and_trail_sell_mirror(be_enabled):
    monitor = ExecutionMonitor(None)
    pos = make_buy_pos(direction="SELL", entry_price=100.0, sl_price=102.0,
                       initial_sl_price=102.0)
    pos.mfe_price = 98.0                        # r_peak = (100-98)/2 = 1.0 -> BE
    monitor._update_sl(pos, atr=None)
    buffer = 100.0 * (2 * config.COMMISSION_PER_SIDE + config.SLIPPAGE_PERCENT)
    assert pos.breakeven_moved
    assert pos.sl_price == pytest.approx(100.0 - buffer)
    # трейлинг SELL: SL = mfe + 3*atr, только ужесточение и не хуже BE-уровня
    monitor._update_sl(pos, atr=0.3)            # кандидат 98+0.9=98.9 < 99.88
    assert pos.sl_price == pytest.approx(98.9)


def test_be_disabled_keeps_original_sl():
    """BE_TRAIL_ENABLED=False (по умолчанию): SL не двигается вообще."""
    monitor = ExecutionMonitor(None)
    pos = make_buy_pos()
    pos.mfe_price = 105.0
    monitor._update_sl(pos, atr=0.5)
    assert pos.sl_price == 98.0 and not pos.breakeven_moved


def test_check_all_classifies_sl_be_trail(be_enabled):
    monitor = ExecutionMonitor(None)
    # штатный SL
    pos1 = make_buy_pos(figi="F1")
    monitor.positions[pos1.figi] = pos1
    to_close = run(monitor.check_all({pos1.figi: 97.9}))
    assert to_close[0].close_reason == "SL"
    # BE: SL на уровне entry+buffer, цена чуть ниже
    pos2 = make_buy_pos(figi="F2", breakeven_moved=True)
    pos2.sl_price = 100.0 + 100.0 * (2 * config.COMMISSION_PER_SIDE + config.SLIPPAGE_PERCENT)
    monitor.positions[pos2.figi] = pos2
    to_close = run(monitor.check_all({pos2.figi: pos2.sl_price - 0.01}))
    assert to_close[0].close_reason == "BE"
    # TRAIL: SL заметно выше уровня безубытка
    pos3 = make_buy_pos(figi="F3", breakeven_moved=True)
    pos3.sl_price = 102.0
    monitor.positions[pos3.figi] = pos3
    to_close = run(monitor.check_all({pos3.figi: 101.9}))
    assert to_close[0].close_reason == "TRAIL"


def test_check_all_old_signature_still_works():
    """Обратная совместимость: вызов с одним аргументом (старые тесты/код)."""
    monitor = ExecutionMonitor(None)
    pos = make_buy_pos()
    monitor.positions[pos.figi] = pos
    to_close = run(monitor.check_all({pos.figi: 97.9}))
    assert to_close[0].close_reason == "SL"
    assert monitor.monitoring_error is False


def test_check_all_updates_extremes_before_sl(be_enabled):
    """Экстремумы применяются ДО триггера BE/SL/TP в том же цикле."""
    monitor = ExecutionMonitor(None)
    pos = make_buy_pos()  # tp=104
    monitor.positions[pos.figi] = pos
    run(monitor.check_all({pos.figi: 104.5}, extremes={pos.figi: (105.0, 99.5)}))
    assert pos.mfe_price == 105.0 and pos.mae_price == 99.5
    assert pos.state == PositionState.CLOSING and pos.close_reason == "TP"


# ---------- logger: авто-миграция CSV и build_version ----------

OLD_FIELDS = [
    "trade_id", "timestamp_utc", "ticker", "figi", "lot_size",
    "side", "signal_reason", "signal_time",
    "price_before_order", "order_send_time", "order_id",
    "execution_status", "requested_lots", "executed_lots",
    "execution_price", "exit_reason", "exit_price", "exit_time",
    "commission_rub", "commission_source", "actual_commission_rub",
    "entry_commission_rub", "exit_commission_rub",
    "slippage_rub", "slippage_percent",
    "gross_pnl_rub", "net_pnl_rub", "cumulative_net_pnl_rub",
    "error_note",
]


def test_csv_migration_preserves_old_journal(isolated_paths):
    path = research_logger.TRADES_FILE
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OLD_FIELDS, delimiter=";")
        writer.writeheader()
        writer.writerow({"trade_id": "old1", "ticker": "OLD", "net_pnl_rub": "-5.00"})
    assert research_logger._ensure_files()
    # старый файл переименован в .bak и содержит данные
    backups = [f for f in os.listdir(isolated_paths) if f.endswith(".bak.csv")]
    assert len(backups) == 1
    with open(os.path.join(isolated_paths, backups[0]), encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    assert rows[0]["trade_id"] == "old1"
    # новый файл — с актуальной шапкой
    with open(path, encoding="utf-8") as f:
        assert f.readline().rstrip("\r\n") == ";".join(research_logger.TRADE_FIELDS)


def test_csv_same_header_not_migrated(isolated_paths):
    path = research_logger.TRADES_FILE
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=research_logger.TRADE_FIELDS, delimiter=";")
        writer.writeheader()
    assert research_logger._ensure_files()
    assert not [f for f in os.listdir(isolated_paths) if f.endswith(".bak.csv")]
    with open(path, encoding="utf-8") as f:
        rows = list(csv.reader(f, delimiter=";"))
    assert len(rows) == 1  # файл не пересоздан


def test_log_event_contains_build_version(isolated_paths):
    assert research_logger.log_event("TEST_EVENT", {"x": 1})
    with open(research_logger.EVENTS_FILE, encoding="utf-8") as f:
        rec = json.loads(f.readline())
    assert rec["build_version"] == config.BUILD_VERSION
