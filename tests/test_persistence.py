# tests/test_persistence.py
# Tsclp_v2.2_volatile — изолированные тесты persistence (state.json).
#
# ВАЖНО:
#   - Импортирует main, но НЕ вызывает main().
#   - Не подключается к Sandbox, не создаёт сетевые клиенты.
#   - Не создаёт настоящий trading.flag.
#   - Все операции с state.json — только в tmp_path.
#   - Не меняет рабочие файлы.
#
# Тесты:
#   1. _save_state: успешное сохранение (файл создан, JSON валиден).
#   2. _load_state: корректное восстановление позиций и счётчиков.
#   3. Аварийное завершение subprocess после сохранения.
#   4. Восстановление во втором процессе (subprocess).
#   5. Повреждённый JSON -> _load_state = False, блокировка.
#   6. _load_state = False при отсутствии файла.
#   7. Нет автоматического восстановления из портфеля (reconcile не вызывается).
#   8. Ошибка атомарной записи -> _save_state = False (не успех).

import json
import os
import subprocess
import sys
import textwrap

import pytest
from decimal import Decimal

import main
from execution import ExecutionMonitor, PositionState
from risk_manager import RiskManager
from tinkoff_client import PositionData


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


def make_position(lots, direction="LONG"):
    return PositionData(lots=lots, direction=direction, raw_shares=Decimal(lots))


def register_buy(mon, figi="FIGI", qty=1):
    return mon.register(
        figi=figi, ticker="T", direction="BUY",
        entry_price=100.0, quantity=qty, sl_price=99.0, tp_price=102.0,
        lot_size=1, trade_id="tid-" + figi,
        entry_requested_lots=qty,
    )


@pytest.fixture
def state_paths(tmp_path, monkeypatch):
    """Перенаправляет STATE_FILE / STATE_FILE_TMP в tmp_path."""
    sf = tmp_path / "state.json"
    sft = tmp_path / "state.json.tmp"
    monkeypatch.setattr(main, "STATE_FILE", str(sf))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(sft))
    return sf, sft


# ------------------------------------------------------------
# 1. _save_state: успешное сохранение
# ------------------------------------------------------------

def test_save_state_creates_file(state_paths):
    sf, sft = state_paths
    client = FakeClient()
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=3)
    rm = RiskManager(client)
    rm.daily_trade_counts = {"F1": 2}

    ok = main._save_state(mon, rm)
    assert ok is True
    assert os.path.isfile(sf)
    assert not os.path.isfile(sft)  # tmp удалён после os.replace

    with open(sf, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert "positions" in data
    assert "F1" in data["positions"]
    assert data["positions"]["F1"]["quantity"] == 3
    assert data["risk"]["daily_trade_counts"] == {"F1": 2}


# ------------------------------------------------------------
# 2. _load_state: корректное восстановление
# ------------------------------------------------------------

def test_load_state_restores_positions_and_counts(state_paths):
    sf, _ = state_paths
    client = FakeClient()
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=5)
    register_buy(mon, figi="F2", qty=2)
    rm = RiskManager(client)
    rm.daily_trade_counts = {"F1": 3, "F2": 1}

    assert main._save_state(mon, rm) is True

    mon2 = ExecutionMonitor(client)
    rm2 = RiskManager(client)
    ok = main._load_state(mon2, rm2)
    assert ok is True
    assert mon2.get_position("F1").quantity == 5
    assert mon2.get_position("F2").quantity == 2
    assert rm2.daily_trade_counts == {"F1": 3, "F2": 1}


# ------------------------------------------------------------
# 3. Аварийное завершение subprocess после сохранения
# ------------------------------------------------------------

def test_subprocess_saves_state_then_killed(state_paths, tmp_path):
    """
    Отдельный процесс: сохраняет state.json, затем os._exit(1) (аварийное
    завершение). Проверяем, что файл на диске валиден.
    """
    sf, sft = state_paths
    script = textwrap.dedent(f"""
        import json, os, sys
        sys.path.insert(0, r"{os.getcwd()}")
        from decimal import Decimal
        import main
        from execution import ExecutionMonitor
        from risk_manager import RiskManager
        from tinkoff_client import PositionData

        main.STATE_FILE = r"{sf}"
        main.STATE_FILE_TMP = r"{sft}"

        class C:
            uncertain_state = False
        mon = ExecutionMonitor(C())
        mon.register(figi="F1", ticker="T", direction="BUY",
                     entry_price=100.0, quantity=7, sl_price=99.0, tp_price=102.0,
                     lot_size=1, trade_id="t", entry_requested_lots=7)
        rm = RiskManager(C())
        rm.daily_trade_counts = {{"F1": 4}}
        ok = main._save_state(mon, rm)
        if not ok:
            sys.exit(2)
        # Аварийное завершение без очистки
        os._exit(1)
    """)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 1  # os._exit(1)
    assert os.path.isfile(sf)

    with open(sf, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["positions"]["F1"]["quantity"] == 7
    assert data["risk"]["daily_trade_counts"] == {"F1": 4}


# ------------------------------------------------------------
# 4. Восстановление во втором процессе (subprocess)
# ------------------------------------------------------------

def test_second_process_restores_state(state_paths):
    """
    Первый процесс сохраняет state.json.
    Второй процесс загружает его и выводит результат.
    """
    sf, sft = state_paths

    # Первый процесс — сохранение
    save_script = textwrap.dedent(f"""
        import sys, os
        sys.path.insert(0, r"{os.getcwd()}")
        import main
        from execution import ExecutionMonitor
        from risk_manager import RiskManager

        main.STATE_FILE = r"{sf}"
        main.STATE_FILE_TMP = r"{sft}"

        class C:
            uncertain_state = False
        mon = ExecutionMonitor(C())
        mon.register(figi="F1", ticker="T", direction="BUY",
                     entry_price=100.0, quantity=7, sl_price=99.0, tp_price=102.0,
                     lot_size=1, trade_id="t", entry_requested_lots=7)
        rm = RiskManager(C())
        rm.daily_trade_counts = {{"F1": 4}}
        ok = main._save_state(mon, rm)
        sys.exit(0 if ok else 3)
    """)
    r1 = subprocess.run([sys.executable, "-c", save_script],
                        capture_output=True, text=True, timeout=30)
    assert r1.returncode == 0, r1.stderr

    # Второй процесс — восстановление
    load_script = textwrap.dedent(f"""
        import sys, os
        sys.path.insert(0, r"{os.getcwd()}")
        import main
        from execution import ExecutionMonitor
        from risk_manager import RiskManager

        main.STATE_FILE = r"{sf}"

        class C:
            uncertain_state = False
        mon = ExecutionMonitor(C())
        rm = RiskManager(C())
        ok = main._load_state(mon, rm)
        if not ok:
            sys.exit(4)
        q = mon.get_position("F1").quantity
        c = rm.daily_trade_counts.get("F1")
        print(f"OK {{q}} {{c}}")
        sys.exit(0)
    """)
    r2 = subprocess.run([sys.executable, "-c", load_script],
                        capture_output=True, text=True, timeout=30)
    assert r2.returncode == 0, r2.stderr
    assert "OK 7 4" in r2.stdout


# ------------------------------------------------------------
# 5. Повреждённый JSON -> _load_state = False
# ------------------------------------------------------------

def test_load_state_corrupted_json(state_paths):
    sf, _ = state_paths
    with open(sf, "w", encoding="utf-8") as f:
        f.write("{not valid json")
    client = FakeClient()
    mon = ExecutionMonitor(client)
    rm = RiskManager(client)
    ok = main._load_state(mon, rm)
    assert ok is False
    assert mon.positions == {}


# ------------------------------------------------------------
# 6. _load_state = False при отсутствии файла
# ------------------------------------------------------------

def test_load_state_missing_file(state_paths):
    sf, _ = state_paths
    assert not os.path.isfile(sf)
    client = FakeClient()
    mon = ExecutionMonitor(client)
    rm = RiskManager(client)
    ok = main._load_state(mon, rm)
    assert ok is False


# ------------------------------------------------------------
# 7. Блокировка при ошибке загрузки
# ------------------------------------------------------------

def test_load_state_false_blocks_entries(state_paths):
    """
    _load_state = False -> в main() это означает uncertain_state=True,
    новые входы запрещены. Проверяем контракт: _load_state возвращает False,
    и это должно блокировать входы (в main() — установкой uncertain_state).
    """
    sf, _ = state_paths
    with open(sf, "w", encoding="utf-8") as f:
        f.write("corrupted")
    client = FakeClient()
    mon = ExecutionMonitor(client)
    rm = RiskManager(client)
    ok = main._load_state(mon, rm)
    assert ok is False
    # Контракт main(): при False -> client.uncertain_state = True, входы запрещены
    # (сам main() не вызываем, проверяем логику через _load_state)


# ------------------------------------------------------------
# 8. Нет автоматического восстановления из портфеля
# ------------------------------------------------------------

def test_no_auto_restore_from_portfolio(state_paths):
    """
    При отсутствии state.json _load_state = False.
    main() НЕ создаёт позиции из брокерского портфеля автоматически
    (reconcile вызывается отдельно, но не заполняет monitor.positions).
    Проверяем: _load_state не трогает monitor.positions, даже если
    у клиента есть позиции.
    """
    sf, _ = state_paths
    assert not os.path.isfile(sf)

    class ClientWithPositions:
        uncertain_state = False
        async def get_positions(self):
            return {"F1": make_position(5)}

    client = ClientWithPositions()
    mon = ExecutionMonitor(client)
    rm = RiskManager(client)
    ok = main._load_state(mon, rm)
    assert ok is False
    # monitor.positions не заполнен из портфеля
    assert mon.positions == {}


# ------------------------------------------------------------
# 9. Ошибка атомарной записи -> _save_state = False
# ------------------------------------------------------------

def test_save_state_write_error_returns_false(tmp_path, monkeypatch):
    """Если STATE_FILE_TMP недоступен для записи -> _save_state = False."""
    bad_dir = tmp_path / "no_such_dir"
    sf = bad_dir / "state.json"
    sft = bad_dir / "state.json.tmp"
    monkeypatch.setattr(main, "STATE_FILE", str(sf))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(sft))

    client = FakeClient()
    mon = ExecutionMonitor(client)
    register_buy(mon, figi="F1", qty=1)
    rm = RiskManager(client)

    ok = main._save_state(mon, rm)
    assert ok is False
    assert not os.path.isfile(sf)