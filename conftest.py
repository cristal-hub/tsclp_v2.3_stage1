# tests/conftest.py
# Tsclp_v2.2_volatile — общая изоляция тестовой среды (Проблема №5 ТЗ).
#
# НАЗНАЧЕНИЕ:
#   Автоматическая (autouse) изоляция файловых побочных эффектов логгера:
#   research_events.jsonl (append-only производственный журнал) и
#   research_trades.csv должны оставаться неизменными при ЛЮБОМ запуске
#   pytest. Раньше тесты, не использовавшие fixture env из
#   test_p0_defect_fixes.py (например, тесты reconcile в test_main.py,
#   test_main_helpers.py), дописывали диагностические события в
#   ПРОИЗВОДСТВЕННЫЙ журнал в корне репозитория.
#
# МЕХАНИЗМ:
#   monkeypatch.setattr для research_logger.EVENTS_FILE / TRADES_FILE на
#   файлы в tmp_path каждого теста. Значения восстанавливаются pytest
#   автоматически после каждого теста. Тесты, которые задают эти пути
#   сами (fixture env, тесты test_fail_closed.py / test_main_helpers.py),
#   не конфликтуют: monkeypatch-подмены корректно накладываются,
#   последний setattr внутри теста действует в его течение.
#
# ЧТО НЕ ДЕЛАЕТ:
#   - не меняет production-код (main.py / logger.py не тронуты);
#   - не отключает и не ослабляет ни одну проверку;
#   - не влияет на subprocess-тесты (child-процесс наследует только явно
#     переданные пути STATE_FILE; журнал в child не пишется).
#
# ГАРАНТИЯ: после прогона pytest содержимое research_events.jsonl и
# research_trades.csv в корне репозитория не изменяется (критерий 14 ТЗ).

import pytest

import logger as research_logger


@pytest.fixture(autouse=True)
def _isolate_research_log_files(tmp_path, monkeypatch):
    """Перенаправляет журнал событий и CSV сделок в tmp_path на время теста."""
    monkeypatch.setattr(
        research_logger, "EVENTS_FILE", str(tmp_path / "research_events.jsonl")
    )
    monkeypatch.setattr(
        research_logger, "TRADES_FILE", str(tmp_path / "research_trades.csv")
    )
    yield
