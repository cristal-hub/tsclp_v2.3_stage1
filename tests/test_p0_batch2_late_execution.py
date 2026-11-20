# tests/test_p0_batch2_late_execution.py
# Tsclp_v2.2_volatile — тесты партии P0-2: поздние исполнения и промоция RECOVERY.
#   + партия P0-3: разделение «остаток позиции» (pos.quantity, достоверный
#   снимок портфеля) и «накопительное исполнение BUY-заявки» (lots_executed,
#   entry_applied_lots/executed_lots).
#
# Соответствие дефект ↔ тест:
#   Дефект «преждевременный RECOVERY -> OPEN, пока заявка может получить
#       дополнительные исполнения»:
#       test_n1_no_premotion_while_order_alive
#       test_n1_promotion_only_at_terminal_syncs_final_quantity
#   Дефект «обновление количества, если поздние лоты поступили после OPEN»:
#       test_n2_open_position_late_lots_increase_no_double
#       test_n2_open_position_shrink_is_contradiction
#   Дефект «исполнение с пустым FIGI как обычная BUY-позиция»:
#       test_n3_unknown_figi_execution_not_registered_global_block
#       test_n3_register_guard_direct_returns_none
#   Дефект «позднее исполнение после терминального CANCELLED: сверка не
#       запускалась»:
#       test_ledger_open_orders_includes_pending_residual
#       test_n4_late_growth_after_terminal_cancelled_actually_repolled
#       test_n4_no_resurrection_after_position_closed
#   Интеграционный сценарий trading_cycle
#   (частичное исполнение -> отмена -> RECOVERY -> OPEN -> поздние лоты):
#       test_integration_trading_cycle_partial_cancel_recovery_open_late_lots
#
#   Партия P0-3 (обязательные три теста постановки):
#   Дефект «повторный ответ старой BUY-заявки восстанавливает проданные
#       лоты / устаревший ответ уменьшает позицию»:
#       test_p03_1_repeated_buy_report_does_not_restore_sold_lots  (Тест 1)
#       test_p03_2_stale_buy_report_contradiction_blocks_buy       (Тест 2)
#       test_p03_3_reliable_snapshot_can_decrease_position         (Тест 3)
#
#   Партия P0-4 (гонка reconcile() -> poll_open_orders() в RECOVERY):
#   Дефект «позднее исполнение, уже отражённое в достоверном снимке
#       портфеля, повторно прибавлялось к остатку (7 -> 9 вместо 7)»:
#       test_p04_reconcile_before_poll_no_reapply_of_late_lots           (интеграционный)
#       test_p04_growth_report_after_snapshot_sync_not_added_to_quantity (юнит)
#
#   Партия P0-5 (недопущение снятия UNCERTAIN повторным опросом):
#   Дефект «повторный опрос с тем же lots_executed перезаписывал
#       UNCERTAIN-запись реестра терминальным статусом (CANCELLED/EXPIRED/
#       FILL) и снимал блокировку новых BUY по FIGI до подтверждённой
#       сверки портфеля»:
#       test_p05_1_first_ambiguous_growth_sets_uncertain
#       test_p05_2_repeated_same_lots_poll_keeps_uncertain
#       test_p05_3_failed_or_contradictory_reconcile_keeps_uncertain
#       test_p05_4_successful_reconcile_allows_resolution_no_side_effects
#
# Тесты проверяют ПОСЛЕДСТВИЯ: появилось ли количество в позиции, изменились
# ли SL/TP, создан ли дубликат, заблокированы ли новые BUY, запустилась ли
# повторная сверка, не «воскрешена» ли закрытая позиция.

import json
from decimal import Decimal

import pytest

import main
from execution import ExecutionMonitor, PositionState
from tinkoff_client import PositionData

from tests.test_p0_defect_fixes import (
    FakeClient, FakeRM, FakeSM,
    fake_state, make_ledger, small_df, env,  # env — общий fixture изоляции
)
from tests.test_p0_defect_fixes import (
    FILL, REJECTED, CANCELLED, NEW, PARTIALLYFILL, EXPIRED, FIGI, TICKER,
)

ACTIVE = (PositionState.OPEN, PositionState.CLOSING,
          PositionState.UNCERTAIN, PositionState.RECOVERY)


def active_positions(monitor):
    return [p for p in monitor.positions.values() if p.state in ACTIVE]


# ============================================================
# Дефект: сверка после терминального CANCELLED не запускалась
# (механика order_ledger.open_orders)
# ============================================================

def test_ledger_open_orders_includes_pending_residual(tmp_path):
    """Терминальные заявки С ИСПОЛНЕНИЕМ остаются доступны для повторной
    сверки; терминальные без исполнения и полный FILL — закрыты."""
    ledger = make_ledger(tmp_path)
    # терминальная с исполнением — residual-сверка
    ledger.append("CANCELLED", "o1", figi=FIGI, side="BUY",
                  requested_lots=10, executed_lots=3)
    # терминальная без исполнения — достоверно разрешена
    ledger.append("CANCELLED", "o2", figi=FIGI, side="BUY",
                  requested_lots=10, executed_lots=0)
    # полный FILL — достоверно разрешён
    ledger.append("FILL", "o3", figi=FIGI, side="BUY",
                  requested_lots=5, executed_lots=5)
    # FILL с исполнением меньше запрошенного — рост ещё возможен (защитно)
    ledger.append("FILL", "o4", figi=FIGI, side="BUY",
                  requested_lots=5, executed_lots=2)
    # активная заявка — прежнее поведение
    ledger.append("NEW", "o5", figi=FIGI, side="BUY",
                  requested_lots=5, executed_lots=0)

    ids = {r["order_id"] for r in ledger.open_orders()}
    assert {"o1", "o4", "o5"} <= ids, "заявка с residual-исполнением недоступна для сверки"
    assert "o2" not in ids, "терминальная без исполнения не должна опрашиваться"
    assert "o3" not in ids, "полный FILL не должен опрашиваться"
    assert {r["order_id"] for r in ledger.open_orders(side="BUY")} == {"o1", "o4", "o5"}


# ============================================================
# Дефект: преждевременный RECOVERY -> OPEN
# ============================================================

@pytest.mark.asyncio
async def test_n1_no_premotion_while_order_alive(env):
    """RECOVERY не повышается до OPEN, пока входная заявка активна
    (PARTIALLYFILL): возможны дополнительные исполнения. Цена, уже видная
    у живой заявки, не является основанием для промоции."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={FIGI: PositionData(lots=2, direction="LONG",
                                                      raw_shares=Decimal(2))})
    oid = "oid-alive"
    monitor = ExecutionMonitor(client)
    pos = monitor.register_recovery(figi=FIGI, ticker=TICKER, direction="BUY",
                                    quantity=2, lot_size=1, order_id=oid,
                                    recovery_source="no_price")

    # у живой заявки УЖЕ есть цена исполнения — промоция всё равно запрещена
    client.order_states[oid] = fake_state(PARTIALLYFILL, lots_executed=2, price_units=200)
    await main.promote_recovery_positions(client, monitor, FakeRM())
    assert pos.state == PositionState.RECOVERY, \
        "преждевременный RECOVERY->OPEN при активной заявке (возможны доп. исполнения)"

    # статус недоступен — тоже без промоции
    client.order_states[oid] = None
    await main.promote_recovery_positions(client, monitor, FakeRM())
    assert pos.state == PositionState.RECOVERY


@pytest.mark.asyncio
async def test_n1_promotion_only_at_terminal_syncs_final_quantity(env):
    """При ТЕРМИНАЛЬНОМ статусе заявки промоция выполняется. Партия P0-3:
    финальное накопительное lots_executed не подставляется в количество как
    готовое значение — применяются только ДЕЙСТВИТЕЛЬНО новые лоты сверх
    учтённых (2 учтено, исполнено 5 -> +3, итог 5). Проданные лоты при этом
    не восстанавливаются: повторный ответ с тем же значением ничего не меняет
    (см. test_p03_1)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-terminal"
    monitor = ExecutionMonitor(client)
    pos = monitor.register_recovery(figi=FIGI, ticker=TICKER, direction="BUY",
                                    quantity=2, lot_size=1, order_id=oid,
                                    recovery_source="no_price")

    # финальный отчёт: CANCELLED, исполнено 5 (больше известного 2),
    # средняя цена 500 за 5 лотов = 100 за штуку
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=5, price_units=500)
    await main.promote_recovery_positions(client, monitor, FakeRM(sl=95.0, tp=110.0))

    assert pos.state == PositionState.OPEN, "промоция не выполнена при терминальном статусе"
    assert pos.quantity == 5, "количество не доуточнено по финальному lots_executed"
    assert pos.entry_price == 100.0
    assert pos.sl_price == 95.0 and pos.tp_price == 110.0


# ============================================================
# Дефект: поздние лоты после перехода позиции в OPEN
# ============================================================

async def _open_position(env, ledger, client, oid, qty):
    monitor = ExecutionMonitor(client)
    pos = await main.register_executed_buy(
        client, monitor, FakeRM(sl=95.0, tp=110.0), ledger,
        figi=FIGI, ticker=TICKER, order_id=oid, trade_id="t-late",
        reason="unit", signal_time="now", price_before_order=100.0,
        order_send_time="now", requested_lots=10, executed_lots=qty,
        exec_price=100.0, lot_size=1, partial=False,
        df=small_df(), source="test", ledger_status="FILL",
    )
    return monitor, pos


@pytest.mark.asyncio
async def test_n2_open_position_late_lots_increase_no_double(env):
    """Поздние дополнительные лоты ПОСЛЕ перехода в OPEN: количество растёт
    точно на дельту, SL/TP не пересчитываются, повторный опрос не удваивает."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-late-open"
    monitor, pos = await _open_position(env, ledger, client, oid, qty=2)
    assert pos.state == PositionState.OPEN and pos.quantity == 2
    entry_price = pos.entry_price
    sl, tp = pos.sl_price, pos.tp_price

    # поздний рост: отчёт брокера обновился после терминального статуса
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=5, price_units=500)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)

    assert len(active_positions(monitor)) == 1, "дубликат позиции при позднем исполнении"
    pos = monitor.get_position(FIGI)
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 5, "поздние лоты не применены к OPEN-позиции"
    assert pos.entry_applied_lots == 5
    assert pos.entry_price == entry_price, "цена входа изменилась"
    assert pos.sl_price == sl and pos.tp_price == tp, "SL/TP пересчитаны (стратегия не меняется)"

    # повторный опрос того же состояния — без повторного прибавления
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert monitor.get_position(FIGI).quantity == 5, "лоты прибавлены повторно"
    assert len(active_positions(monitor)) == 1


@pytest.mark.asyncio
async def test_n2_open_position_shrink_is_contradiction(env):
    """Уменьшение lots_executed против уже применённого — противоречие:
    исполнение не убывает, количество не меняется."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-shrink"
    monitor, pos = await _open_position(env, ledger, client, oid, qty=5)
    assert pos.quantity == 5 and pos.entry_applied_lots == 5

    client.order_states[oid] = fake_state(CANCELLED, lots_executed=3, price_units=300)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert monitor.get_position(FIGI).quantity == 5, "исполнение не должно убывать"
    assert monitor.get_position(FIGI).entry_applied_lots == 5
    assert len(active_positions(monitor)) == 1


# ============================================================
# Дефект: исполнение с пустым FIGI
# ============================================================

@pytest.mark.asyncio
async def test_n3_unknown_figi_execution_not_registered_global_block(env):
    """Подтверждённое исполнение с пустым FIGI НЕ регистрируется как обычная
    BUY-позиция: заявка остаётся UNCERTAIN, новые BUY запрещены ГЛОБАЛЬНО
    (в торговом цикле), существующие позиции продолжают мониториться."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-nofigi"
    ledger.record_intent(oid, trade_id="t-nofigi", figi="", ticker=TICKER,
                         side="BUY", requested_lots=5)
    client.order_states[oid] = fake_state(FILL, lots_executed=4, price_units=400)
    monitor = ExecutionMonitor(client)
    monitor.register(figi="BBG00EXISTING", ticker="EXIST", direction="BUY",
                     entry_price=50.0, quantity=1, sl_price=49.0, tp_price=52.0,
                     lot_size=1, trade_id="t-exist", entry_requested_lots=1)

    await main.poll_open_orders(client, monitor, FakeRM(), ledger)

    # ПОСЛЕДСТВИЯ: позиции с пустым FIGI НЕТ
    assert len(monitor.positions) == 1, "исполнение с пустым FIGI зарегистрировано как позиция"
    assert monitor.get_position("") is None
    assert monitor.get_position(None) is None
    assert client.uncertain_state is True
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN", f"статус заявки: {rec['status']}"
    assert ledger.has_unknown_figi_open_orders() is True, "глобальная блокировка не активна"
    # существующая позиция продолжается мониториться
    assert monitor.get_position("BBG00EXISTING").state == PositionState.OPEN

    # глобальная блокировка действует в торговом цикле (даже после reconcile)
    main.ledger_ref = ledger
    rm, sm = FakeRM(), FakeSM("BUY")
    main.top_instruments.clear()
    main.top_instruments.extend([{"figi": FIGI, "ticker": TICKER}])
    client.prices_value = {FIGI: 100.0}
    await main.trading_cycle(client, rm, sm, monitor, ledger)
    assert sm.calls == 0, "новые BUY разрешены при заявке без FIGI"
    assert client.last_post is None


@pytest.mark.asyncio
async def test_n3_register_guard_direct_returns_none(env):
    """Прямой вызов единой точки регистрации с пустым FIGI не создаёт
    позицию и включает глобальную блокировку."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient()
    monitor = ExecutionMonitor(client)
    pos = await main.register_executed_buy(
        client, monitor, FakeRM(), ledger,
        figi="", ticker=TICKER, order_id="oid-nofigi-direct", trade_id="t-nf",
        reason="unit", signal_time="now", price_before_order=100.0,
        order_send_time="now", requested_lots=4, executed_lots=4,
        exec_price=100.0, lot_size=1, partial=False,
        df=small_df(), source="test",
    )
    assert pos is None, "позиция с пустым FIGI зарегистрирована"
    assert len(monitor.positions) == 0
    assert client.uncertain_state is True
    rec = ledger.latest("oid-nofigi-direct")
    assert rec is not None and rec["status"] == "UNCERTAIN"
    assert ledger.has_unknown_figi_open_orders() is True


# ============================================================
# Дефект: позднее исполнение после терминального CANCELLED —
# сверка реально запускается
# ============================================================

@pytest.mark.asyncio
async def test_n4_late_growth_after_terminal_cancelled_actually_repolled(env):
    """После регистрации исполнения по терминальному CANCELLED повторная
    сверка ПРОДОЛЖАЕТСЯ: поздний рост 3 -> 8 лотов обнаруживается опросом
    и применяется к RECOVERY-позиции без дубликатов и повторного счёта."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-residual"
    ledger.record_intent(oid, trade_id="t-r", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    monitor = ExecutionMonitor(client)

    # фаза 1: терминальный CANCELLED с исполнением 3 -> RECOVERY qty=3
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=3, price_units=300)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos is not None and pos.quantity == 3 and pos.state == PositionState.RECOVERY
    assert ledger.latest(oid)["status"] == "CANCELLED"

    # фаза 2: поздний рост исполнения до 8 — сверка обязана запуститься
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=8, price_units=800)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert len(active_positions(monitor)) == 1, "дубликат при позднем исполнении после CANCELLED"
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 8, "поздний рост после терминального CANCELLED не обнаружен сверкой"
    assert pos.state == PositionState.RECOVERY
    rec = ledger.latest(oid)
    assert rec["executed_lots"] == 8

    # фаза 3: состояние без изменений — учёт не меняется
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert monitor.get_position(FIGI).quantity == 8
    assert len(active_positions(monitor)) == 1


@pytest.mark.asyncio
async def test_n4_no_resurrection_after_position_closed(env):
    """Закрытая позиция не «воскрешается» повторной регистрацией исполненного;
    поздний рост при закрытой позиции разрешается СВЕРКОЙ (адопция по
    достоверному снимку брокера показывает фактический остаток)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-closed"
    monitor, pos = await _open_position(env, ledger, client, oid, qty=3)
    assert pos.state == PositionState.OPEN and pos.quantity == 3
    pos.state = PositionState.CLOSED
    pos.quantity = 0

    # тот же отчёт брокера (3 == учтённым 3): повторная регистрация запрещена
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=3, price_units=300)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert active_positions(monitor) == [], "закрытая позиция «воскрешена»"
    assert monitor.get_position(FIGI).state == PositionState.CLOSED

    # поздний рост (8 > 3): авторегистрация по отчёту заявки не выполняется,
    # включается неопределённость -> разрешается сверкой
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=8, price_units=800)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert client.uncertain_state is True, "поздний рост при закрытой позиции не обнаружен"
    assert active_positions(monitor) == [], "позиция зарегистрирована по отчёту заявки вместо сверки"

    # сверка по ДОСТОВЕРНОМУ снимку брокера: фактический остаток 5 (8-3 продано)
    client.positions_value = {FIGI: PositionData(lots=5, direction="LONG",
                                                 raw_shares=Decimal(5))}
    await main.reconcile(client, monitor, ledger, FakeRM())
    adopted = monitor.get_position(FIGI)
    assert adopted is not None and adopted.state == PositionState.RECOVERY
    assert adopted.quantity == 5, "адопция не по фактическому остатку брокера"


# ============================================================
# Интеграция: trading_cycle
# частичное исполнение -> отмена -> RECOVERY -> OPEN -> поздние лоты
# ============================================================

@pytest.mark.asyncio
async def test_integration_trading_cycle_partial_cancel_recovery_open_late_lots(env):
    """Полная последовательность торгового цикла:
    1) BUY с частичным исполнением (2 из 5), отмена остатка, цена недоступна
       -> RECOVERY qty=2;
    2) заявка ещё жива (NEW с ценой) -> промоция НЕ выполняется;
    3) заявка терминальна (CANCELLED, цена доступна) -> RECOVERY -> OPEN;
    4) поздний рост исполнения 2 -> 5 -> количество OPEN-позиции растёт до 5,
       SL/TP сохранены, дубликатов нет."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM(sl=95.0, tp=110.0, size=5)
    sm = FakeSM("BUY")
    main.monitor_ref = monitor
    main.ledger_ref = ledger

    # --- Фаза 1: вход с частичным исполнением, отмена остатка, без цены ---
    main.top_instruments.clear()
    main.top_instruments.extend([{"figi": FIGI, "ticker": TICKER}])
    client.prices_value = {FIGI: 100.0}
    client.post_state = fake_state(PARTIALLYFILL, lots_executed=2)
    client.final_state = fake_state(CANCELLED, lots_executed=2, price_units=0)  # цены нет
    await main.trading_cycle(client, rm, sm, monitor, ledger)

    oid = client.last_post["order_id"]
    pos = monitor.get_position(FIGI)
    assert pos is not None, "исполнение потеряно"
    assert pos.state == PositionState.RECOVERY, "без цены позиция должна быть в RECOVERY"
    assert pos.quantity == 2
    assert pos.entry_order_id == oid
    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED" and rec["executed_lots"] == 2

    # --- Фаза 2: заявка ещё жива (NEW с ценой) — промоции НЕТ ---
    client.positions_value = {FIGI: PositionData(lots=2, direction="LONG",
                                                 raw_shares=Decimal(2))}
    client.order_states[oid] = fake_state(NEW, lots_executed=2, price_units=200)
    await main.trading_cycle(client, rm, sm, monitor, ledger)
    pos = monitor.get_position(FIGI)
    assert pos.state == PositionState.RECOVERY, "промоция при живой заявке"
    assert pos.quantity == 2

    # --- Фаза 3: заявка терминальна, цена доступна — RECOVERY -> OPEN ---
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=2, price_units=200)
    await main.trading_cycle(client, rm, sm, monitor, ledger)
    pos = monitor.get_position(FIGI)
    assert pos.state == PositionState.OPEN, "промоция не выполнена при терминальном статусе"
    assert pos.quantity == 2
    assert pos.entry_price == 100.0
    assert pos.sl_price == 95.0 and pos.tp_price == 110.0

    # --- Фаза 4: поздний рост исполнения 2 -> 5 (после OPEN) ---
    client.positions_value = {FIGI: PositionData(lots=5, direction="LONG",
                                                 raw_shares=Decimal(5))}
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=5, price_units=500)
    client.last_post = None  # отслеживаем только НОВЫЕ входы фазы 4
    await main.trading_cycle(client, rm, sm, monitor, ledger)
    pos = monitor.get_position(FIGI)
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 5, "поздние лоты не применены после OPEN"
    assert pos.entry_applied_lots == 5
    assert pos.entry_price == 100.0, "цена входа изменилась"
    assert pos.sl_price == 95.0 and pos.tp_price == 110.0, "SL/TP пересчитаны"
    assert len(active_positions(monitor)) == 1, "дубликат позиции"
    assert client.cancel_calls == [oid], "отмена остатка выполнена не один раз"
    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED" and rec["executed_lots"] == 5
    # новых входов по занятому инструменту нет
    assert client.last_post is None, "новый BUY отправлен при уже открытой позиции"


# ============================================================
# Партия P0-3: остаток позиции (достоверный снимок портфеля) и
# накопительное исполнение BUY-заявки (lots_executed) — разные величины.
# Обязательные три теста постановки.
# ============================================================

@pytest.mark.asyncio
async def test_p03_1_repeated_buy_report_does_not_restore_sold_lots(env):
    """P0-3 Тест 1. BUY-заявка суммарно исполнилась на 8 лотов (RECOVERY).
    Оператор вручную продал 3 лота: достоверная сверка портфеля уменьшает
    остаток до 5. Повторный ответ ТОЙ ЖЕ заявки (lots_executed=8) НЕ должен
    восстанавливать проданные лоты: quantity остаётся 5, накопительные
    счётчики исполнения остаются 8, новые лоты не добавляются."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-p03-1"
    ledger.record_intent(oid, trade_id="t-p03-1", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    monitor = ExecutionMonitor(client)

    # фаза 1: исполнение 8 лотов без свечей -> RECOVERY qty=8, учтено 8
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=8, price_units=800)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos is not None and pos.state == PositionState.RECOVERY
    assert pos.quantity == 8
    assert pos.entry_applied_lots == 8 and pos.executed_lots == 8

    # фаза 2: достоверная сверка брокерского портфеля: остаток 5 (продано 3)
    client.positions_value = {FIGI: PositionData(lots=5, direction="LONG",
                                                 raw_shares=Decimal(5))}
    await main.reconcile(client, monitor, ledger, FakeRM())
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 5, "достоверный снимок портфеля обязан управлять остатком"
    assert pos.entry_applied_lots == 8, "счётчик исполненных BUY-лотов подменён остатком позиции"
    assert pos.executed_lots == 8, "executed_lots превратился в остаток позиции"

    # фаза 3: повторный ответ той же заявки — снова lots_executed=8
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 5, "проданные лоты восстановлены повторным ответом заявки"
    assert pos.entry_applied_lots == 8, "счётчики изменены повторным ответом"
    assert pos.executed_lots == 8
    assert len(active_positions(monitor)) == 1, "дубликат позиции"
    assert client.uncertain_state is False, \
        "повторный ответ с тем же значением не должен давать противоречие"


@pytest.mark.asyncio
async def test_p03_2_stale_buy_report_contradiction_blocks_buy(env):
    """P0-3 Тест 2. RECOVERY-позиция: учтено 5 лотов (quantity=5).
    Устаревший ответ BUY-заявки сообщает lots_executed=3 (меньше учтённого).
    Ожидается: quantity не уменьшается, счётчики не уменьшаются,
    фиксируется неопределённость/противоречие существующей архитектурой
    (client.uncertain_state + UNCERTAIN-запись реестра), новые BUY по FIGI
    блокированы предусмотренным механизмом."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-p03-2"
    ledger.record_intent(oid, trade_id="t-p03-2", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    monitor = ExecutionMonitor(client)

    # фаза 1: учтено 5 лотов (RECOVERY), в реестре CANCELLED/5
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=5, price_units=500)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos is not None and pos.state == PositionState.RECOVERY
    assert pos.quantity == 5 and pos.entry_applied_lots == 5 and pos.executed_lots == 5
    assert ledger.latest(oid)["executed_lots"] == 5

    # фаза 2: устаревший ответ той же заявки: lots_executed=3
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=3, price_units=300)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)

    # ПОСЛЕДСТВИЯ
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 5, "устаревший ответ уменьшил количество позиции"
    assert pos.entry_applied_lots == 5, "накопительные счётчики уменьшены"
    assert pos.executed_lots == 5
    assert client.uncertain_state is True, "противоречие не зафиксировано"
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN", \
        f"противоречие не зафиксировано в реестре: {rec['status']}"
    assert ledger.figi_blocks_buy(FIGI) is True, "новые BUY не заблокированы"
    assert len(active_positions(monitor)) == 1


def test_p03_3_reliable_snapshot_can_decrease_position(env):
    """P0-3 Тест 3. Достоверная сверка брокерского портфеля ОБЯЗАНА уметь
    уменьшать остаток позиции (8 -> 5), не подменяя накопительные счётчики
    BUY-исполнения значением остатка: entry_applied_lots остаётся 8,
    executed_lots (накопительное исполнение BUY) не превращается в 5.
    Увеличение по достоверному снимку тоже работает (функция не монотонна).
    Сохранение/загрузка state переживает раунд-трип."""
    client = FakeClient()
    monitor = ExecutionMonitor(client)
    pos = monitor.register_recovery(figi=FIGI, ticker=TICKER, direction="BUY",
                                    quantity=8, lot_size=1,
                                    order_id="oid-p03-3",
                                    recovery_source="no_price")
    assert pos.quantity == 8
    assert pos.entry_applied_lots == 8 and pos.executed_lots == 8

    # достоверный снимок брокера: остаток 5 (оператор продал 3)
    assert monitor.sync_recovery_quantity(pos, 5) is True
    assert pos.quantity == 5, "достоверный снимок обязан уменьшить остаток"
    assert pos.entry_applied_lots == 8, \
        "накопительное исполнение BUY подменено остатком позиции"
    assert pos.executed_lots == 8, "executed_lots ошибочно стал равен остатку"
    assert pos.state == PositionState.RECOVERY
    assert pos.direction == "BUY"

    # идемпотентность: тот же снимок — без изменений
    assert monitor.sync_recovery_quantity(pos, 5) is False
    assert pos.quantity == 5 and pos.entry_applied_lots == 8

    # увеличение по достоверному снимку по-прежнему работает (не монотонно)
    assert monitor.sync_recovery_quantity(pos, 7) is True
    assert pos.quantity == 7
    assert pos.entry_applied_lots == 8, "счётчики исполнения не должны уменьшаться"

    # сохранение/повторная загрузка state: величины не смешиваются
    data = json.loads(json.dumps(monitor.export_state()))
    monitor2 = ExecutionMonitor(client)
    monitor2.import_state(data)
    pos2 = monitor2.get_position(FIGI)
    assert pos2 is not None and pos2.state == PositionState.RECOVERY
    assert pos2.quantity == 7
    assert pos2.entry_applied_lots == 8 and pos2.executed_lots == 8


# ============================================================
# Партия P0-4: гонка reconcile() -> poll_open_orders() в RECOVERY.
# Позднее исполнение, уже отражённое в достоверном снимке портфеля,
# не должно повторно прибавляться к остатку.
# ============================================================

@pytest.mark.asyncio
async def test_p04_reconcile_before_poll_no_reapply_of_late_lots(env):
    """P0-4, воспроизводимый сценарий постановки (§2).

    Исходное состояние RECOVERY: quantity=8, entry_applied_lots=8,
    executed_lots=8, в реестре исполнение 8 лотов. Далее:
      1) оператор вручную продаёт 3 лота, затем по отменённой BUY-заявке
         приходит позднее исполнение ещё 2 лотов: у брокера остаток 7,
         накопительное исполнение заявки — 10;
      2) в одном торговом цикле reconcile() выполняется РАНЬШЕ
         poll_open_orders(): сверка получает остаток 7 и обновляет RECOVERY;
      3) затем poll_open_orders() получает lots_executed=10.

    ОБЯЗАТЕЛЬНОЕ утверждение: локальный quantity == 7, а НЕ 9 — позднее
    исполнение уже отражено в брокерском остатке, повторно прибавлять
    его нельзя. Дополнительно: старое накопительное исполнение не
    восстанавливает проданные лоты; счётчики исполнения заявки догоняются
    по отчёту заявки; неоднозначность фиксируется существующей
    архитектурой (uncertain_state + UNCERTAIN в реестре, новые BUY по
    FIGI блокированы); повторный опрос того же ордера не меняет остаток;
    контрольная сверка подтверждает остаток."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    oid = "oid-p04-race"
    ledger.record_intent(oid, trade_id="t-p04", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    monitor = ExecutionMonitor(client)

    # --- Фаза 1: RECOVERY 8/8/8 (исполнение ДО ручной продажи) ---
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=8, price_units=800)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos is not None and pos.state == PositionState.RECOVERY
    assert pos.quantity == 8
    assert pos.entry_applied_lots == 8 and pos.executed_lots == 8
    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED" and rec["executed_lots"] == 8

    # --- Фаза 2: ручная продажа 3 лотов + позднее исполнение 2 лотов —
    # у брокера остаток 7 (уже включает поздние лоты). reconcile()
    # выполняется РАНЬШЕ poll_open_orders() ---
    client.positions_value = {FIGI: PositionData(lots=7, direction="LONG",
                                                 raw_shares=Decimal(7))}
    await main.reconcile(client, monitor, ledger, FakeRM())
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "достоверный снимок портфеля обязан управлять остатком"
    assert pos.entry_applied_lots == 8 and pos.executed_lots == 8, \
        "счётчики накопительного исполнения подменены остатком"

    # --- Фаза 3: poll_open_orders() получает lots_executed=10 той же заявки ---
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=10, price_units=1000)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)

    # ОБЯЗАТЕЛЬНОЕ утверждение постановки: quantity == 7, а не 9
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, \
        f"позднее исполнение, уже учтённое снимком, повторно прибавлено: {pos.quantity}"
    # накопительное исполнение заявки учтено в счётчиках (не в остатке)
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10
    # защитное поведение существующей архитектурой: неопределённость + блокировка
    assert client.uncertain_state is True, "неоднозначность не зафиксирована"
    assert ledger.figi_blocks_buy(FIGI) is True, "новые BUY по FIGI не заблокированы"
    assert ledger.latest(oid)["status"] == "UNCERTAIN"
    assert len(active_positions(monitor)) == 1, "дубликат позиции"

    # --- Фаза 4: повторный опрос того же ордера не меняет остаток ---
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "повторный опрос изменил остаток"
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10
    assert len(active_positions(monitor)) == 1

    # --- Фаза 5: контрольная сверка — остаток подтверждается снимком брокера,
    # неопределённость разрешена, проданные лоты не восстановлены ---
    await main.reconcile(client, monitor, ledger, FakeRM())
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "контрольная сверка изменила остаток"
    assert pos.state == PositionState.RECOVERY
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10
    assert client.uncertain_state is False, "неопределённость не разрешена сверкой"


def test_p04_growth_report_after_snapshot_sync_not_added_to_quantity():
    """P0-4, юнит: после ДОСТОВЕРНОЙ синхронизации остатка снимком портфеля
    рост накопительного lots_executed той же BUY-заявки НЕ добавляется к
    остатку (снимок мог уже учесть это исполнение): quantity сохраняется,
    счётчики исполнения догоняются по отчёту заявки; повторный отчёт с тем
    же значением — no-op. Без снимка (контроль) рост применяется как прежде
    — только действительно новые лоты."""
    client = FakeClient()
    monitor = ExecutionMonitor(client)
    pos = monitor.register_recovery(figi=FIGI, ticker=TICKER, direction="BUY",
                                    quantity=8, lot_size=1, order_id="oid-p04-unit",
                                    recovery_source="no_price")
    assert pos.quantity == 8 and pos.entry_applied_lots == 8
    assert pos.entry_snapshot_synced is False

    # достоверный снимок портфеля: остаток 7 (продан 1 лот; снимок мог уже
    # включить и позднее исполнение)
    assert monitor.sync_recovery_quantity(pos, 7) is True
    assert pos.quantity == 7
    assert pos.entry_applied_lots == 8 and pos.executed_lots == 8
    assert pos.entry_snapshot_synced is True, "маркер покрытия снимком не выставлен"

    # рост отчёта заявки 8 -> 10 ПОСЛЕ снимка: дельта к остатку НЕ добавляется
    assert monitor.record_recovery_entry_lots(
        pos, 10, order_id="oid-p04-unit", already_seen_lots=8) is True
    assert pos.quantity == 7, "дельта прибавлена к остатку после снимка (повторный учёт)"
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10, \
        "счётчики накопительного исполнения заявки не догнаны по отчёту"

    # повторный отчёт с тем же значением — no-op
    assert monitor.record_recovery_entry_lots(
        pos, 10, order_id="oid-p04-unit", already_seen_lots=10) is False
    assert pos.quantity == 7
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10

    # устаревший ответ (меньше виденного) — противоречие, без изменений
    assert monitor.record_recovery_entry_lots(
        pos, 9, order_id="oid-p04-unit", already_seen_lots=10) is False
    assert pos.quantity == 7
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10

    # КОНТРОЛЬ: без достоверного снимка рост применяется как прежде (дельта)
    monitor2 = ExecutionMonitor(client)
    pos2 = monitor2.register_recovery(figi=FIGI, ticker=TICKER, direction="BUY",
                                      quantity=3, lot_size=1, order_id="oid-p04-unit2",
                                      recovery_source="no_price")
    assert pos2.entry_snapshot_synced is False
    assert monitor2.record_recovery_entry_lots(
        pos2, 8, order_id="oid-p04-unit2", already_seen_lots=3) is True
    assert pos2.quantity == 8, "без снимка рост исполнения должен применяться к остатку"
    assert pos2.entry_applied_lots == 8 and pos2.executed_lots == 8


# ============================================================
# Партия P0-5: недопущение снятия UNCERTAIN повторным опросом.
# После неоднозначного роста lots_executed заявка в реестре UNCERTAIN;
# повторный отчёт с тем же значением не должен перезаписывать её
# терминальным статусом (CANCELLED/EXPIRED/FILL) и снимать блокировку
# новых BUY по FIGI до подтверждённой сверки портфеля.
# ============================================================

async def _p05_setup_ambiguity(env, monkeypatch, oid="oid-p05"):
    """Общая подготовка сценария P0-5:
      фаза 1: исполнение 8 лотов без свечей -> RECOVERY 8/8/8;
      фаза 2: достоверная сверка портфеля: остаток 7 (снимок мог уже
              включать поздние лоты) — маркер подтверждённой сверки
              выставляется ДО появления UNCERTAIN;
      фаза 3: неоднозначный рост lots_executed 8 -> 10 ПОСЛЕ достоверной
              синхронизации остатка снимком -> UNCERTAIN-запись реестра,
              FIGI заблокирован, quantity остаётся 7.
    Возвращает (ledger, client, monitor, oid)."""
    # raising=False: маркер существует только в коде с фиксом P0-5 — на коде
    # без фикса атрибут создаётся как None (демонстрация дефекта честная:
    # падают только тесты, проверяющие перезапись UNCERTAIN).
    monkeypatch.setattr(main, "_last_reconcile_confirmed_utc", None, raising=False)
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    ledger.record_intent(oid, trade_id="t-p05", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    monitor = ExecutionMonitor(client)

    # фаза 1: RECOVERY 8/8/8
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=8, price_units=800)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos is not None and pos.state == PositionState.RECOVERY
    assert pos.quantity == 8 and pos.entry_applied_lots == 8 and pos.executed_lots == 8

    # фаза 2: достоверная сверка: остаток 7
    client.positions_value = {FIGI: PositionData(lots=7, direction="LONG",
                                                 raw_shares=Decimal(7))}
    await main.reconcile(client, monitor, ledger, FakeRM())
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "достоверный снимок обязан управлять остатком"
    assert pos.entry_applied_lots == 8 and pos.executed_lots == 8
    assert pos.entry_snapshot_synced is True, "маркер покрытия снимком не выставлен"

    # фаза 3: неоднозначный рост 8 -> 10 ПОСЛЕ снимка -> UNCERTAIN
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=10, price_units=1000)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "дельта прибавлена к остатку (дефект партии P0-4)"
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10
    assert client.uncertain_state is True, "неоднозначный рост не зафиксирован"
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN", \
        f"первый неоднозначный рост не перевёл заявку в UNCERTAIN: {rec['status']}"
    assert rec["executed_lots"] == 10
    assert ledger.figi_blocks_buy(FIGI) is True, "новые BUY по FIGI не заблокированы"
    assert len(active_positions(monitor)) == 1
    return ledger, client, monitor, oid


@pytest.mark.asyncio
async def test_p05_1_first_ambiguous_growth_sets_uncertain(env, monkeypatch):
    """P0-5 (1). Первый неоднозначный рост lots_executed ПОСЛЕ достоверной
    синхронизации остатка снимком портфеля переводит заявку в UNCERTAIN:
    client.uncertain_state=True, UNCERTAIN-запись реестра, блокировка новых
    BUY по FIGI; дельта к остатку не добавлена (защита партии P0-4)."""
    ledger, client, monitor, oid = await _p05_setup_ambiguity(env, monkeypatch)
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN"
    assert client.uncertain_state is True
    assert ledger.figi_blocks_buy(FIGI) is True
    pos = monitor.get_position(FIGI)
    assert pos.state == PositionState.RECOVERY
    assert pos.quantity == 7
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10


@pytest.mark.asyncio
async def test_p05_2_repeated_same_lots_poll_keeps_uncertain(env, monkeypatch):
    """P0-5 (2)+(3). Повторный опрос с тем же lots_executed (терминальный
    статус брокера CANCELLED) НЕ снимает ранее установленную UNCERTAIN:
    запись реестра сохраняется (в том числе время последнего перехода),
    терминальный статус НЕ пишется, блокировка новых BUY по FIGI остаётся,
    количество позиции и счётчики не меняются, дубликата нет."""
    ledger, client, monitor, oid = await _p05_setup_ambiguity(env, monkeypatch)
    before = ledger.latest(oid)
    assert before["status"] == "UNCERTAIN"

    # повторный опрос с тем же значением lots_executed
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=10, price_units=1000)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)

    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN", \
        f"повторный терминальный отчёт снял UNCERTAIN: {rec['status']}"
    assert rec["updated_at_utc"] == before["updated_at_utc"], \
        "повторный отчёт перезаписал UNCERTAIN-запись реестра"
    assert ledger.figi_blocks_buy(FIGI) is True, \
        "блокировка FIGI снята повторным терминальным статусом до сверки"
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "повторный отчёт изменил количество позиции"
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10
    assert len(active_positions(monitor)) == 1, "дубликат позиции"
    assert client.uncertain_state is True, \
        "неопределённость снята повторным отчётом без сверки"

    # и третий опрос с тем же значением — результат тот же (стабильность)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    rec = ledger.latest(oid)
    assert rec["status"] == "UNCERTAIN"
    assert rec["updated_at_utc"] == before["updated_at_utc"]
    assert monitor.get_position(FIGI).quantity == 7


@pytest.mark.asyncio
async def test_p05_3_failed_or_contradictory_reconcile_keeps_uncertain(env, monkeypatch):
    """P0-5 (4)+(5). Ошибка reconcile() (недоступный снимок = None) и
    противоречивый снимок (расхождение направлений) НЕ разрешают
    неопределённость: маркер подтверждённой сверки не выставляется,
    UNCERTAIN сохраняется, блокировка FIGI не снимается."""
    ledger, client, monitor, oid = await _p05_setup_ambiguity(env, monkeypatch)
    marker_after_setup = main._last_reconcile_confirmed_utc
    assert marker_after_setup is not None, \
        "успешная сверка подготовки не выставила маркер подтверждённой сверки"

    # ошибка: снимок портфеля недоступен (None) — учёт не меняется
    client.positions_value = None
    await main.reconcile(client, monitor, ledger, FakeRM())
    assert client.uncertain_state is True, "ошибка сверка сняла неопределённость"
    assert main._last_reconcile_confirmed_utc == marker_after_setup, \
        "ошибка сверка выставила маркер подтверждённого разрешения"

    client.order_states[oid] = fake_state(CANCELLED, lots_executed=10, price_units=1000)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert ledger.latest(oid)["status"] == "UNCERTAIN", \
        "ошибка сверка разрешила снятие UNCERTAIN повторным опросом"
    assert ledger.figi_blocks_buy(FIGI) is True

    # противоречивый снимок: направление брокера SHORT при локальном LONG
    client.positions_value = {FIGI: PositionData(lots=-7, direction="SHORT",
                                                 raw_shares=Decimal(-7))}
    await main.reconcile(client, monitor, ledger, FakeRM())
    assert client.uncertain_state is True, "противоречивая сверка сняла неопределённость"
    assert main._last_reconcile_confirmed_utc == marker_after_setup, \
        "противоречивая сверка выставила маркер подтверждённого разрешения"

    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    assert ledger.latest(oid)["status"] == "UNCERTAIN", \
        "противоречивая сверка разрешила снятие UNCERTAIN повторным опросом"
    assert ledger.figi_blocks_buy(FIGI) is True
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "количество изменено при неразрешённой неопределённости"
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10
    assert len(active_positions(monitor)) == 1


@pytest.mark.asyncio
async def test_p05_4_successful_reconcile_allows_resolution_no_side_effects(env, monkeypatch):
    """P0-5 (5)+(6). Успешный reconcile() с подтверждённым остатком
    позволяет штатно разрешить неопределённость: последующий повторный
    опрос с тем же lots_executed закрывает UNCERTAIN-запись терминальным
    статусом брокера и снимает блокировку FIGI; количество позиции не
    меняется, дубликат позиции не создаётся, повторные опросы стабильны."""
    ledger, client, monitor, oid = await _p05_setup_ambiguity(env, monkeypatch)

    # успешная сверка с подтверждённым остатком (7 совпадает с локальным)
    client.positions_value = {FIGI: PositionData(lots=7, direction="LONG",
                                                 raw_shares=Decimal(7))}
    await main.reconcile(client, monitor, ledger, FakeRM())
    assert client.uncertain_state is False, "успешная сверка не сняла неопределённость"
    assert main._last_reconcile_confirmed_utc is not None, \
        "маркер подтверждённой сверки не выставлен"
    pos = monitor.get_position(FIGI)
    assert pos.quantity == 7, "контрольная сверка изменила остаток"
    assert ledger.latest(oid)["status"] == "UNCERTAIN", \
        "сверка не должна сама перезаписывать запись реестра"

    # повторный опрос с тем же lots_executed — штатное разрешение
    client.order_states[oid] = fake_state(CANCELLED, lots_executed=10, price_units=1000)
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)

    rec = ledger.latest(oid)
    assert rec["status"] == "CANCELLED", \
        f"неопределённость не разрешена штатно после подтверждённой сверки: {rec['status']}"
    assert rec["executed_lots"] == 10
    assert ledger.figi_blocks_buy(FIGI) is False, \
        "блокировка FIGI не снята после подтверждённого разрешения"
    pos = monitor.get_position(FIGI)
    assert pos.state == PositionState.RECOVERY
    assert pos.quantity == 7, "разрешение изменило количество позиции"
    assert pos.entry_applied_lots == 10 and pos.executed_lots == 10
    assert len(active_positions(monitor)) == 1, "дубликат позиции при разрешении"
    assert client.uncertain_state is False

    # повторный опрос после разрешения — стабилен: статус и учёт не меняются
    await main.poll_open_orders(client, monitor, FakeRM(), ledger)
    rec2 = ledger.latest(oid)
    assert rec2["status"] == "CANCELLED"
    assert rec2["executed_lots"] == 10
    pos2 = monitor.get_position(FIGI)
    assert pos2.quantity == 7, "повторный опрос после разрешения изменил количество"
    assert pos2.entry_applied_lots == 10 and pos2.executed_lots == 10
    assert len(active_positions(monitor)) == 1, "дубликат позиции после разрешения"
