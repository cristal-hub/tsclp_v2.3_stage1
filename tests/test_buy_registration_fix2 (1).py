# tests/test_buy_registration_fix2.py
# Tsclp_v2.2_volatile — регрессионные тесты этапа BUY REGISTRATION FIX 2.
#
# Дефект P1 — OPEN order_id mismatch: отчёт старой/чужой заявки A (тот же
#   FIGI, другой order_id) для активной OPEN-позиции B не применяется к B,
#   не изменяет количество/цену входа, не помечается успешным маркером
#   («позиция уже зарегистрирована — идемпотентность»), а явно фиксируется
#   как UNCERTAIN (client.uncertain_state + UNCERTAIN-запись реестра с
#   причиной "order_id mismatch" и диагностикой). Совпадение trade_id само
#   по себе применением не разрешает. Повторная обработка идемпотентна.
#
# Дефект P2 — Telegram при ошибке сохранения state.json: сообщение
#   «Позиция зарегистрирована как RECOVERY — контроль сохранён...» и
#   уведомление первичной регистрации BUY не должны противоречить
#   результату _save_state_checked(); при state_saved=False отправляется
#   отдельное критическое предупреждение.
#
# Соответствие обязательным тестам ТЗ (п.3):
#   Тест A — старый BUY A при OPEN позиции B:  test_a_old_order_a_vs_open_b
#   Тест B — совпадающий trade_id, разные order_id:
#                                  test_b_matching_trade_id_does_not_authorize
#   Тест C — повторное получение старого FILL A:
#                                  test_c_repeat_of_old_fill_stays_uncertain
#   Тест D — правильный order_id:  test_d_matching_order_id_still_applies
#   Тест E — RECOVERY и успешное сохранение:
#                                  test_e_recovery_notifies_saved_state
#   Тест F — RECOVERY и неудачное сохранение:
#                                  test_f_recovery_notifies_unsaved_state
#   Тест G — обычная регистрация BUY и неудачное сохранение:
#                                  test_g_primary_buy_unsaved_state
#
# Этап BUY REGISTRATION FIX 3 (исполнение BUY без order_id) — тесты H1-H6
# добавлены в ЭТОТ ЖЕ файл (по ТЗ FIX 3 файл не перемещается, не
# переименовывается и не дублируется):
#   Тест H1 — order_id=None:       test_h1_order_id_none_not_applied
#   Тест H2 — order_id="":         test_h2_order_id_empty_not_applied
#   Тест H3 — order_id="   ":      test_h3_order_id_blank_not_applied
#   Тест H4 — повтор события:      test_h4_repeat_no_order_id_idempotent
#   Тест H5 — контроль своей заявки:
#                              test_h5_matching_order_id_still_applies
#   Тест H6 — контроль чужой заявки (совпадение trade_id):
#                              test_h6_foreign_order_still_blocked

import json
import os
from decimal import Decimal

import pytest

import logger
import main
from execution import ExecutionMonitor, PositionState
from tinkoff_client import PositionData

from tests.test_p0_defect_fixes import (
    FakeClient, FakeRM,
    fake_state, make_ledger, small_df, env,  # env — общий fixture изоляции
)
from tests.test_p0_defect_fixes import FILL, FIGI, TICKER

TERMINAL = ("FILL", "CANCELLED", "REJECTED", "EXPIRED")


class FakeRMState(FakeRM):
    """FakeRM с сериализацией риска (для перезапуска state.json в тестах)."""

    def import_state(self, state):
        return None


def open_kwargs(order_id, trade_id="t-1", executed=5, requested=5, price=100.0):
    return dict(
        figi=FIGI, ticker=TICKER, order_id=order_id, trade_id=trade_id,
        reason="unit", signal_time="now", price_before_order=price,
        order_send_time="now", requested_lots=requested, executed_lots=executed,
        exec_price=price, lot_size=1, partial=False,
        df=small_df(), source="test", ledger_status="FILL",
    )


def journal_records_for(ledger, order_id):
    """Все записи журнала для одной заявки (журнал append-only)."""
    with open(ledger.path, "r", encoding="utf-8") as f:
        recs = [json.loads(x) for x in f if x.strip()]
    return [r for r in recs if str(r.get("order_id")) == str(order_id)]


def make_open_b(client, monitor, rm, ledger, order_id="oid-B",
                trade_id="t-B", executed=3, requested=10):
    """Регистрирует активную OPEN-позицию B по FIGI через единую точку."""
    ledger.record_intent(order_id, trade_id=trade_id, figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=requested)
    ledger.record_sent(order_id, figi=FIGI, side="BUY",
                       requested_lots=requested, executed_lots=executed,
                       status_hint="FILL", source="post_response")
    return main.register_executed_buy(
        client, monitor, rm, ledger,
        **open_kwargs(order_id, trade_id=trade_id, executed=executed,
                      requested=requested))


# ============================================================
# Тест A — старый BUY A при OPEN позиции B
# ============================================================

@pytest.mark.asyncio
async def test_a_old_order_a_vs_open_b(env):
    """OPEN-позиция B (entry_order_id=B). Приходит исполнение старой заявки A
    (тот же FIGI, другой order_id/trade_id): количество и цена позиции B не
    меняются; заявка A получает UNCERTAIN с причиной "order_id mismatch";
    uncertain_state=True; успешный маркер обработки A отсутствует; новые BUY
    по FIGI блокированы существующей архитектурой."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-A-B", trade_id="t-AB")
    assert pos_b.state == PositionState.OPEN and pos_b.quantity == 3
    assert pos_b.entry_price == 100.0 and pos_b.entry_applied_lots == 3

    # поздний отчёт старой заявки A: в реестре она ещё НЕ терминальна (INTENT)
    oid_a = "oid-A-A"
    ledger.record_intent(oid_a, trade_id="t-AA", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=5)
    client.uncertain_state = False
    kw_a = open_kwargs(oid_a, trade_id="t-AA", executed=5, requested=5)
    kw_a["df"] = None
    res = await main.register_executed_buy(client, monitor, rm, ledger, **kw_a)

    # позиция B не затронута, вторая позиция не создана
    assert res is pos_b, "создана вторая позиция по тому же FIGI"
    assert pos_b.quantity == 3, "чужое исполнение прибавлено к позиции B"
    assert pos_b.entry_applied_lots == 3, "счётчик применённых лотов изменён"
    assert pos_b.entry_price == 100.0, "цена входа B изменена исполнением A"
    assert pos_b.entry_order_id == "oid-A-B", "позиция B перепривязана к заявке A"
    assert pos_b.state == PositionState.OPEN
    assert len(monitor.positions) == 1

    # заявка A: явный UNCERTAIN с причиной order_id mismatch и диагностикой
    assert client.uncertain_state is True, "неопределённость не зафиксирована"
    rec_a = ledger.latest(oid_a)
    assert rec_a["status"] == "UNCERTAIN", \
        f"заявка A помечена обработанной (status={rec_a['status']})"
    assert "order_id mismatch" in str(rec_a.get("reason") or "")
    # диагностика: FIGI, старый order_id, trade_id, количество исполнения
    assert rec_a["figi"] == FIGI and rec_a["order_id"] == oid_a
    assert rec_a["trade_id"] == "t-AA" and rec_a["executed_lots"] == 5
    assert "oid-A-B" in str(rec_a.get("reason") or ""), \
        "текущий entry_order_id позиции отсутствует в диагностике"

    # успешного маркера обработки A нет: ни одного терминального статуса и
    # ни одного «позиция уже зарегистрирована — идемпотентность»
    recs_a = journal_records_for(ledger, oid_a)
    assert not any(r.get("status") in TERMINAL for r in recs_a), \
        "заявка A закрыта терминальным статусом без применения исполнения"
    assert not any("позиция уже зарегистрирована" in str(r.get("reason") or "")
                   for r in recs_a), "ложный маркер идемпотентности для заявки A"

    # новые BUY по FIGI блокированы существующим механизмом
    assert ledger.figi_blocks_buy(FIGI), \
        "UNCERTAIN-запись не блокирует новые BUY по FIGI"

    # тот же отчёт A с УЖЕ терминальной записью в реестре (FILL от брокера):
    # молчаливого пропуска нет — фиксируется тот же UNCERTAIN
    ledger.record_sent(oid_a, figi=FIGI, side="BUY", requested_lots=5,
                       executed_lots=5, status_hint="FILL", source="post_response")
    client.uncertain_state = False
    res2 = await main.register_executed_buy(client, monitor, rm, ledger, **kw_a)
    assert res2 is pos_b
    assert pos_b.quantity == 3 and pos_b.entry_applied_lots == 3
    assert client.uncertain_state is True
    assert ledger.latest(oid_a)["status"] == "UNCERTAIN"
    recs_a2 = journal_records_for(ledger, oid_a)
    assert not any("позиция уже зарегистрирована" in str(r.get("reason") or "")
                   for r in recs_a2)


# ============================================================
# Тест B — совпадающий trade_id, но разные order_id
# ============================================================

@pytest.mark.asyncio
async def test_b_matching_trade_id_does_not_authorize(env):
    """У позиции B entry_order_id=B и trade_id=T. Приходит исполнение заявки A
    с ТЕМ ЖЕ trade_id, но другим order_id: совпадение trade_id само по себе НЕ
    разрешает применять лоты A к позиции B — результат UNCERTAIN
    (принадлежность заявки и сделки проверяется согласованно)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-B-B", trade_id="t-same")
    assert pos_b.state == PositionState.OPEN
    assert pos_b.trade_id == "t-same" and pos_b.entry_order_id == "oid-B-B"

    oid_a = "oid-B-A"
    ledger.record_intent(oid_a, trade_id="t-same", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=5)
    client.uncertain_state = False
    kw_a = open_kwargs(oid_a, trade_id="t-same", executed=5, requested=5)
    kw_a["df"] = None
    res = await main.register_executed_buy(client, monitor, rm, ledger, **kw_a)

    assert res is pos_b
    assert pos_b.quantity == 3, \
        "лоты заявки A применены к позиции B при другом order_id"
    assert pos_b.entry_applied_lots == 3
    assert pos_b.entry_order_id == "oid-B-B", "позиция перепривязана к заявке A"
    assert client.uncertain_state is True
    rec_a = ledger.latest(oid_a)
    assert rec_a["status"] == "UNCERTAIN", \
        "совпадение trade_id закрыло чужую заявку как обработанную"
    assert "order_id mismatch" in str(rec_a.get("reason") or "")
    recs_a = journal_records_for(ledger, oid_a)
    assert not any(r.get("status") in TERMINAL for r in recs_a)


# ============================================================
# Тест C — повторное получение старого FILL A
# ============================================================

@pytest.mark.asyncio
async def test_c_repeat_of_old_fill_stays_uncertain(env):
    """Повторная обработка того же исполнения A идемпотентна: лоты не
    удваиваются, ложного успешного маркера не появляется, неопределённость
    не снимается самопроизвольно."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-C-B", trade_id="t-CB")
    assert pos_b.quantity == 3

    oid_a = "oid-C-A"
    ledger.record_intent(oid_a, trade_id="t-CA", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=5)
    kw_a = open_kwargs(oid_a, trade_id="t-CA", executed=5, requested=5)
    kw_a["df"] = None
    client.uncertain_state = False
    res1 = await main.register_executed_buy(client, monitor, rm, ledger, **kw_a)
    assert res1 is pos_b and pos_b.quantity == 3
    assert client.uncertain_state is True
    assert ledger.latest(oid_a)["status"] == "UNCERTAIN"

    # повторная обработка ТОГО ЖЕ события
    res2 = await main.register_executed_buy(client, monitor, rm, ledger, **kw_a)
    assert res2 is pos_b
    assert pos_b.quantity == 3, "повторная обработка удвоила лоты заявки A"
    assert pos_b.entry_applied_lots == 3
    assert pos_b.entry_order_id == "oid-C-B"
    assert client.uncertain_state is True, \
        "неопределённость исчезла после повторной обработки"
    assert ledger.latest(oid_a)["status"] == "UNCERTAIN", \
        "повторный отчёт снял UNCERTAIN-запись"

    # третий повтор — тот же результат
    res3 = await main.register_executed_buy(client, monitor, rm, ledger, **kw_a)
    assert res3 is pos_b and pos_b.quantity == 3
    assert client.uncertain_state is True

    recs_a = journal_records_for(ledger, oid_a)
    assert not any(r.get("status") in TERMINAL for r in recs_a), \
        "повторная обработка создала успешный терминальный маркер"
    assert not any("позиция уже зарегистрирована" in str(r.get("reason") or "")
                   for r in recs_a)
    assert not any("исполнение применено" in str(r.get("reason") or "")
                   for r in recs_a), "повторная запись успешного применения"


# ============================================================
# Тест D — правильный order_id (контроль от перегиба)
# ============================================================

@pytest.mark.asyncio
async def test_d_matching_order_id_still_applies(env):
    """Исполнение СВОЕЙ заявки (order_id совпадает с entry_order_id)
    применяется корректно: рост cumulative добавляет только дельту,
    повторная обработка не удваивает количество, существующая
    идемпотентность сохраняется, ложная неопределённость не фиксируется."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    oid = "oid-D"
    pos = await main.register_executed_buy(
        client, monitor, rm, ledger,
        **open_kwargs(oid, trade_id="t-D", executed=3, requested=10))
    assert pos.state == PositionState.OPEN and pos.quantity == 3
    client.uncertain_state = False

    # рост cumulative своей заявки 3 -> 5: применяется дельта +2
    res = await main.register_executed_buy(
        client, monitor, rm, ledger,
        **open_kwargs(oid, trade_id="t-D", executed=5, requested=10))
    assert res is pos
    assert pos.quantity == 5 and pos.entry_applied_lots == 5, \
        "проверка order_id заблокировала рост своей заявки"
    assert pos.entry_price == 100.0, "цена входа изменена ростом своей заявки"
    assert client.uncertain_state is False, \
        "ложная неопределённость для своей заявки"
    assert ledger.latest(oid)["status"] == "FILL"
    recs = journal_records_for(ledger, oid)
    assert not any(r.get("status") == "UNCERTAIN" for r in recs)

    # повторная обработка того же отчёта — идемпотентность
    res2 = await main.register_executed_buy(
        client, monitor, rm, ledger,
        **open_kwargs(oid, trade_id="t-D", executed=5, requested=10))
    assert res2 is pos
    assert pos.quantity == 5 and pos.entry_applied_lots == 5, \
        "повторная обработка удвоила количество"
    assert client.uncertain_state is False
    rec = ledger.latest(oid)
    assert rec["status"] == "FILL"
    assert "позднее исполнение применено" in str(rec.get("reason") or ""), \
        "существующая идемпотентность своей заявки нарушена"


# ============================================================
# Тест E — RECOVERY и успешное сохранение state.json
# ============================================================

@pytest.mark.asyncio
async def test_e_recovery_notifies_saved_state(env, monkeypatch):
    """_save_state_checked() вернул True: отправляется обычное уведомление
    RECOVERY («контроль сохранён»), в тексте нет предупреждения о
    несохранённом состоянии."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    oid = "oid-E"
    ledger.record_intent(oid, trade_id="t-E", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=4)
    ledger.record_sent(oid, figi=FIGI, side="BUY", requested_lots=4,
                       executed_lots=4, status_hint="FILL")

    sent = []

    async def spy_notify(text):
        sent.append(text)

    monkeypatch.setattr(main, "notify", spy_notify)
    kw = open_kwargs(oid, trade_id="t-E", executed=4, requested=4)
    kw["df"] = None  # без свечей -> RECOVERY
    pos = await main.register_executed_buy(client, monitor, rm, ledger, **kw)

    assert pos.state == PositionState.RECOVERY
    assert len(sent) == 1, f"ожидалось 1 уведомление, отправлено {len(sent)}"
    assert "RECOVERY" in sent[0]
    assert "контроль сохранён" in sent[0], \
        "обычное уведомление RECOVERY не отправлено при успешном сохранении"
    assert "КРИТИЧЕСКОЕ" not in sent[0], \
        "предупреждение о несохранённом состоянии при успешном сохранении"
    assert "НЕ подтверждено" not in sent[0]


# ============================================================
# Тест F — RECOVERY и неудачное сохранение state.json
# ============================================================

@pytest.mark.asyncio
async def test_f_recovery_notifies_unsaved_state(env, monkeypatch):
    """_save_state_checked() вернул False: отправляется предупреждение о
    несохранённом state.json; сообщение «контроль сохранён» НЕ отправляется;
    новые BUY остаются заблокированными (state_save_failed)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    oid = "oid-F"
    ledger.record_intent(oid, trade_id="t-F", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=4)
    ledger.record_sent(oid, figi=FIGI, side="BUY", requested_lots=4,
                       executed_lots=4, status_hint="FILL")

    sent = []

    async def spy_notify(text):
        sent.append(text)

    monkeypatch.setattr(main, "notify", spy_notify)
    monkeypatch.setattr(main, "_save_state", lambda m, r: False)
    kw = open_kwargs(oid, trade_id="t-F", executed=4, requested=4)
    kw["df"] = None  # без свечей -> RECOVERY
    pos = await main.register_executed_buy(client, monitor, rm, ledger, **kw)

    assert pos.state == PositionState.RECOVERY
    assert len(sent) == 1, \
        f"ожидалось 1 уведомление-предупреждение, отправлено {len(sent)}"
    text = sent[0]
    assert "КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ" in text, "предупреждение не отправлено"
    assert "только в оперативной памяти" in text
    assert "state.json НЕ подтверждено" in text
    assert "Новые BUY заблокированы" in text
    assert "контроль сохранён" not in text, \
        "при несохранённом state.json заявлено «контроль сохранён»"
    assert "RECOVERY" in text

    # новые BUY заблокированы: механизм state_save_failed
    assert main.state_save_failed is True, "новые входы не заблокированы"
    # подтверждение регистрации в реестр не записано
    recs = journal_records_for(ledger, oid)
    assert not any("позиция зарегистрирована" in str(r.get("reason") or "")
                   for r in recs), \
        "подтверждение регистрации записано при несохранённом state.json"


# ============================================================
# Тест G — обычная регистрация BUY и неудачное сохранение
# ============================================================

@pytest.mark.asyncio
async def test_g_primary_buy_unsaved_state(env, monkeypatch):
    """Первичная регистрация BUY при неудачном сохранении state.json:
    Telegram не сообщает о надёжно сохранённой позиции; успешный маркер
    регистрации не записывается преждевременно; заявка остаётся доступной
    повторному разбору события после перезапуска."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    oid = "oid-G"
    ledger.record_intent(oid, trade_id="t-G", figi=FIGI, ticker=TICKER,
                         side="BUY", requested_lots=10)
    ledger.record_sent(oid, figi=FIGI, side="BUY", requested_lots=10,
                       executed_lots=5, status_hint="FILL")

    sent = []

    async def spy_notify(text):
        sent.append(text)

    real_save_state = main._save_state
    monkeypatch.setattr(main, "notify", spy_notify)
    monkeypatch.setattr(main, "_save_state", lambda m, r: False)
    pos = await main.register_executed_buy(
        client, monitor, rm, ledger,
        **open_kwargs(oid, trade_id="t-G", executed=5, requested=10))

    # позиция зарегистрирована в памяти (исполнение не потеряно)
    assert pos.state == PositionState.OPEN and pos.quantity == 5
    assert pos.entry_order_id == oid

    # Telegram не сообщает о надёжно сохранённой позиции
    assert len(sent) == 1
    assert "КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ" in sent[0], \
        "уведомление об успешной регистрации без предупреждения о state.json"
    assert "Сохранение state.json НЕ подтверждено" in sent[0]
    assert "контроль сохранён" not in sent[0]

    # успешный маркер регистрации не записан преждевременно
    recs = journal_records_for(ledger, oid)
    assert not any("позиция зарегистрирована" in str(r.get("reason") or "")
                   for r in recs), \
        "успешный маркер регистрации записан при несохранённом state.json"
    assert main.state_save_failed is True

    # сохранение восстановлено: перезапуск восстанавливает позицию
    monkeypatch.setattr(main, "_save_state", real_save_state)
    assert real_save_state(monitor, rm), "восстановленное сохранение не работает"

    m2 = ExecutionMonitor(client)
    assert main._load_state(m2, FakeRMState())
    p2 = m2.get_position(FIGI)
    assert p2 is not None and p2.quantity == 5 and p2.entry_order_id == oid, \
        "позиция потеряна при перезапуске"

    # заявка остаётся доступной повторному разбору события (FILL с остатком:
    # requested=10 > executed=5), повторный разбор не создаёт дубликат
    assert any(r["order_id"] == oid for r in ledger.open_orders()), \
        "заявка недоступна повторному разбору после перезапуска"
    client2 = FakeClient(positions={})
    client2.order_states[oid] = fake_state(FILL, lots_executed=5, price_units=500)
    m2.client = client2
    await main.startup_recovery(client2, m2, FakeRMState(), ledger)
    p3 = m2.get_position(FIGI)
    assert p3 is not None and p3.quantity == 5 and p3.entry_order_id == oid, \
        "повторный разбор изменил восстановленную позицию"
    assert len([p for p in m2.positions.values()
                if p.state in (PositionState.OPEN, PositionState.RECOVERY)]) == 1, \
        "повторный разбор события создал дубликат позиции"


# ============================================================
# Этап BUY REGISTRATION FIX 3 — исполнение BUY без order_id
# ============================================================

def journal_all(ledger):
    """Все записи журнала реестра (журнал append-only)."""
    with open(ledger.path, "r", encoding="utf-8") as f:
        return [json.loads(x) for x in f if x.strip()]


def diagnostic_events(env):
    """События диагностики (research_events.jsonl, файл перенаправлен env)."""
    path = logger.EVENTS_FILE
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(x) for x in f if x.strip()]


def no_order_id_kwargs(order_id_value, trade_id, executed=5, requested=5):
    kw = open_kwargs(order_id_value, trade_id=trade_id, executed=executed,
                     requested=requested)
    kw["df"] = None
    return kw


async def run_no_order_id_event(client, monitor, rm, ledger, order_id_value,
                                trade_id):
    return await main.register_executed_buy(
        client, monitor, rm, ledger,
        **no_order_id_kwargs(order_id_value, trade_id))


def assert_no_order_id_rejected(env, ledger, client, monitor, pos_b,
                                journal_before, oid_b, order_id_raw, trade_id_in):
    """Общие проверки H1-H3: позиция не изменена, неопределённость
    зафиксирована, успешного маркера нет, диагностика записана."""
    # 1-4: лоты не применены — quantity/entry_price/entry_applied_lots
    # не изменены, привязка позиции не изменена, вторая позиция не создана
    assert pos_b.quantity == 3, "quantity изменён исполнением без order_id"
    assert pos_b.entry_applied_lots == 3, "entry_applied_lots изменён"
    assert pos_b.entry_price == 100.0, "entry_price изменён"
    assert pos_b.entry_order_id == oid_b, "привязка позиции изменена"
    assert pos_b.state == PositionState.OPEN
    assert len(monitor.positions) == 1, "создана вторая позиция по тому же FIGI"

    # 6-7: неопределённость зафиксирована — глобальный флаг, которым
    # trading_cycle запрещает новые входы до устранения
    assert client.uncertain_state is True, "uncertain_state не установлен"
    assert client.is_uncertain(), "новые BUY не заблокированы (is_uncertain)"

    # 5: успешной отметки регистрации исполнения нет: журнал реестра не
    # получил НИ ОДНОЙ новой записи (событие не адресуемо order_id),
    # фиктивные идентификаторы не придуманы
    journal_after = journal_all(ledger)
    assert journal_after == journal_before, \
        "исполнение без order_id изменило реестр (возможен ложный маркер регистрации)"
    assert not any(str(r.get("order_id")) in ("None", "") for r in journal_after), \
        "для события без order_id придумана фиктивная запись/ID в реестре"

    # 8: диагностическое событие с причиной отсутствия order_id
    evs = [e for e in diagnostic_events(env)
           if e.get("event") == "BUY_EXEC_NO_ORDER_ID"]
    assert len(evs) == 1, \
        f"ожидалось 1 диагностическое событие, записано {len(evs)}"
    ev = evs[0]
    assert ev["figi"] == FIGI and ev["ticker"] == TICKER
    assert ev["executed_lots"] == 5
    assert ev["requested_lots"] == 5
    assert ev["trade_id"] == trade_id_in
    assert ev["order_id_raw"] == order_id_raw, \
        "причина отсутствия order_id не отражена в диагностике"
    assert ev["position_state"] == "OPEN"
    assert ev["entry_order_id"] == oid_b
    assert ev["entry_applied_lots"] == 3 and ev["position_quantity"] == 3
    assert "order_id" in str(ev.get("note") or ""), \
        "в диагностике нет причины (отсутствие order_id)"


@pytest.mark.asyncio
async def test_h1_order_id_none_not_applied(env):
    """H1 (FIX 3): OPEN-позиция B существует; приходит исполнение с
    order_id=None. Лоты НЕ применяются: quantity/entry_price/
    entry_applied_lots не изменены; successful-маркера нет;
    uncertain_state=True (новые BUY запрещены); диагностическое событие с
    причиной отсутствия order_id записано; фиктивные ID не придумываются."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-H1-B", trade_id="t-H1B")
    assert pos_b.state == PositionState.OPEN
    assert pos_b.quantity == 3 and pos_b.entry_applied_lots == 3
    assert pos_b.entry_price == 100.0

    journal_before = _journal_all_safe(ledger)
    client.uncertain_state = False
    res = await run_no_order_id_event(client, monitor, rm, ledger, None,
                                      trade_id="t-H1X")
    assert res is pos_b, "создана вторая позиция по тому же FIGI"
    assert_no_order_id_rejected(env, ledger, client, monitor, pos_b,
                                journal_before, "oid-H1-B", None, "t-H1X")


@pytest.mark.asyncio
async def test_h2_order_id_empty_not_applied(env):
    """H2 (FIX 3): тот же сценарий с order_id="" — те же проверки."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-H2-B", trade_id="t-H2B")
    assert pos_b.state == PositionState.OPEN
    assert pos_b.quantity == 3 and pos_b.entry_applied_lots == 3

    journal_before = _journal_all_safe(ledger)
    client.uncertain_state = False
    res = await run_no_order_id_event(client, monitor, rm, ledger, "",
                                      trade_id="t-H2X")
    assert res is pos_b
    assert_no_order_id_rejected(env, ledger, client, monitor, pos_b,
                                journal_before, "oid-H2-B", "", "t-H2X")


@pytest.mark.asyncio
async def test_h3_order_id_blank_not_applied(env):
    """H3 (FIX 3): тот же сценарий с order_id="   " (только пробелы) —
    те же проверки."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-H3-B", trade_id="t-H3B")
    assert pos_b.state == PositionState.OPEN
    assert pos_b.quantity == 3 and pos_b.entry_applied_lots == 3

    journal_before = _journal_all_safe(ledger)
    client.uncertain_state = False
    res = await run_no_order_id_event(client, monitor, rm, ledger, "   ",
                                      trade_id="t-H3X")
    assert res is pos_b
    assert_no_order_id_rejected(env, ledger, client, monitor, pos_b,
                                journal_before, "oid-H3-B", "   ", "t-H3X")


@pytest.mark.asyncio
async def test_h4_repeat_no_order_id_idempotent(env):
    """H4 (FIX 3): повторное получение того же события без order_id
    идемпотентно: позиция не меняется повторно, неопределённость
    сохраняется, реестр не изменяется."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-H4-B", trade_id="t-H4B")
    assert pos_b.quantity == 3 and pos_b.entry_applied_lots == 3

    journal_before = _journal_all_safe(ledger)
    client.uncertain_state = False
    res1 = await run_no_order_id_event(client, monitor, rm, ledger, None,
                                       trade_id="t-H4X")
    assert res1 is pos_b
    assert pos_b.quantity == 3 and pos_b.entry_applied_lots == 3
    assert client.uncertain_state is True
    journal_mid = _journal_all_safe(ledger)
    assert journal_mid == journal_before, "первая обработка изменила реестр"

    # повторная обработка ТОГО ЖЕ события
    res2 = await run_no_order_id_event(client, monitor, rm, ledger, None,
                                       trade_id="t-H4X")
    assert res2 is pos_b
    assert pos_b.quantity == 3, "повторная обработка изменила количество"
    assert pos_b.entry_applied_lots == 3, \
        "повторная обработка изменила entry_applied_lots"
    assert pos_b.entry_price == 100.0, "повторная обработка изменила цену"
    assert pos_b.entry_order_id == "oid-H4-B"
    assert pos_b.state == PositionState.OPEN
    assert client.uncertain_state is True, \
        "неопределённость исчезла после повторной обработки"
    assert journal_all(ledger) == journal_mid, \
        "повторная обработка изменила реестр (повторные записи состояния)"


@pytest.mark.asyncio
async def test_h5_matching_order_id_still_applies(env, monkeypatch):
    """H5 (FIX 3): контроль положительного сценария — при достоверном
    совпадении order_id прежняя логика применения исполнения работает:
    дельта применяется, цена входа не меняется, повтор идемпотентен,
    ложной неопределённости нет, штатное уведомление отправляется."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    oid = "oid-H5-B"
    pos = await make_open_b(client, monitor, rm, ledger, order_id=oid,
                            trade_id="t-H5B")
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 3 and pos.entry_applied_lots == 3
    client.uncertain_state = False

    sent = []

    async def spy_notify(text):
        sent.append(text)

    monkeypatch.setattr(main, "notify", spy_notify)

    # рост cumulative своей заявки 3 -> 5: применяется дельта +2
    res = await main.register_executed_buy(
        client, monitor, rm, ledger,
        **open_kwargs(oid, trade_id="t-H5B", executed=5, requested=10))
    assert res is pos
    assert pos.quantity == 5 and pos.entry_applied_lots == 5, \
        "исправление FIX 3 заблокировало штатное применение своей заявки"
    assert pos.entry_price == 100.0, "цена входа изменена ростом своей заявки"
    assert client.uncertain_state is False, "ложная неопределённость"
    assert ledger.latest(oid)["status"] == "FILL"
    assert "позднее исполнение применено" in str(
        ledger.latest(oid).get("reason") or "")
    assert any("позднее исполнение входной заявки" in t for t in sent), \
        "штатное уведомление о позднем исполнении не отправлено"

    # повторная обработка того же отчёта — идемпотентность
    res2 = await main.register_executed_buy(
        client, monitor, rm, ledger,
        **open_kwargs(oid, trade_id="t-H5B", executed=5, requested=10))
    assert res2 is pos
    assert pos.quantity == 5 and pos.entry_applied_lots == 5, \
        "повторная обработка удвоила количество"
    assert client.uncertain_state is False


@pytest.mark.asyncio
async def test_h6_foreign_order_still_blocked(env):
    """H6 (FIX 3): контроль чужой заявки — защита FIX 2 сохранена:
    исполнение старой заявки A (тот же FIGI, другой order_id, тот же
    trade_id) не применяется к OPEN-позиции B; совпадение trade_id
    применением не разрешает; UNCERTAIN с order_id mismatch."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    pos_b = await make_open_b(client, monitor, rm, ledger,
                              order_id="oid-H6-B", trade_id="t-H6-same")
    assert pos_b.state == PositionState.OPEN
    assert pos_b.quantity == 3 and pos_b.entry_applied_lots == 3
    assert pos_b.trade_id == "t-H6-same"

    oid_a = "oid-H6-A"
    ledger.record_intent(oid_a, trade_id="t-H6-same", figi=FIGI,
                         ticker=TICKER, side="BUY", requested_lots=5)
    client.uncertain_state = False
    kw_a = open_kwargs(oid_a, trade_id="t-H6-same", executed=5, requested=5)
    kw_a["df"] = None
    res = await main.register_executed_buy(client, monitor, rm, ledger, **kw_a)

    assert res is pos_b
    assert pos_b.quantity == 3, \
        "лоты заявки A применены к позиции B при другом order_id"
    assert pos_b.entry_applied_lots == 3
    assert pos_b.entry_price == 100.0
    assert pos_b.entry_order_id == "oid-H6-B", "позиция перепривязана к A"
    assert client.uncertain_state is True
    rec_a = ledger.latest(oid_a)
    assert rec_a["status"] == "UNCERTAIN", \
        "совпадение trade_id закрыло чужую заявку как обработанную"
    assert "order_id mismatch" in str(rec_a.get("reason") or "")
    recs_a = journal_records_for(ledger, oid_a)
    assert not any(r.get("status") in TERMINAL for r in recs_a)
    assert not any("позиция уже зарегистрирована" in str(r.get("reason") or "")
                   for r in recs_a)


# ============================================================
# Серия I (этап «финальный согласованный комплект FIX 3»):
#   I1 — прямые вызовы apply_late_entry_lots (execution.py) с
#        отсутствующим/пустым/пробельным order_id + свой/чужой ID;
#   I2 — прямые вызовы record_recovery_entry_lots (execution.py);
#   I3 — RECOVERY-ветка main-пути: исполнение без order_id
#        (аналог H1–H3 для RECOVERY) + идемпотентность повтора;
#   I4 — перезапуск: позиция восстановлена без неприменённых лотов,
#        неопределённость не теряется молча (повтор события или
#        расхождение снимка брокера при сверке её восстанавливают);
#   I5 — ошибка _save_state_checked(): нет ложного подтверждения
#        сохранения/регистрации, state_save_failed, STATE_SAVE_FAILED.
# ============================================================


def _make_recovery_b(monitor, order_id="oid-REC-B", executed=3):
    """RECOVERY-позиция B по FIGI (штатный источник: отчёт без свечей —
    регистрация через register_recovery, entry_order_id известен)."""
    return monitor.register_recovery(
        figi=FIGI, ticker=TICKER, direction="BUY",
        quantity=executed, lot_size=1, order_id=order_id,
        recovery_source="no_price")


def _journal_all_safe(ledger):
    """journal_all, устойчивый к отсутствию файла реестра (если в реестр
    ещё не было ни одной записи, файла нет — реестр пуст)."""
    if not os.path.isfile(ledger.path):
        return []
    return journal_all(ledger)


@pytest.mark.asyncio
async def test_i1_execution_apply_late_rejects_missing_order_id(env):
    """I1 (защита в глубину execution.py): apply_late_entry_lots() при
    order_id=None/""/пробелах возвращает False и НЕ изменяет OPEN-позицию
    (quantity/entry_price/entry_applied_lots/entry_order_id); чужой
    order_id отклоняется; собственный корректный order_id применяется
    штатно (совместимость сохранена)."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    oid = "oid-I1-B"
    pos = await make_open_b(client, monitor, rm, ledger, order_id=oid,
                            trade_id="t-I1B")
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 3 and pos.entry_applied_lots == 3
    assert pos.entry_price == 100.0

    for bad in (None, "", "   "):
        assert monitor.apply_late_entry_lots(pos, 5, bad) is False, \
            f"исполнение с order_id={bad!r} применено к OPEN-позиции"
        assert pos.quantity == 3, "quantity изменён исполнением без order_id"
        assert pos.entry_applied_lots == 3, "entry_applied_lots изменён"
        assert pos.entry_price == 100.0, "entry_price изменён"
        assert pos.entry_order_id == oid, "привязка позиции изменена"
        assert pos.state == PositionState.OPEN

    # чужой (несовпадающий) order_id — отклонение на уровне метода
    assert monitor.apply_late_entry_lots(pos, 7, "oid-foreign") is False
    assert pos.quantity == 3 and pos.entry_applied_lots == 3

    # контроль совместимости: собственный корректный order_id работает
    assert monitor.apply_late_entry_lots(pos, 5, oid) is True
    assert pos.quantity == 5 and pos.entry_applied_lots == 5
    assert pos.entry_price == 100.0, "цена входа изменена применением дельты"


def test_i2_execution_record_recovery_rejects_missing_order_id(env):
    """I2 (защита в глубину execution.py): record_recovery_entry_lots()
    при order_id=None/""/пробелах возвращает False и НЕ изменяет
    RECOVERY-позицию; чужой order_id отклоняется; собственный корректный
    order_id применяется штатно (совместимость сохранена)."""
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    oid = "oid-I2-B"
    pos = _make_recovery_b(monitor, order_id=oid)
    assert pos.state == PositionState.RECOVERY
    assert pos.quantity == 3 and pos.entry_applied_lots == 3
    assert pos.entry_snapshot_synced is False

    for bad in (None, "", "   "):
        assert monitor.record_recovery_entry_lots(pos, 5, order_id=bad) is False, \
            f"исполнение с order_id={bad!r} применено к RECOVERY-позиции"
        assert pos.quantity == 3, "quantity изменён исполнением без order_id"
        assert pos.entry_applied_lots == 3, "entry_applied_lots изменён"
        assert pos.entry_order_id == oid, "привязка позиции изменена"
        assert pos.state == PositionState.RECOVERY

    # чужой (несовпадающий) order_id — отклонение на уровне метода
    assert monitor.record_recovery_entry_lots(pos, 7, order_id="oid-foreign") is False
    assert pos.quantity == 3 and pos.entry_applied_lots == 3

    # контроль совместимости: собственный корректный order_id работает
    assert monitor.record_recovery_entry_lots(pos, 5, order_id=oid) is True
    assert pos.quantity == 5 and pos.entry_applied_lots == 5


@pytest.mark.asyncio
async def test_i3_recovery_branch_no_order_id_not_applied(env):
    """I3 (RECOVERY-ветка main-пути, аналог H1–H3): активная
    RECOVERY-позиция + исполнение без order_id (None/""/пробелы):
    лоты НЕ применены, uncertain_state=True, диагностическое событие
    BUY_EXEC_NO_ORDER_ID с position_state=RECOVERY, реестр не изменён;
    повтор того же события идемпотентен."""
    for idx, bad in enumerate((None, "", "   ")):
        subdir = env.tmp_path / f"i3-it{idx}"
        subdir.mkdir(exist_ok=True)
        ledger = make_ledger(subdir)
        client = FakeClient(positions={})
        monitor = ExecutionMonitor(client)
        rm = FakeRM()
        oid = "oid-I3-B"
        pos = _make_recovery_b(monitor, order_id=oid)
        assert pos.state == PositionState.RECOVERY
        assert pos.quantity == 3 and pos.entry_applied_lots == 3

        journal_before = _journal_all_safe(ledger)
        client.uncertain_state = False
        evs_before = len([e for e in diagnostic_events(env)
                          if e.get("event") == "BUY_EXEC_NO_ORDER_ID"])

        res = await run_no_order_id_event(client, monitor, rm, ledger, bad,
                                          trade_id="t-I3X")
        assert res is pos, "создана вторая позиция по тому же FIGI"
        assert pos.quantity == 3, "quantity изменён (RECOVERY-ветка)"
        assert pos.entry_applied_lots == 3, "entry_applied_lots изменён"
        assert pos.entry_order_id == oid, "привязка позиции изменена"
        assert pos.state == PositionState.RECOVERY
        assert client.uncertain_state is True, "uncertain_state не установлен"
        assert client.is_uncertain(), "новые BUY не заблокированы"
        assert _journal_all_safe(ledger) == journal_before, \
            "исполнение без order_id изменило реестр (RECOVERY-ветка)"

        evs = [e for e in diagnostic_events(env)
               if e.get("event") == "BUY_EXEC_NO_ORDER_ID"]
        assert len(evs) == evs_before + 1, \
            "диагностическое событие не записано (RECOVERY-ветка)"
        ev = evs[-1]
        assert ev["position_state"] == "RECOVERY"
        assert ev["order_id_raw"] == bad
        assert ev["entry_order_id"] == oid
        assert ev["entry_applied_lots"] == 3 and ev["position_quantity"] == 3

        # повтор того же события — идемпотентность
        journal_mid = _journal_all_safe(ledger)
        price_before_repeat = pos.entry_price
        res2 = await run_no_order_id_event(client, monitor, rm, ledger, bad,
                                           trade_id="t-I3X")
        assert res2 is pos
        assert pos.quantity == 3 and pos.entry_applied_lots == 3
        assert pos.entry_price == price_before_repeat, \
            "повторная обработка изменила цену входа"
        assert client.uncertain_state is True, \
            "неопределённость исчезла после повтора"
        assert _journal_all_safe(ledger) == journal_mid, \
            "повтор изменил реестр (RECOVERY-ветка)"


@pytest.mark.asyncio
async def test_i4_uncertain_state_survives_restart(env):
    """I4 (перезапуск): после события без order_id состояние сохранено
    (guard вызывает _save_state_checked); после перезапуска позиция
    восстановлена БЕЗ неприменённых лотов (quantity/entry_applied_lots
    как до события); неопределённость не теряется молча — при повторе
    события uncertain_state снова True; при расхождении снимка брокера
    (лишние лоты у брокера) сверка снова фиксирует неопределённость,
    учёт не раздувается."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRMState()
    oid = "oid-I4-B"
    pos = await make_open_b(client, monitor, rm, ledger, order_id=oid,
                            trade_id="t-I4B")
    assert pos.quantity == 3 and pos.entry_applied_lots == 3

    client.uncertain_state = False
    res = await run_no_order_id_event(client, monitor, rm, ledger, None,
                                      trade_id="t-I4X")
    assert res is pos and client.uncertain_state is True
    assert pos.quantity == 3 and pos.entry_applied_lots == 3

    # --- перезапуск: новый клиент/монитор/риск, state.json на диске ---
    client2 = FakeClient(positions={})
    monitor2 = ExecutionMonitor(client2)
    rm2 = FakeRMState()
    assert main._load_state(monitor2, rm2) is True, \
        "guard не сохранил состояние (state.json отсутствует)"
    pos2 = monitor2.get_position(FIGI)
    assert pos2 is not None
    assert pos2.quantity == 3, \
        "неприменённые лоты попали в сохранённое состояние"
    assert pos2.entry_applied_lots == 3
    assert pos2.entry_order_id == oid
    assert pos2.state == PositionState.OPEN
    assert client2.uncertain_state is False  # новый процесс: флаг сброшен

    # путь (а): то же событие приходит снова — неопределённость включается
    client2.uncertain_state = False
    res2 = await run_no_order_id_event(client2, monitor2, rm2, ledger, None,
                                       trade_id="t-I4X")
    assert res2 is pos2
    assert client2.uncertain_state is True, \
        "после перезапуска повтор события без order_id не заблокировал новые BUY"
    assert pos2.quantity == 3 and pos2.entry_applied_lots == 3

    # путь (б): исполнение дошло до брокера — снимок показывает 5 лот.
    # Сверка фиксирует расхождение: uncertain_state=True, учёт не раздувается.
    client2.uncertain_state = False
    client2.positions_value = {FIGI: PositionData(lots=5, direction="LONG",
                                                  raw_shares=Decimal(5))}
    await main.reconcile(client2, monitor2, ledger, rm2)
    assert client2.uncertain_state is True, \
        "расхождение снимка брокера не зафиксировано как неопределённость"
    assert main.reconcile_done is False, \
        "сверка с расхождением помечена завершённой"
    assert monitor2.get_position(FIGI).quantity == 3, \
        "сверка раздула учёт без разбора расхождения"


@pytest.mark.asyncio
async def test_i5_save_failure_no_false_confirmation(env, monkeypatch):
    """I5 (ошибка _save_state_checked при событии без order_id): нет
    ложного подтверждения сохранения/регистрации — state_save_failed=True,
    STATE_SAVE_FAILED в диагностике, реестр без новых записей
    (успешного маркера нет), Telegram-уведомлений об успехе НЕТ."""
    ledger = make_ledger(env.tmp_path)
    client = FakeClient(positions={})
    monitor = ExecutionMonitor(client)
    rm = FakeRM()
    oid = "oid-I5-B"
    pos = await make_open_b(client, monitor, rm, ledger, order_id=oid,
                            trade_id="t-I5B")
    assert pos.quantity == 3

    journal_before = _journal_all_safe(ledger)
    client.uncertain_state = False
    main.state_save_failed = False

    sent = []

    async def spy_notify(text):
        sent.append(text)

    monkeypatch.setattr(main, "notify", spy_notify)

    def broken_save_state(m, r):
        return False

    monkeypatch.setattr(main, "_save_state", broken_save_state)

    res = await run_no_order_id_event(client, monitor, rm, ledger, None,
                                      trade_id="t-I5X")
    assert res is pos
    assert pos.quantity == 3 and pos.entry_applied_lots == 3
    assert pos.entry_price == 100.0 and pos.entry_order_id == oid
    assert client.uncertain_state is True

    # сохранение НЕ удалось — флаг установлен, событие диагностики записано
    assert main.state_save_failed is True, \
        "ошибка сохранения не зафиксирована (state_save_failed)"
    evs_save = [e for e in diagnostic_events(env)
                if e.get("event") == "STATE_SAVE_FAILED"]
    assert len(evs_save) == 1, "STATE_SAVE_FAILED не записан в диагностику"

    # ни одного Telegram-уведомления об успешной регистрации/сохранении
    assert sent == [], \
        f"при ошибке сохранения отправлены уведомления: {sent}"

    # реестр не получил ни одной записи (успешного маркера регистрации нет)
    assert journal_all(ledger) == journal_before
