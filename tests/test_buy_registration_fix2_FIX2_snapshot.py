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

import json

import pytest

import main
from execution import ExecutionMonitor, PositionState

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
