# main.py
# Tsclp_v2.2_volatile — основной цикл (ТОЛЬКО ПЕСОЧНИЦА).
#
# ИЗМЕНЕНИЯ (этап 4):
#   - Цены: TOP-10 + открытые позиции (объединение FIGI).
#   - prices=None -> monitoring_error, мониторинг продолжается.
#   - reconcile: двусторонний, с количествами и знаком (PositionData).
#   - process_closing: обработка UNCERTAIN через resolve_uncertain.
#   - partial BUY: cancel_order + подтверждённый финальный статус.
#   - потерянный ответ BUY: OrderResult.order_id сохраняется.
#   - _extract_price: цена за штуку (lots * lot_size).
#   - persistence (state.json): атомарная запись, обязательный reconcile.
#   - monitoring_error проверяется перед входами.
#   - trading.flag: main() НЕ создаёт автоматически.
#
# ИЗМЕНЕНИЯ (этап 4.1):
#   - trading_cycle: при status != FILL (например, NEW) — НЕ просто continue,
#     а запрос get_sandbox_order_state(order_id) и проверка lots_executed.
#     Если lots_executed > 0 -> создать TrackedPosition (как при FILL),
#     сохранить state.json, отправить POSITION_OPENED / notify.
#     Логирование ORDER_NOT_FILLED в JSONL (раньше — только logging.warning
#     в stdout, что терялось).
#
# ИЗМЕНЕНИЯ (этап 5, партия P0 — устранение потери учёта заявок и позиций):
#   ЦЕЛЬ: подтверждённое исполнение заявки не может быть потеряно; при
#   неопределённости новые BUY по соответствующему FIGI блокируются.
#
#   - OrderLedger (новый модуль order_ledger.py): order_id генерируется ДО
#     записи SIGNAL; запись INTENT — ДО сетевого вызова; если запись не
#     удалась — заявка НЕ отправляется (fail-closed). SENT — сразу после
#     ответа. Переходы — append-only с fsync.
#   - Ветки обработки BUY переписаны через ЕДИНУЮ точку регистрации
#     register_executed_buy(): тихие выходы FILL-ветки (D1) устранены —
#     при отсутствии цены исполнения или SL/TP позиция регистрируется в
#     RECOVERY (никогда не «continue молча»); цена входа НЕ заменяется
#     произвольной; расчёт SL/TP — только штатной стратегией.
#   - poll_open_orders(): пошаговый (по заявке за цикл) контроль незавершённых
#     BUY-заявок; отказ без исполнения фиксируется ТОЛЬКО при терминальном
#     статусе брокера с executed_lots==0; «не найдена»/ошибка -> UNCERTAIN,
#     НЕ отказ. При старении заявки остаток отменяется.
#   - promote_recovery_positions(): восстановление цены входа для RECOVERY
#     из статуса заявки + штатный расчёт SL/TP на свечах -> OPEN.
#     Автоматическое закрытие RECOVERY ЗАПРЕЩЕНО.
#   - reconcile(): адопция «брокер есть — локально нет» через единую точку
#     (RECOVERY), синхронизация количества, закрытие учёта при внешней
#     очистке (D2). Частичный снимок портфеля (None после fail-closed
#     в tinkoff_client) учёт не меняет.
#   - startup_recovery(): четырёхсторонняя сверка при рестарте (реестр ×
#     state.json × активные заявки брокера × позиции брокера); неизвестная
#     заявка брокера -> UNCERTAIN; подтверждённая позиция -> RECOVERY.
#   - _save_state_checked(): результат _save_state контролируется (D6);
#     при ошибке записи новые входы блокируются.
#   - close_position(): INTENT/SENT для SELL-заявок в реестре.
#   - Новые события: ORDER_INTENT, ORDER_INTENT_FAILED, ORDER_STATE_UNKNOWN,
#     POSITION_RECOVERED, POSITION_RECOVERY_RESOLVED, POSITION_RECOVERY_SYNC,
#     POSITION_RECOVERY_CLOSED, STARTUP_RECOVERY, STATE_SAVE_FAILED.
#     Существующие события (SIGNAL, POSITION_OPENED, BUY_UNKNOWN,
#     PARTIAL_FILL, ORDER_NOT_FILLED, RECONCILE_*, CLOSE_*) сохранены.
#     SIGNAL дополнен полем order_id (создаётся до записи события).
#
# ИЗМЕНЕНИЯ (партия P0-2 — поздние исполнения и промоция RECOVERY):
#   - promote_recovery_positions(): RECOVERY -> Open ТОЛЬКО при ТЕРМИНАЛЬНОМ
#     статусе входной заявки (FILL/CANCELLED/REJECTED/EXPIRED). Пока заявка
#     жива (NEW/PARTIALLYFILL), возможны дополнительные исполнения — позиция
#     остаётся в RECOVERY, количество синхронизируется опросом/reconcile.
#     Перед промоцией количество доуточняется по финальному lots_executed.
#   - register_executed_buy(): поздние дополнительные лоты ПОСЛЕ перехода
#     позиции в OPEN применяются к количеству (apply_late_entry_lots),
#     SL/TP не пересчитываются; синхронизация RECOVERY и запись в реестр
#     сохраняются немедленно.
#   - register_executed_buy(): исполнение с ПУСТЫМ (неопределённым) FIGI НЕ
#     регистрируется как обычная BUY-позиция — заявка остаётся UNCERTAIN,
#     новые BUY запрещены ГЛОБАЛЬНО до разрешения (has_unknown_figi_open_orders).
#   - register_executed_buy(): защита от «воскрешения» закрытой позиции —
#     терминальная заявка, чьё исполнение уже применено к учёту (executed_lots
#     в реестре), без активной позиции не регистрируется повторно; рост
#     lots_executed сверх учтённого обрабатывается как позднее исполнение.
#   - poll_open_orders(): повторная сверка терминальных заявок с исполнением
#     РЕАЛЬНО выполняется (order_ledger.open_orders() возвращает заявки с
#     residual-исполнением) — поздний рост lots_executed после терминального
#     CANCELLED/EXPIRED не теряется.
#   - Новые события: POSITION_ENTRY_LATE_FILL, BUY_UNKNOWN_FIGI.
#
# ИЗМЕНЕНИЯ (партия P0-3 — разделение «остаток позиции» и «исполнение заявки»):
#   Дефект: sync_recovery_quantity() использовалась для двух разных величин —
#   актуальный остаток позиции по достоверному снимку брокерского портфеля и
#   накопительное lots_executed старой BUY-заявки. Повторный ответ старой
#   заявки восстанавливал проданные оператором лоты, устаревший меньший
#   lots_executed ошибочно уменьшал позицию.
#   - register_executed_buy(): повторный ответ входной BUY-заявки для
#     RECOVERY-позиции обрабатывается методом record_recovery_entry_lots()
#     (исполнение конкретной заявки): применяются только ДЕЙСТВИТЕЛЬНО новые
#     лоты (сверх учтённых и сверх уже виденных у этой заявки по реестру);
#     повторный ответ с тем же значением ничего не меняет; устаревший
#     меньший lots_executed — противоречие: позиция не изменяется,
#     client.uncertain_state=True + заявка переводится в UNCERTAIN (новые
#     BUY по FIGI блокируются существующим механизмом реестра).
#   - promote_recovery_positions(): перед промоцией финальное накопительное
#     lots_executed больше НЕ подставляется в количество через
#     sync_recovery_quantity (тот — только для достоверных снимков
#     портфеля). Используется record_recovery_entry_lots: счётчики исполнения
#     обновляются, в позицию попадают только новые лоты, устаревший ответ —
#     противоречие (промоция отложена, UNCERTAIN). Остаток позиции остаётся
#     равным последнему достоверно подтверждённому брокерскому остатку;
#     синхронизация остатка — только по достоверным снимкам (reconcile).
#   - sync_recovery_quantity() остаётся ЕДИНСТВЕННЫМ путём актуализации
#     количества RECOVERY-позиции по ДОСТОВЕРНОМУ снимку брокерского
#     портфеля (reconcile-сверка, адопция).
#
# ИЗМЕНЕНИЯ (партия P0-4 — гонка reconcile() -> poll_open_orders() в RECOVERY):
#   Дефект: reconcile() актуализирует остаток RECOVERY-позиции по достоверному
#   снимку портфеля (8 -> 7), счётчики entry_applied_lots остаются 8; затем в
#   том же цикле poll_open_orders() получает выросший накопительный
#   lots_executed=10 той же заявки и прибавлял дельту (10-8=2) к остатку —
#   7 -> 9, хотя дельта уже была отражена в снимке брокера. Порядок вызовов
#   process_closing()/reconcile()/poll_open_orders() НЕ меняется: защита
#   делается на уровне данных.
#   - register_executed_buy(): рост lots_executed сверх учтённых ПОСЛЕ
#     достоверной синхронизации остатка снимком (entry_snapshot_synced)
#     НЕОДНОЗНАЧЕН: дельта к остатку НЕ добавляется (снимок мог уже учесть
#     это исполнение), счётчики догоняются по отчёту заявки; неопределённость
#     фиксируется существующей архитектурой (client.uncertain_state +
#     UNCERTAIN-запись реестра — новые BUY по FIGI блокированы), остаток
#     актуализируется сверкой reconcile.
#   - promote_recovery_positions(): тот же рост откладывает промоцию до
#     сверки — RECOVERY -> OPEN выполняется только с остатком,
#     подтверждённым достоверным снимком; количество не уменьшается,
#     противоречия (устаревший ответ) обрабатываются как прежде.
#   - Порядок вызовов и остальные ветки жизненного цикла process_closing(),
#     reconcile() и trading_cycle() не менялись.
#
# ИЗМЕНЕНИЯ (партия P0-5 — недопущение снятия UNCERTAIN повторным опросом):
#   Дефект: после неоднозначного роста lots_executed заявка переводится в
#   UNCERTAIN (реестр + client.uncertain_state). При ПОВТОРНОМ опросе с тем
#   же значением lots_executed record_recovery_entry_lots() возвращает False
#   и snapshot_covered_growth становится False — срабатывал общий блок
#   идемпотентности register_executed_buy() и ledger.record_status() писал
#   терминальный статус CANCELLED/EXPIRED/FILL ПОВЕРХ UNCERTAIN-записи:
#   неопределённость снималась и блокировка новых BUY по FIGI отпускалась
#   ДО подтверждённой сверки портфеля.
#   - register_executed_buy(): обычный повторный отчёт с тем же
#     lots_executed (состояние учёта не изменилось) НЕ перезаписывает
#     действующую UNCERTAIN-запись и НЕ пишет терминальный статус;
#     UNCERTAIN сохраняется в реестре до успешного разрешения;
#     то же — для ветки реально применённого позднего исполнения при
#     действующей UNCERTAIN (исполнение применяется, терминальный статус
#     не пишется до сверки).
#   - reconcile(): УСПЕШНАЯ сверка (достоверный снимок портфеля, без
#     расхождений) выставляет маркер _last_reconcile_confirmed_utc —
#     подтверждённое безопасное разрешение неопределённостей реестра,
#     появившихся ДО этого момента. Только после него повторный терминальный
#     отчёт заявки, совпадающий с учтённым исполнением, штатно закрывает
#     UNCERTAIN-запись и снимает блокировку FIGI. Ошибка/расхождение
#     сверки маркер НЕ выставляют — защита не ослабляется при
#     противоречивом или недоступном снимке портфеля.
#   - Количество позиции повторным отчётом не меняется (record_recovery_
#     entry_lots при том же значении — no-op); стратегия, сигналы, риск-
#     модель и правила открытия не затронуты.
#
# ИЗМЕНЕНИЯ (исправление потери первичной регистрации исполненных BUY):
#   Дефект №1 (блокировка первичной регистрации): guard register_executed_buy
#   трактовал терминальную запись реестра (FILL с executed_lots=N, которую
#   record_sent() пишет В trading_cycle ДО регистрации позиции) как
#   доказательство применения лотов к учёту. На момент вызова позиции ещё
#   нет (не has_active_position) и executed_lots <= recorded_le — функция
#   возвращала None: купленная позиция НЕ регистрировалась, штатное
#   уведомление об открытии не отправлялось, бумаги обнаруживались только
#   поздней адопцией reconcile как RECOVERY. Логика исправления:
#   - Блокировка ТОЛЬКО при положительном доказательстве применения:
#     (а) есть запись позиции (в т.ч. CLOSED) с entry_order_id == order_id
#     и entry_applied_lots >= executed_lots; или (б) order_id в компактном
#     реестре закрытых входных заявок (closed_entry_order_ids в
#     execution.py/state.json — признак закрытой сделки, переживает
#     перезапуск и перезапись CLOSED-записи новой позицией по тому же FIGI).
#   - Отсутствие активной позиции и наличие FILL в реестре НЕ считаются
#     доказательством применения лотов: первичная регистрация разрешена.
#   - Заявка, относящаяся к закрытой позиции (entry_order_id совпал или
#     есть в реестре закрытых), НЕ воскрешает позицию; рост executed_lots
#     сверх учтённого при закрытой сделке — UNCERTAIN + сверка (не новая
#     позиция, не дельта).
#   - startup_recovery(): добавлен второй проход — терминальные FILL-заявки
#     с подтверждённым исполнением БЕЗ признака применения к позиции
#     (крах между record_sent и регистрацией) доводятся до регистрации
#     через ту же единую точку: после перезапуска позиция создаётся
#     с правильными FIGI/количеством/ценой/entry_order_id вместо
#     слепой адопции RECOVERY. Заявки с признаком применения
#     (reason содержит «позиция зарегистрирована»/«исполнение применено»)
#     не переобрабатываются; идемпотентность обеспечивает guard.
#
#   Дефект №2 (слепая привязка RECOVERY): привязка entry_order_id к
#   RECOVERY-позиции выполнялась по одному совпадению FIGI, без проверки
#   принадлежности заявки. Поздний отчёт ЧУЖОЙ (старой) заявки привязывался
#   к позиции и её исполнение применялось к количеству. Исправление:
#   привязка разрешена ТОЛЬКО если оба trade_id известны и совпадают;
#   при несовпадении/отсутствии trade_id заявка НЕ привязывается и её
#   исполнение НЕ применяется к позиции — фиксируется неопределённость
#   существующей архитектурой (client.uncertain_state + UNCERTAIN-запись
#   реестра), разрешается сверкой/оператором.
#
# ИЗМЕНЕНИЯ (дополнительный этап — целостность реестра и state.json):
#   1. Архив закрытых входных заявок БЕЗ лимита (execution.py: удалён
#      MAX_CLOSED_ENTRY_ORDER_IDS=200): FIFO-обрезка вытесняла признаки
#      СТАРЫХ закрытых сделок — после 200 закрытий поздний residual-отчёт
#      вытесненной заявки не блокировался guard'ом («воскрешение»).
#      Запись архива — только короткий order_id, лимит не нужен.
#   2. Подтверждение регистрации в реестре пишется ТОЛЬКО после успешного
#      сохранения state.json (register_executed_buy): reason-маркер
#      «позиция зарегистрирована»/«исполнение применено» используется
#      startup_recovery как признак «применение уже учтено», поэтому
#      подтверждение несохранённого состояния приводило к потере позиции
#      при перезапуске. Ветка позднего роста: сохранение перенесено ДО
#      записи подтверждения; при сбое сохранения пишется UNCERTAIN-запись
#      (заявка остаётся доступной опросу, дельта применяется повторно).
#   3. Активная RECOVERY-позиция с УЖЕ заданным entry_order_id: отчёт
#      ДРУГОЙ заявки (несовпадение entry_order_id) явно фиксируется как
#      UNCERTAIN (client.uncertain_state + UNCERTAIN-запись реестра);
#      исполнение НЕ применяется, чужая заявка НЕ отмечается обработанной
#      («позиция уже зарегистрирована — идемпотентность» не пишется).
#      Совпадение trade_id само по себе перепривязку не выполняет.
#
# ИЗМЕНЕНИЯ (этап BUY REGISTRATION FIX 2 — OPEN order_id mismatch +
#            корректный Telegram при ошибке сохранения state.json):
#   1. register_executed_buy(), ветка активной OPEN-позиции: проверка
#      принадлежности исполнения СВОЕЙ заявке. Раньше отчёт старой/чужой
#      заявки A (тот же FIGI, другой order_id) проваливался в общий блок
#      идемпотентности: apply_late_entry_lots() отказывал (order_id не
#      совпадает), но заявка A помечалась обработанной — писалcя
#      терминальный статус с маркером «позиция уже зарегистрирована —
#      идемпотентность» (или молча пропускался уже терминальный отчёт),
#      хотя исполнение A к позиции НЕ применялось. Теперь несовпадение
#      order_id (и неизвестный entry_order_id позиции) — явная
#      неопределённость: client.uncertain_state + UNCERTAIN-запись реестра
#      с причиной "order_id mismatch" и диагностикой (FIGI, order_id
#      отчёта, entry_order_id позиции, trade_id, количество исполнения);
#      лоты НЕ применяются, цена входа НЕ изменяется, успешный маркер НЕ
#      пишется; совпадение trade_id применением не разрешается; новые BUY
#      по FIGI блокированы существующей архитектурой до устранения;
#      повторная обработка идемпотентна (неопределённость самопроизвольно
#      не снимается).
#   2. register_executed_buy(), уведомления после регистрации: сообщение
#      «Позиция зарегистрирована как RECOVERY — контроль сохранён...»
#      отправлялось безусловно, даже если _save_state_checked() вернул
#      False. Теперь при state_saved=False отправляется отдельное
#      критическое предупреждение (позиция только в оперативной памяти,
#      сохранение state.json НЕ подтверждено, новые BUY заблокированы);
#      обычное уведомление RECOVERY и уведомление первичной регистрации
#      BUY дополняются предупреждением о несохранённом состоянии.
#      Механизм _save_state_checked() не менялся.
#
# ИЗМЕНЕНИЯ (этап BUY REGISTRATION FIX 3 — исполнение BUY без order_id):
#   register_executed_buy(), ветка активной позиции: проверка
#   принадлежности исполнения (FIX 2) выполнялась только при непустом
#   order_id — при order_id=None/""/пробелах она пропускалась, и
#   apply_late_entry_lots() (execution.py допускает order_id=None)
#   применял дельту к уже существующей OPEN-позиции БЕЗ подтверждения,
#   что исполнение относится именно к этой позиции (та же дыра у
#   record_recovery_entry_lots() для RECOVERY-позиций). Теперь
#   отсутствие достоверного order_id при активной позиции по тому же
#   FIGI — явная неопределённость: client.uncertain_state=True (новые
#   BUY запрещены до устранения), лоты НЕ применяются (quantity/
#   entry_price/entry_applied_lots не изменяются), успешный маркер
#   регистрации НЕ пишется; диагностическое событие с причиной
#   отсутствия order_id пишется в research_events.jsonl (append-only).
#   UNCERTAIN-запись реестра не пишется: record_uncertain() адресуется
#   order_id, фиктивные идентификаторы запрещены (согласовано с веткой
#   «исполнение без FIGI»: ledger пишется только при непустом order_id).
#   Повторная обработка того же события идемпотентна. execution.py не
#   менялся: безопасность гарантирована на уровне вызывающего кода
#   (register_executed_buy), до вызова apply_late_entry_lots().
#
# НЕ ИЗМЕНЕНО: стратегия (MA(9/21), фильтры, calculate_sl_tp), параметры
#   риска, Telegram-lifecycle, trading.flag логика, process_closing,
#   resolve_closing/resolve_uncertain (execution.py — см. его шапку),
#   порядок вызовов trading_cycle/reconcile/poll_open_orders.
#   (_load_state изменена ТОЛЬКО добавлением восстановления ключа
#   closed_entry_order_ids — см. блок выше.) Режим — ТОЛЬКО ПЕСОЧНИЦА
#   (SANDBOX).
#
# ИЗМЕНЕНИЯ (v2.3.0-stage1 — пакет В: самовосстановление учёта; + С1):
#   В1. state.json сохраняется В КОНЦЕ КАЖДОГО цикла trading_cycle.
#       Раньше тихий цикл (HOLD, без входов/ошибок) не сохранял
#       CLOSED/CLOSING-переходы: рестарт возвращал проданные позиции как
#       фантомные OPEN -> вечный RECONCILE_MISMATCH (главный генератор
#       «локальных расхождений»).
#   В2. reconcile: локальная активная позиция отсутствует у брокера, но есть
#       СВОЯ SELL-заявка закрытия (pos.close_order_id или запись реестра,
#       созданная ПОСЛЕ входа) -> учёт закрывается штатно
#       (POSITION_CLOSED_BY_LEDGER), CSV-строка пишется с реальной ценой
#       исполнения. Окна «закрытие офлайн / крах между отправкой SELL и
#       сохранением» самоизлечиваются без оператора.
#   В3. reconcile: та же ситуация БЕЗ своей SELL-заявки (позиция продана
#       вне робота) -> двойное подтверждение на двух последовательных
#       сверках -> учёт закрывается как EXTERNAL_CLOSE с приблизительной
#       ценой выхода на момент обнаружения (POSITION_CLOSED_EXTERNAL,
#       CSV с пометкой в error_note). Вечная заморозка при ручной продаже
#       устранена; расхождения по количеству/направлению по-прежнему
#       замораживают входы (там неоднозначность настоящая).
#   В4. RECOVERY-позиция без entry_order_id (адоптированная) привязывается
#       к ЕДИНСТВЕННОМУ терминальному BUY-отчёту по FIGI с
#       executed_lots == quantity (RECOVERY_ENTRY_BOUND); UNCERTAIN-запись
#       этой заявки снимается, промоция RECOVERY->OPEN идёт штатно.
#       Защита от «воскрешения» сохранена (closed_entry_order_ids).
#   С1. Безубыток/трейлинг (config.BE_TRAIL_ENABLED=False по умолчанию):
#       свечные экстремумы MAE/MFE -> триггер BE по r_peak от достигнутого
#       максимума -> трейлинг; классификация выходов SL/BE/TRAIL; новые
#       поля CSV с авто-миграцией старого research_trades.csv в .bak.
#       Порядок вызовов trading_cycle/reconcile/poll_open_orders НЕ менялся.

import asyncio
import csv
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from t_tech.invest import (
    CandleInterval,
    OrderDirection,
    OrderExecutionReportStatus,
)

import config
import logger as research_logger
from order_ledger import OrderLedger, STATUS_FILL, STATUS_UNCERTAIN, utc_now_iso
from risk_manager import RiskManager
from strategy import StrategyManager
from tinkoff_client import TinkoffClient, PositionData
from execution import ExecutionMonitor, PositionState, ACTIVE_STATES
from screener import Screener

if config.ENABLE_TELEGRAM:
    from telegram_bot import TelegramBot
else:
    TelegramBot = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._utils.networkloop").setLevel(logging.WARNING)

TRADING_FLAG = "trading.flag"
STATE_FILE = "state.json"
STATE_FILE_TMP = "state.json.tmp"

# Этап 5: реестр заявок (рядом с state.json)
LEDGER_FILE = "order_ledger.jsonl"
# Этап 5: возраст нетерминальной заявки (в циклах), после которого остаток отменяется.
# ~5 минут при TRADING_CYCLE_SECONDS=30. Задержка исполнения — штатный сценарий;
# заявка не выбрасывается из учёта, а доводится до терминального статуса.
ORDER_POLL_MAX_AGE_CYCLES = 10

# Маппинг кодов статусов (верифицирован по документации T-Invest API:
# FILL=1, REJECTED=2, CANCELLED=3, NEW=4, PARTIALLYFILL=5; 6 — EXPIRED defensively)
_STATUS_NAME_BY_CODE = {
    1: "FILL", 2: "REJECTED", 3: "CANCELLED", 4: "NEW", 5: "PARTIALLYFILL", 6: "EXPIRED",
}
_TERMINAL_ORDER_STATUSES = {"FILL", "REJECTED", "CANCELLED", "EXPIRED"}


def _status_name(raw_status):
    """OrderExecutionReportStatus/число -> 'FILL'|'REJECTED'|'CANCELLED'|'NEW'|'PARTIALLYFILL'|'EXPIRED'|None."""
    if raw_status is None:
        return None
    try:
        code = int(raw_status)
    except (TypeError, ValueError):
        return None
    return _STATUS_NAME_BY_CODE.get(code)


def _age_in_cycles(rec) -> float:
    """
    Возраст записи реестра в циклах (для решения об отмене остатка).

    Дефект №3: возраст считается от НЕИЗМЕНЯЕМОГО времени создания заявки
    (created_at_utc — время записи INTENT), а не от timestamp_utc, который
    record_status() обновляет при каждом опросе. Обновление статуса NEW
    не сбрасывает возраст; после перезапуска возраст восстанавливается
    из реестра (created_at_utc хранится в журнале).
    """
    try:
        t0_raw = rec.get("created_at_utc") or rec.get("timestamp_utc")
        t0 = datetime.fromisoformat(t0_raw)
        return (datetime.now(timezone.utc) - t0).total_seconds() / max(1, config.TRADING_CYCLE_SECONDS)
    except Exception:
        return 0.0


def _uncertainty_resolved_by_reconcile(rec: dict) -> bool:
    """
    Партия P0-5: разрешена ли UNCERTAIN-запись заявки ПОДТВЕРЖДЁННОЙ сверкой.

    True ТОЛЬКО если после появления UNCERTAIN-записи (updated_at_utc) был
    успешный reconcile(): достоверный снимок портфеля без расхождений
    (маркер _last_reconcile_confirmed_utc). Такая сверка подтверждает
    фактический остаток у брокера, после чего повторный терминальный отчёт
    заявки, совпадающий с учтённым исполнением, штатно закрывает запись и
    снимает блокировку новых BUY по FIGI.

    Любая НОВАЯ UNCERTAIN-запись, появившаяся ПОСЛЕ последней успешной
    сверки (updated_at_utc больше маркера), считается неразрешённой —
    повторные отчёты её не снимают. Ошибка/противоречие сверки маркер
    не выставляют; неразборчивые/несравнимые метки времени трактуются
    консервативно (не разрешена).
    """
    if _last_reconcile_confirmed_utc is None:
        return False
    rec_ts = rec.get("updated_at_utc") or rec.get("timestamp_utc")
    if not rec_ts:
        return False
    try:
        rec_dt = datetime.fromisoformat(str(rec_ts))
        mark_dt = datetime.fromisoformat(str(_last_reconcile_confirmed_utc))
        return rec_dt <= mark_dt
    except (TypeError, ValueError):
        return False


top_instruments = []
telegram_bot = None
main_client = None
reconcile_done = False
monitor_ref = None
rm_ref = None

# Этап 5: реестр заявок и флаг ошибки сохранения state.json
ledger_ref = None
state_save_failed = False

# Партия P0-5: UTC-момент последней УСПЕШНОЙ сверки reconcile() (достоверный
# снимок портфеля, без расхождений) — маркер «подтверждённого безопасного
# разрешения» неопределённостей реестра, появившихся до этого момента.
# None — подтверждённой сверки ещё не было: UNCERTAIN-записи не разрешаются
# повторными отчётами (блокировки сохраняются). Выставляется ТОЛЬКО в
# reconcile() при полном успехе; ошибки/расхождения маркер не трогают.
_last_reconcile_confirmed_utc = None

_strategy_event_keys = set()
_promote_throttle = {"n": 0, "alive": 0}

# Пакет В3: кандидаты на внешнее закрытие (figi -> UTC-время первого
# обнаружения). НЕ персистится намеренно: после рестарта двойное
# подтверждение начинается заново (два последовательных достоверных снимка).
_external_close_candidates = {}


def is_trading_enabled() -> bool:
    return os.path.exists(TRADING_FLAG)


def start_trading():
    try:
        with open(TRADING_FLAG, "w", encoding="utf-8") as f:
            f.write(datetime.now(timezone.utc).isoformat())
        logging.info("✅ Торговля ВКЛЮЧЕНА (создан trading.flag)")
        research_logger.log_event("TRADING_START", {})
    except Exception as e:
        logging.error(f"Не удалось создать {TRADING_FLAG}: {type(e).__name__}: {e}")


def stop_trading():
    try:
        if os.path.exists(TRADING_FLAG):
            os.remove(TRADING_FLAG)
        logging.info("🛑 Торговля ВЫКЛЮЧЕНА (удалён trading.flag)")
        research_logger.log_event("TRADING_STOP", {})
    except Exception as e:
        logging.error(f"Не удалось удалить {TRADING_FLAG}: {type(e).__name__}: {e}")


def get_status():
    instruments = ", ".join(i['ticker'] for i in top_instruments) or "—"
    monitored = ", ".join(
        p.ticker for p in (monitor_ref.positions.values() if monitor_ref else [])
        if p.state in ACTIVE_STATES
    ) or "—"
    uncertain = "ДА" if (main_client and main_client.is_uncertain()) else "нет"
    trading = "АКТИВНА" if is_trading_enabled() else "СТОП"
    tg_status = "работает" if telegram_bot else "НЕДОСТУПЕН"
    monitoring_err = "ДА" if (monitor_ref and monitor_ref.monitoring_error) else "нет"
    open_orders = ledger_ref.open_count() if ledger_ref else 0
    save_err = "ДА" if state_save_failed else "нет"
    ledger_ok = ledger_ref.is_reliable() if ledger_ref else True
    ledger_ok_text = "OK" if ledger_ok else "НЕДОСТОВЕРЕН — новые BUY запрещены"
    recovery = ", ".join(
        f"{p.ticker} ({p.recovery_source or '?'})"
        for p in (monitor_ref.positions.values() if monitor_ref else [])
        if p.state == PositionState.RECOVERY
    ) or "—"
    return (
        f"🤖 Статус\n"
        f"Торговля: {trading}\n"
        f"Telegram: {tg_status}\n"
        f"Режим: SANDBOX\n"
        f"Сверка: {'OK' if reconcile_done else 'НЕ ЗАВЕРШЕНА'}\n"
        f"Неопределённость: {uncertain}\n"
        f"Ошибка мониторинга цен: {monitoring_err}\n"
        f"Ошибка сохранения state: {save_err}\n"
        f"Реестр заявок: {ledger_ok_text}\n"
        f"Незавершённых заявок: {open_orders}\n"
        f"RECOVERY: {recovery}\n"
        f"Для новых входов ({len(top_instruments)}): {instruments}\n"
        f"Под мониторингом ({monitor_ref.active_count() if monitor_ref else 0}): {monitored}"
    )


async def notify(text: str):
    logging.info(f"[notify] {text}")
    if telegram_bot:
        try:
            await telegram_bot.send_message(text)
        except Exception as e:
            logging.warning(f"Telegram send error: {type(e).__name__}: {e}")


async def _stop_telegram_bot():
    global telegram_bot
    if telegram_bot is None:
        return
    try:
        try:
            await asyncio.wait_for(telegram_bot.application.updater.stop(), timeout=10.0)
        except Exception as e:
            logging.warning(f"[tg] updater.stop(): {type(e).__name__}: {e}")
        try:
            await asyncio.wait_for(telegram_bot.application.stop(), timeout=10.0)
        except Exception as e:
            logging.warning(f"[tg] application.stop(): {type(e).__name__}: {e}")
        try:
            await asyncio.wait_for(telegram_bot.application.shutdown(), timeout=10.0)
        except Exception as e:
            logging.warning(f"[tg] application.shutdown(): {type(e).__name__}: {e}")
        logging.info("[tg] Telegram-бот остановлен.")
    except Exception as e:
        logging.warning(f"[tg] Ошибка остановки: {type(e).__name__}: {e}")
    finally:
        telegram_bot = None


async def _try_create_telegram_bot() -> bool:
    global telegram_bot
    if not (config.ENABLE_TELEGRAM and TelegramBot):
        return False
    try:
        bot = TelegramBot(
            config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID,
            config.TELEGRAM_ALLOWED_USER_ID,
            {'start_trading': start_trading, 'stop_trading': stop_trading, 'get_status': get_status},
        )
        await asyncio.wait_for(bot.application.initialize(), timeout=20.0)
        await asyncio.wait_for(bot.application.start(), timeout=20.0)
        await asyncio.wait_for(bot.application.updater.start_polling(), timeout=20.0)
        await bot.register_commands()
        telegram_bot = bot
        logging.info("[tg] Telegram-бот создан и запущен (initialize → start → polling).")
        await notify("🤖 Tsclp_v2.2_volatile: Telegram восстановлен.")
        return True
    except Exception as e:
        logging.warning(f"[tg] Не удалось создать Telegram-бот: {type(e).__name__}: {e!r}")
        await _stop_telegram_bot()
        return False


async def _telegram_watchdog():
    if not (config.ENABLE_TELEGRAM and TelegramBot):
        return
    failures = 0
    while True:
        await asyncio.sleep(60)
        if telegram_bot is None:
            await _try_create_telegram_bot()
            continue
        try:
            await asyncio.wait_for(telegram_bot.application.bot.get_me(), timeout=15.0)
            if failures > 0:
                logging.info(f"[tg-watchdog] Telegram восстановлен после {failures} неудач.")
            failures = 0
        except Exception as e:
            failures += 1
            logging.warning(f"[tg-watchdog] Telegram недоступен ({failures}/3): {type(e).__name__}: {e!r}")
            if failures >= 3:
                logging.info("[tg-watchdog] Полный перезапуск Telegram...")
                await _stop_telegram_bot()
                failures = 0


def _trade_id_already_logged(trade_id: str) -> bool:
    if not trade_id:
        return False
    try:
        if not os.path.isfile(research_logger.TRADES_FILE):
            return False
        with open(research_logger.TRADES_FILE, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                if row.get("trade_id") == trade_id:
                    return True
        return False
    except Exception as e:
        logging.warning(f"[csv] Не удалось проверить trade_id {trade_id}: {type(e).__name__}: {e}")
        return True


def _timestamp_for_log(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _log_strategy_diagnostic(
    event_type: str,
    ticker: str,
    figi: str,
    signal: str,
    reason: str,
    sm: StrategyManager,
    df: pd.DataFrame,
    price: float,
):
    try:
        candle_time = getattr(sm, "last_signal_candle_time", None)
        if candle_time is None and df is not None and not df.empty:
            candle_time = df.index[-1]
        candle_time_text = _timestamp_for_log(candle_time)
        if candle_time_text is None:
            candle_time_text = "UNKNOWN"
        event_key = (event_type, ticker, candle_time_text)
        if event_key in _strategy_event_keys:
            return

        last_close = None
        last_volume = None
        if df is not None and not df.empty:
            if "close" in df.columns:
                value = df["close"].iloc[-1]
                if pd.notna(value):
                    last_close = float(value)
            if "volume" in df.columns:
                value = df["volume"].iloc[-1]
                if pd.notna(value):
                    last_volume = float(value)

        adx_value = getattr(sm, "last_adx_value", None)
        if adx_value is not None:
            try:
                adx_value = float(adx_value)
            except (TypeError, ValueError):
                adx_value = None

        payload = {
            "ticker": ticker, "figi": figi, "signal": signal, "reason": reason,
            "signal_candle_time": candle_time_text,
            "candles_count": int(len(df)) if df is not None else 0,
            "regime": getattr(sm, "_last_regime", None),
            "regime_candidate": getattr(sm, "_last_regime_candidate", None),
            "regime_streak": getattr(sm, "_regime_streak", None),
            "adx": adx_value, "price": float(price),
            "last_close": last_close, "last_volume": last_volume,
        }
        if research_logger.log_event(event_type, payload):
            _strategy_event_keys.add(event_key)
    except Exception as e:
        logging.warning(
            f"[strategy-log] Не удалось записать {event_type} "
            f"для {ticker}: {type(e).__name__}: {e}"
        )


# ============================================================
# С1: СВЕЧНЫЕ ЭКСТРЕМУМЫ И ATR ДЛЯ БЕЗУБЫТКА/ТРЕЙЛИНГА
# (используются ТОЛЬКО при config.BE_TRAIL_ENABLED=True;
#  в базовой поставке выключено — ноль доп. вызовов API)
# ============================================================

def _parse_iso_ts(value):
    """ISO-строка -> datetime (UTC-aware) или None."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _atr_from_df(df: pd.DataFrame, period: int):
    """ATR(period) по 5-минутным свечам (среднее True Range за period)."""
    try:
        if df is None or df.empty or not {"high", "low", "close"}.issubset(df.columns):
            return None
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float)
        prev_close = close.shift(1)
        tr = pd.concat(
            [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1).dropna()
        if period <= 0 or len(tr) < period:
            return None
        value = float(tr.tail(period).mean())
        return value if value > 0 else None
    except Exception as e:
        logging.warning(f"[stage1] ATR не рассчитан: {type(e).__name__}: {e}")
        return None


async def _fetch_extremes_and_atrs(client: TinkoffClient, figis):
    """Свечные экстремумы (high/low последней 5-мин свечи — кумулятивны
    внутри бара, поэтому поллинг 30 с их не пропускает) и ATR по OPEN-позициям."""
    extremes, atrs = {}, {}
    for figi in figis:
        try:
            df = await _build_candles_df(client, figi)
        except Exception as e:
            logging.warning(f"[stage1] свечи {figi}: {type(e).__name__}: {e}")
            continue
        if df is None or df.empty:
            continue
        last = df.iloc[-1]
        try:
            hi, lo = float(last["high"]), float(last["low"])
            if hi > 0 and lo > 0:
                extremes[figi] = (hi, lo)
        except (KeyError, TypeError, ValueError):
            pass
        atr = _atr_from_df(df, config.TRAIL_ATR_PERIOD)
        if atr:
            atrs[figi] = atr
    return extremes, atrs


# ============================================================
# PERSISTENCE (state.json)
# ============================================================

def _save_state(monitor: ExecutionMonitor, rm: RiskManager) -> bool:
    try:
        export = monitor.export_state()
        data = {
            "positions": export.get("positions", {}),
            # Исправление (дефект №1): компактный реестр закрытых входных
            # заявок — признак «BUY заявка применена и её позиция закрыта»
            # переживает перезапуск и перезапись CLOSED-записи новой
            # позицией по тому же FIGI; используется guard
            # register_executed_buy для запрета «воскрешения».
            "closed_entry_order_ids": export.get("closed_entry_order_ids", []),
            "monitoring_error": monitor.monitoring_error,
            "risk": rm.export_state(),
            # Этап 5: краткий срез реестра заявок для диагностики
            # (источник истины — order_ledger.jsonl, здесь только срез)
            "open_orders": ledger_ref.export_summary() if ledger_ref else [],
        }
        with open(STATE_FILE_TMP, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(STATE_FILE_TMP, STATE_FILE)
        return True
    except Exception as e:
        logging.error(f"[state] Ошибка сохранения: {type(e).__name__}: {e}")
        return False


def _save_state_checked(monitor: ExecutionMonitor, rm: RiskManager) -> bool:
    """
    Этап 5 (D6): результат сохранения контролируется. При ошибке записи
    новые входы блокируются (state_save_failed) до успешного сохранения —
    незаписанное состояние не должно дополняться новыми рисками.
    """
    global state_save_failed
    ok = _save_state(monitor, rm)
    if ok:
        state_save_failed = False
    else:
        if not state_save_failed:
            research_logger.log_event("STATE_SAVE_FAILED", {})
        state_save_failed = True
        logging.error("[state] _save_state не удался — новые входы заблокированы до успешного сохранения")
    return ok


def _load_state(monitor: ExecutionMonitor, rm: RiskManager) -> bool:
    if not os.path.isfile(STATE_FILE):
        logging.warning(f"[state] {STATE_FILE} отсутствует — новые входы запрещены до сверки.")
        return False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logging.error("[state] Корневой объект не dict")
            return False
        positions = data.get("positions", {})
        if not isinstance(positions, dict):
            logging.error("[state] positions не dict")
            return False
        monitor.import_state({
            "positions": positions,
            "monitoring_error": bool(data.get("monitoring_error", False)),
            # Исправление (дефект №1): восстановление компактного реестра
            # закрытых входных заявок. Старый state.json без ключа
            # восстанавливается с пустым списком (обратная совместимость).
            "closed_entry_order_ids": data.get("closed_entry_order_ids", []),
        })
        risk = data.get("risk", {})
        if isinstance(risk, dict):
            rm.import_state(risk)
        logging.info(f"[state] Загружено позиций: {len(monitor.positions)}")
        return True
    except Exception as e:
        logging.error(f"[state] Ошибка загрузки: {type(e).__name__}: {e}")
        return False


# ============================================================
# ПАКЕТ В2/В3: САМОВОССТАНОВЛЕНИЕ УЧЁТА ПРИ ОТСУТСТВИИ ПОЗИЦИИ У БРОКЕРА
# ============================================================

def _ledger_last_buy_time(ledger: Optional[OrderLedger], figi: str):
    """Время последней исполненной BUY-заявки по FIGI (нижняя граница для
    «своя SELL-закрытие постфактум входа»)."""
    if ledger is None:
        return None
    latest = None
    for rec in ledger.latest_all().values():
        if rec.get("side") != "BUY" or rec.get("figi") != figi:
            continue
        try:
            executed = int(rec.get("executed_lots") or 0)
        except (TypeError, ValueError):
            executed = 0
        if executed <= 0:
            continue
        for key in ("updated_at_utc", "created_at_utc", "timestamp_utc"):
            dt = _parse_iso_ts(rec.get(key))
            if dt and (latest is None or dt > latest):
                latest = dt
    return latest


def _match_own_close_sell(ledger: Optional[OrderLedger], pos) -> list:
    """Пакет В2: кандидаты «своя SELL-заявка закрытия» для позиции (лучшие
    первыми). Доказательства принадлежности роботу (по убыванию силы):
      1) pos.close_order_id — заявка закрытия записана в самой позиции
         (close_position пишет её всегда); запись реестра при наличии —
         берётся из неё;
      2) запись реестра: обратная сторона, тот же FIGI, requested_lots > 0,
         создана ПОСЛЕ входа позиции (order_send_time/signal_time, а для
         позиций без них — после последней исполненной BUY по FIGI).
    Возвращает список записей реестра (или одну синтезированную для случая
    close_order_id без записи — старый формат state.json)."""
    close_side = "SELL" if pos.direction == "BUY" else "BUY"
    # 1) Прямое доказательство — активная заявка закрытия в позиции.
    if pos.close_order_id:
        oid = str(pos.close_order_id)
        rec = ledger.latest(oid) if ledger is not None else None
        if rec is not None and rec.get("side") == close_side and rec.get("figi") == pos.figi:
            return [rec]
        # Записи в реестре нет (старый формат state.json): поле позиции —
        # доказательство принадлежности по построению. Статус/исполнение
        # уточняются у брокера в _try_resolve_absent_position.
        return [{
            "order_id": oid, "figi": pos.figi, "side": close_side,
            "requested_lots": int(pos.close_requested_lots or pos.quantity or 0),
            "executed_lots": int(pos.close_applied_lots or 0),
            "avg_exec_price": None, "status": "UNKNOWN",
            "reason": f"close:{pos.close_reason or ''}",
            "created_at_utc": pos.close_requested_at,
            "updated_at_utc": pos.close_requested_at,
        }]
    # 2) Поиск в реестре — для краха между отправкой SELL и сохранением
    #    state.json (close_order_id не успел попасть в state).
    if ledger is None:
        return []
    entry_floor = (_parse_iso_ts(pos.order_send_time)
                   or _parse_iso_ts(pos.signal_time)
                   or _ledger_last_buy_time(ledger, pos.figi))
    if entry_floor is None:
        return []
    candidates = []
    for rec in ledger.latest_all().values():
        if rec.get("side") != close_side or rec.get("figi") != pos.figi:
            continue
        try:
            requested = int(rec.get("requested_lots") or 0)
            executed = int(rec.get("executed_lots") or 0)
        except (TypeError, ValueError):
            continue
        if requested <= 0:
            continue
        rec_dt = (_parse_iso_ts(rec.get("created_at_utc"))
                  or _parse_iso_ts(rec.get("timestamp_utc")))
        if rec_dt is None or rec_dt <= entry_floor:
            continue  # заявка старее входа — относится к предыдущей сделке
        full = requested > 0 and executed >= requested
        candidates.append((
            (1 if full else 0, 1 if executed > 0 else 0, rec_dt.timestamp()),
            rec,
        ))
    candidates.sort(key=lambda x: x[0], reverse=True)
    return [rec for _, rec in candidates]


def _finalize_position_closed(pos, *, sold_lots: int, exit_price,
                              close_reason: str, order_id: str,
                              requested_at: str, note: str):
    """Пакет В2/В3: единая точка закрытия учёта позиции, когда брокерская
    заявка не может быть обработана штатно (позиция уже отсутствует у
    брокера). CSV-строка будет записана штатным process_closing (logged=False);
    note попадает в error_note CSV (динамический атрибут, не персистится)."""
    sold = max(1, int(sold_lots))
    price = None
    try:
        p = float(exit_price) if exit_price is not None else None
        price = p if (p is not None and p > 0) else None
    except (TypeError, ValueError):
        price = None
    pos.quantity = 0
    pos.executed_lots = sold
    pos.execution_price = price
    pos.close_reason = close_reason
    pos.close_order_id = order_id or None
    pos.close_requested_lots = sold
    pos.close_requested_at = requested_at or datetime.now(timezone.utc).isoformat()
    pos.close_price_before_order = price if price is not None else 0.0
    pos.partial = False
    pos.logged = False
    pos.state = PositionState.CLOSED
    pos.error_note = note or ""
    return pos


async def _try_resolve_absent_position(client: TinkoffClient,
                                       ledger: Optional[OrderLedger], pos) -> str:
    """Пакет В2/В3 для ветки reconcile «активная позиция локально,
    у брокера отсутствует» (достоверный снимок).

    Возвращает:
      "ledger_close"   — учёт закрыт по СВОЕЙ SELL-заявке (В2);
      "external_close" — учёт закрыт как внешнее (ручное) закрытие после
                         двойного подтверждения (В3);
      "candidate"      — первое обнаружение: учёт НЕ менялся, подтверждение
                         — на следующей сверке;
      "mismatch"       — разрешить не удалось: прежнее поведение
                         (RECONCILE_MISMATCH + блокировка новых входов).
    """
    ticker = pos.ticker
    # --- В2: своя SELL-заявка закрытия ---
    for rec in _match_own_close_sell(ledger, pos):
        oid = str(rec.get("order_id") or "")
        if not oid:
            continue
        try:
            executed = int(rec.get("executed_lots") or 0)
            requested = int(rec.get("requested_lots") or 0)
        except (TypeError, ValueError):
            executed, requested = 0, 0
        status = rec.get("status")
        price = None
        rec_price = rec.get("avg_exec_price")
        if rec_price is not None:
            try:
                p = float(rec_price)
                price = p if p > 0 else None
            except (TypeError, ValueError):
                price = None
        # Уточняем статус/исполнение/цену у брокера, если запись неполная
        if executed <= 0 or status not in _TERMINAL_ORDER_STATUSES or price is None:
            state = await client.get_sandbox_order_state(oid)
            if state is not None:
                status = _status_name(getattr(state, "execution_report_status", None)) or status
                broker_le = int(getattr(state, "lots_executed", 0) or 0)
                if broker_le > 0:
                    executed = broker_le
                info = await client.get_instrument_info(pos.figi)
                lot = int(info.get("lot")) if info and info.get("lot") else (pos.lot_size or 1)
                p = ExecutionMonitor._extract_price(state, executed, lot)
                if p:
                    price = p
        if executed <= 0:
            continue  # эта заявка не доказывает исполнение — следующий кандидат
        sold = min(executed, int(pos.quantity) or executed)
        full = requested > 0 and executed >= requested
        reason_text = str(rec.get("reason") or "")
        close_reason = "LEDGER_CLOSE"
        if reason_text.startswith("close:"):
            close_reason = reason_text[len("close:"):].split()[0].upper() or close_reason
        note = "POSITION_CLOSED_BY_LEDGER"
        if not full:
            note += "; частичное исполнение SELL, остаток закрыт вне робота"
        _finalize_position_closed(
            pos, sold_lots=sold, exit_price=price, close_reason=close_reason,
            order_id=oid,
            requested_at=rec.get("updated_at_utc") or rec.get("timestamp_utc"),
            note=note,
        )
        logging.warning(
            f"[reconcile] {ticker}: позиция отсутствует у брокера, найдена своя "
            f"SELL-заявка {oid} ({executed} лот., price={price}) — учёт закрыт "
            f"({close_reason}), CSV-строка будет записана"
        )
        research_logger.log_event("POSITION_CLOSED_BY_LEDGER", {
            "figi": pos.figi, "ticker": ticker, "order_id": oid,
            "executed_lots": executed, "requested_lots": requested,
            "exit_price": price, "close_reason": close_reason,
            "ledger_status": status,
        })
        await notify(
            f"ℹ️ <b>{ticker}</b>: позиция закрыта (подтверждено реестром заявок)\n"
            f"SELL {oid}: {executed} лот. по {price if price else '—'}\n"
            f"Причина: {close_reason}. Учёт закрыт, CSV будет записан."
        )
        return "ledger_close"

    # --- В3: внешнее (ручное) закрытие — двойное подтверждение ---
    first_seen = _external_close_candidates.get(pos.figi)
    if first_seen is None:
        _external_close_candidates[pos.figi] = datetime.now(timezone.utc).isoformat()
        logging.warning(
            f"[reconcile] {ticker}: позиция {pos.quantity} лот. отсутствует у брокера, "
            f"своя SELL-заявка в реестре не найдена — подозрение на внешнее "
            f"закрытие; учёт не менялся, подтверждение на следующей сверке"
        )
        research_logger.log_event("POSITION_EXTERNAL_CLOSE_SUSPECTED", {
            "figi": pos.figi, "ticker": ticker, "quantity": pos.quantity,
        })
        return "candidate"

    prices = await client.get_last_prices([pos.figi])
    current = prices.get(pos.figi) if isinstance(prices, dict) else None
    exit_price = None
    if current is not None:
        try:
            p = float(current)
            exit_price = p if p > 0 else None
        except (TypeError, ValueError):
            exit_price = None
    if exit_price is None:
        exit_price = pos.close_price_before_order or pos.price_before_order or 0.0
    sold = int(pos.quantity)
    _finalize_position_closed(
        pos, sold_lots=sold, exit_price=exit_price, close_reason="EXTERNAL_CLOSE",
        order_id=None, requested_at=datetime.now(timezone.utc).isoformat(),
        note=("EXTERNAL_CLOSE: позиция продана вне робота; цена выхода "
              "приблизительная (на момент обнаружения), вход учтён штатно"),
    )
    _external_close_candidates.pop(pos.figi, None)
    logging.warning(
        f"[reconcile] {ticker}: внешнее закрытие подтверждено повторной сверкой — "
        f"учёт закрыт (EXTERNAL_CLOSE, {sold} лот., цена~{exit_price})"
    )
    research_logger.log_event("POSITION_CLOSED_EXTERNAL", {
        "figi": pos.figi, "ticker": ticker, "quantity": sold,
        "approx_exit_price": exit_price, "first_seen_utc": first_seen,
    })
    await notify(
        f"ℹ️ <b>{ticker}</b>: позиция продана вне робота (подтверждено дважды)\n"
        f"Учёт закрыт: {sold} лот., цена выхода ~{exit_price}\n"
        f"CSV-строка будет записана с пометкой EXTERNAL_CLOSE."
    )
    return "external_close"


# ============================================================
# RECONCILE (двусторонний)
# ============================================================

async def reconcile(client: TinkoffClient, monitor: ExecutionMonitor,
                    ledger: Optional[OrderLedger] = None, rm: Optional[RiskManager] = None):
    """
    Двусторонняя сверка (этап 5: с адопцией через единую точку регистрации).

    Важные правила:
      - get_positions()=None (ошибка/НЕПОЛНЫЙ снимок после fail-closed в
        tinkoff_client) — учёт НЕ меняется, сверка не завершена (D5).
      - позиция брокера без локального учёта -> регистрируется как RECOVERY
        (единая точка register_recovery, защита от дублей), а не вечный
        MISMATCH (D2). Направление/количество берутся из ДОСТОВЕРНОГО снимка.
      - RECOVERY-позиция, отсутствующая у брокера (достоверный снимок) —
        внешняя очистка: учёт закрывается, CSV-строка не пишется (данных
        входа нет).
      - Количество RECOVERY-позиции синхронизируется с брокером; направление
        не меняется. OPEN/CLOSING/UNCERTAIN — прежние правила.

    Партия P0-5: при ПОЛНОМ успехе (достоверный снимок, без расхождений)
    выставляется маркер _last_reconcile_confirmed_utc — подтверждённое
    безопасное разрешение неопределённостей реестра, появившихся ДО этого
    момента (см. _uncertainty_resolved_by_reconcile). Ошибка снимка или
    расхождения маркер НЕ выставляют.
    """
    global reconcile_done, _last_reconcile_confirmed_utc
    positions = await client.get_positions()

    if positions is None:
        logging.error("[reconcile] Портфель недостоверен (None) — учёт НЕ меняется, сверка не завершена.")
        client.uncertain_state = True
        reconcile_done = False
        return

    broker = {f: p for f, p in positions.items()
              if not f.startswith(("RUB", "USD", "EUR"))}

    local = {f: p for f, p in monitor.positions.items()
             if p.state in ACTIVE_STATES}

    mismatches = []
    mutated = False  # этап 5: изменения учёта сразу сохраняются (D6)
    pending_external = False  # пакет В3: кандидаты на внешнее закрытие в этой сверке

    for figi, pdata in broker.items():
        pos = local.get(figi)
        if pos is None:
            # Этап 5 (D2): адопция — позиция брокера без локального учёта.
            info = await client.get_instrument_info(figi)
            ticker = info.get("ticker") if info else figi
            if pdata.direction == "UNKNOWN":
                mismatches.append(f"{figi}: направление брокера=UNKNOWN — адопция невозможна")
                continue
            direction = "BUY" if pdata.direction == "LONG" else "SELL"
            lot = int(info.get("lot")) if info and info.get("lot") else 1
            adopted = monitor.register_recovery(
                figi=figi, ticker=ticker, direction=direction,
                quantity=abs(pdata.lots), lot_size=lot,
                recovery_source="adopted",
            )
            local[figi] = adopted
            mutated = True
            logging.error(
                f"[reconcile] {ticker}: позиция брокера без локального учёта — "
                f"зарегистрирована как RECOVERY ({abs(pdata.lots)} лот, {direction})"
            )
            research_logger.log_event("POSITION_RECOVERED", {
                "figi": figi, "ticker": ticker, "direction": direction,
                "lots": abs(pdata.lots), "source": "reconcile_adoption",
            })
            await notify(
                f"⚠️ <b>{ticker}</b>: позиция у брокера без локального учёта\n"
                f"Зарегистрирована как RECOVERY: {abs(pdata.lots)} лот ({direction})\n"
                f"Новые BUY по инструменту заблокированы. Автозакрытие отключено — решение за оператором."
            )
            pos = adopted

        if pos.state == PositionState.RECOVERY:
            our_side = "LONG" if pos.direction == "BUY" else "SHORT"
            if pdata.direction != our_side:
                mismatches.append(f"{figi}: направление брокера={pdata.direction}, локально={our_side} (RECOVERY)")
                continue
            if monitor.sync_recovery_quantity(pos, pdata.lots):
                mutated = True
                research_logger.log_event("POSITION_RECOVERY_SYNC", {
                    "figi": figi, "ticker": pos.ticker, "lots": pdata.lots,
                })
            continue

        our_side = "LONG" if pos.direction == "BUY" else "SHORT"
        if pdata.direction != our_side:
            mismatches.append(f"{figi}: направление брокера={pdata.direction}, локально={our_side}")
            continue
        if pdata.lots != pos.quantity:
            mismatches.append(f"{figi}: lots брокера={pdata.lots}, локально={pos.quantity}")

    for figi, pos in local.items():
        if figi in broker:
            continue
        if pos.state == PositionState.RECOVERY:
            # Достоверный снимок: позиция исчезла у брокера (внешняя очистка).
            # Учёт закрывается; CSV НЕ пишется (данных входа нет — расчёт PnL невозможен).
            pos.quantity = 0
            pos.state = PositionState.CLOSED
            pos.logged = True
            mutated = True
            logging.warning(
                f"[reconcile] {pos.ticker}: RECOVERY-позиция отсутствует у брокера — учёт закрыт (внешнее закрытие)"
            )
            research_logger.log_event("POSITION_RECOVERY_CLOSED", {
                "figi": figi, "ticker": pos.ticker, "source": "reconcile",
            })
            await notify(
                f"ℹ️ <b>{pos.ticker}</b>: RECOVERY-позиция отсутствует у брокера — учёт закрыт."
            )
            continue
        # Пакет В2/В3: позиция активна локально, но отсутствует у брокера
        # (достоверный снимок). Сначала пробуем разрешить по реестру заявок
        # или двойному подтверждению — и только если не удалось, фиксируем
        # расхождение (прежнее поведение).
        action = await _try_resolve_absent_position(client, ledger, pos)
        if action in ("ledger_close", "external_close"):
            mutated = True
            continue
        if action == "candidate":
            # первое обнаружение внешнего закрытия: учёт не менялся;
            # повторная сверка на следующем цикле (флаг ниже), расхождение
            # не фиксируется — входы по FIGI и так заблокированы локальной
            # активной позицией
            pending_external = True
            continue
        mismatches.append(f"{figi}: есть локально ({pos.quantity}), нет у брокера")

    if mutated and rm is not None:
        # Этап 5: изменения учёта (адопция/синхронизация/закрытие) сохраняются немедленно;
        # при ошибке записи новые входы блокируются (state_save_failed)
        _save_state_checked(monitor, rm)

    if mismatches:
        client.uncertain_state = True
        reconcile_done = False
        msg = "⚠️ Расхождения: " + "; ".join(mismatches)
        logging.error(f"[reconcile] {msg}")
        research_logger.log_event("RECONCILE_MISMATCH", {"mismatches": mismatches})
        await notify(msg)
        return

    if pending_external:
        # Пакет В3: первое обнаружение внешнего закрытия требует повторного
        # подтверждения. Сверка НЕ считается завершённой (маркер успеха
        # не выставляется), повторный reconcile — на следующем цикле.
        client.uncertain_state = True
        reconcile_done = False
        logging.warning(
            "[reconcile] есть кандидаты на внешнее закрытие — повторная сверка на следующем цикле"
        )
        return

    if not local and not broker:
        logging.info("[reconcile] Позиций нет (достоверно).")
    else:
        logging.info(f"[reconcile] Все позиции совпадают ({len(local)} активных).")

    reconcile_done = True
    client.uncertain_state = False
    # Партия P0-5: успешная сверка (достоверный снимок портфеля, без
    # расхождений) — подтверждённое безопасное разрешение неопределённостей
    # реестра, появившихся до этого момента. Только после него повторный
    # терминальный отчёт заявки, совпадающий с учтённым исполнением,
    # штатно закрывает UNCERTAIN-запись (см. register_executed_buy).
    _last_reconcile_confirmed_utc = utc_now_iso()
    research_logger.log_event("RECONCILE_OK", {"positions": len(local)})


# ============================================================
# CLOSE / PROCESS CLOSING
# ============================================================

async def close_position(client: TinkoffClient, pos, price: float):
    direction = (
        OrderDirection.ORDER_DIRECTION_SELL if pos.direction == "BUY"
        else OrderDirection.ORDER_DIRECTION_BUY
    )

    # Этап 5: SELL-заявки тоже проходят через реестр (INTENT до сетевого вызова).
    # При ошибке записи закрытие откладывается: позиция остаётся CLOSING ->
    # resolve_closing -> UNCERTAIN -> сверка вернёт её в OPEN и SL/TP повторятся.
    close_order_id = str(uuid.uuid4())
    if ledger_ref is not None:
        intent_ok = ledger_ref.record_intent(
            close_order_id, trade_id=pos.trade_id, figi=pos.figi,
            ticker=pos.ticker, side="SELL", requested_lots=pos.quantity,
            reason=f"close:{pos.close_reason}",
        )
        if not intent_ok:
            logging.error(
                f"[close] {pos.ticker}: INTENT не сохранён — закрытие отложено (fail-closed)"
            )
            return

    result = await client.post_market_order(pos.figi, pos.quantity, direction, order_id=close_order_id)

    if result.state is None:
        pos.state = PositionState.UNCERTAIN
        client.uncertain_state = True
        if ledger_ref is not None:
            ledger_ref.record_uncertain(
                close_order_id, trade_id=pos.trade_id, figi=pos.figi, ticker=pos.ticker,
                side="SELL", requested_lots=pos.quantity,
                reason="post: ответ потерян — закрытие могло быть принято", source="close_position",
                error=str(result.error) if result.error else None,
            )
        logging.error(
            f"[close] {pos.ticker}: state=None (order_id={result.order_id}) — UNCERTAIN. "
            f"Повторная отправка запрещена."
        )
        research_logger.log_event("CLOSE_FAILED", {
            "trade_id": pos.trade_id, "ticker": pos.ticker, "figi": pos.figi,
            "order_id": result.order_id,
        })
        return

    if ledger_ref is not None:
        ledger_ref.record_sent(
            close_order_id, trade_id=pos.trade_id, figi=pos.figi, ticker=pos.ticker,
            side="SELL", requested_lots=pos.quantity,
            executed_lots=int(getattr(result.state, "lots_executed", 0) or 0),
            status_hint=_status_name(getattr(result.state, "execution_report_status", None)),
            source="post_response",
        )

    pos.close_order_id = result.order_id
    pos.close_requested_lots = pos.quantity
    pos.close_requested_at = datetime.now(timezone.utc).isoformat()
    logging.info(
        f"🔒 [CLOSE] {pos.ticker} {pos.close_reason} @ {price:.4f} → CLOSING "
        f"(id={result.order_id}, requested={pos.close_requested_lots})"
    )
    research_logger.log_event("CLOSE_SENT", {
        "trade_id": pos.trade_id,
        "ticker": pos.ticker, "figi": pos.figi, "reason": pos.close_reason,
        "order_id": result.order_id, "price_before_order": price,
        "requested_lots": pos.close_requested_lots,
    })


async def process_closing(client: TinkoffClient, monitor: ExecutionMonitor, rm: Optional[RiskManager] = None):
    global reconcile_done

    for pos in list(monitor.positions.values()):
        if pos.state == PositionState.CLOSING:
            await monitor.resolve_closing(pos)

    uncertain_positions = [p for p in monitor.positions.values() if p.state == PositionState.UNCERTAIN]
    if uncertain_positions:
        positions = await client.get_positions()
        if positions is None:
            logging.error("[process_closing] get_positions()=None — UNCERTAIN не разрешаются.")
            client.uncertain_state = True
            reconcile_done = False
        else:
            broker = {f: p for f, p in positions.items()
                      if not f.startswith(("RUB", "USD", "EUR"))}
            for pos in uncertain_positions:
                monitor.resolve_uncertain(pos, broker)
            # v2.3.0-stage1: передаём ledger и rm — пакет В2/В3 в reconcile
            # требует реестр, а изменения учёта должны сохраняться немедленно.
            await reconcile(client, monitor, ledger_ref, rm)

    for pos in list(monitor.positions.values()):
        if pos.state != PositionState.CLOSED or pos.logged:
            continue

        trade_id = pos.trade_id or uuid.uuid4().hex[:12]
        if _trade_id_already_logged(trade_id):
            logging.warning(f"[csv] {pos.ticker}: trade_id={trade_id} уже в CSV — помечаю logged=True")
            pos.logged = True
            continue

        exit_price = pos.execution_price or 0.0
        executed_lots = pos.executed_lots
        if executed_lots <= 0:
            logging.error(
                f"[csv] {pos.ticker}: executed_lots={executed_lots} — пропуск записи"
            )
            pos.logged = True
            continue
        shares = executed_lots * pos.lot_size

        entry_slippage = (pos.entry_price - pos.price_before_order) * shares
        exit_slippage = (pos.close_price_before_order - exit_price) * shares
        total_slippage = entry_slippage + exit_slippage

        gross_pnl = (exit_price - pos.entry_price) * shares
        turnover = (exit_price + pos.entry_price) * shares
        model_total_commission = abs(turnover * config.COMMISSION_PER_SIDE)

        if pos.actual_commission is not None:
            exit_commission = pos.actual_commission
        else:
            exit_commission = model_total_commission / 2.0

        if pos.entry_actual_commission is not None:
            entry_commission = pos.entry_actual_commission
        else:
            entry_commission = model_total_commission / 2.0

        total_commission = entry_commission + exit_commission

        entry_actual = pos.entry_actual_commission is not None
        exit_actual = pos.actual_commission is not None
        if entry_actual and exit_actual:
            commission_source = "actual"
        elif entry_actual or exit_actual:
            commission_source = "mixed"
        else:
            commission_source = "model"

        net_pnl = gross_pnl - total_commission
        slippage_percent = (total_slippage / turnover * 100.0) if turnover > 0 else 0.0

        cumulative_before = research_logger.get_cumulative_net_pnl()
        cumulative_after = cumulative_before + net_pnl

        # v2.3.0-stage1 (С1): телеметрия позиции для новых полей CSV.
        # Для позиций из старого state.json initial_*/mae/mfe восстановлены
        # from_dict (initial_* = текущие sl/tp, экстремумы — 0: пусто в CSV).
        build_version = getattr(config, "BUILD_VERSION", "?")
        entry_sl_price = float(pos.initial_sl_price or 0.0)
        entry_tp_price = float(pos.initial_tp_price or 0.0)
        mae_price = float(pos.mae_price or 0.0)
        mfe_price = float(pos.mfe_price or 0.0)
        exit_sl_price = float(pos.sl_price or 0.0)
        risk_share = (pos.entry_price - entry_sl_price if pos.direction == "BUY"
                      else entry_sl_price - pos.entry_price)
        if risk_share > 0:
            if pos.direction == "BUY":
                mae_r = f"{(mae_price - pos.entry_price) / risk_share:.3f}" if mae_price > 0 else ""
                mfe_r = f"{(mfe_price - pos.entry_price) / risk_share:.3f}" if mfe_price > 0 else ""
                r_multiple = f"{(exit_price - pos.entry_price) / risk_share:.3f}" if exit_price > 0 else ""
            else:
                mae_r = f"{(pos.entry_price - mae_price) / risk_share:.3f}" if mae_price > 0 else ""
                mfe_r = f"{(pos.entry_price - mfe_price) / risk_share:.3f}" if mfe_price > 0 else ""
                r_multiple = f"{(pos.entry_price - exit_price) / risk_share:.3f}" if exit_price > 0 else ""
        else:
            mae_r = mfe_r = r_multiple = ""
        hold_minutes = ""
        start_dt = _parse_iso_ts(pos.order_send_time) or _parse_iso_ts(pos.signal_time)
        if start_dt is not None:
            hold_minutes = f"{(datetime.now(timezone.utc) - start_dt).total_seconds() / 60.0:.1f}"

        trade_row = {
            "trade_id": trade_id,
            "timestamp_utc": pos.close_requested_at or "",
            "ticker": pos.ticker, "figi": pos.figi, "lot_size": pos.lot_size,
            "side": pos.direction,
            "signal_reason": pos.signal_reason,
            "signal_time": pos.signal_time,
            "price_before_order": pos.price_before_order,
            "order_send_time": pos.order_send_time,
            "order_id": pos.close_order_id or "",
            "execution_status": "FILL_PARTIAL" if pos.partial else "FILL",
            "requested_lots": pos.close_requested_lots or pos.quantity,
            "executed_lots": executed_lots,
            "execution_price": f"{pos.entry_price:.4f}",
            "exit_reason": pos.close_reason or "",
            "exit_price": f"{exit_price:.4f}",
            "exit_time": datetime.now(timezone.utc).isoformat(),
            "commission_rub": f"{total_commission:.2f}",
            "commission_source": commission_source,
            "actual_commission_rub": f"{pos.actual_commission:.2f}" if pos.actual_commission is not None else "",
            "entry_commission_rub": f"{entry_commission:.2f}",
            "exit_commission_rub": f"{exit_commission:.2f}",
            "slippage_rub": f"{total_slippage:+.2f}",
            "slippage_percent": f"{slippage_percent:+.3f}",
            "gross_pnl_rub": f"{gross_pnl:.2f}",
            "net_pnl_rub": f"{net_pnl:.2f}",
            "cumulative_net_pnl_rub": f"{cumulative_after:.2f}",
            "error_note": getattr(pos, "error_note", "") or "",
            # --- v2.3.0-stage1: телеметрия С1 ---
            "build_version": build_version,
            "entry_sl_price": f"{entry_sl_price:.4f}" if entry_sl_price > 0 else "",
            "entry_tp_price": f"{entry_tp_price:.4f}" if entry_tp_price > 0 else "",
            "mae_price": f"{mae_price:.4f}" if mae_price > 0 else "",
            "mfe_price": f"{mfe_price:.4f}" if mfe_price > 0 else "",
            "mae_r": mae_r,
            "mfe_r": mfe_r,
            "r_multiple": r_multiple,
            "hold_minutes": hold_minutes,
            "exit_sl_price": f"{exit_sl_price:.4f}" if exit_sl_price > 0 else "",
        }

        if research_logger.log_trade(trade_row):
            pos.logged = True
            logging.info(
                f"✅ [CLOSED] {pos.ticker} {pos.close_reason} "
                f"entry={pos.entry_price:.4f} exit={exit_price:.4f} "
                f"lots={executed_lots} gross={gross_pnl:+.2f}₽ "
                f"net={net_pnl:+.2f}₽ cum={cumulative_after:+.2f}₽"
            )
            try:
                research_logger.log_event("CLOSED", {
                    "trade_id": trade_id, "ticker": pos.ticker, "figi": pos.figi,
                    "entry_price": pos.entry_price, "exit_price": exit_price,
                    "executed_lots": executed_lots, "lot_size": pos.lot_size,
                    "gross_pnl": gross_pnl,
                    "entry_commission": entry_commission,
                    "exit_commission": exit_commission,
                    "commission_source": commission_source,
                    "slippage": total_slippage, "slippage_percent": slippage_percent,
                    "net_pnl": net_pnl, "cumulative_net_pnl": cumulative_after,
                    "exit_reason": pos.close_reason or "",
                })
            except Exception as e:
                logging.warning(f"[jsonl] {pos.ticker}: не удалось записать CLOSED: {type(e).__name__}: {e}")
            await notify(
                f"🔒 <b>{pos.ticker}</b> закрыто по {pos.close_reason}\n"
                f"Вход: {pos.entry_price:.4f} → Выход: {exit_price:.4f}\n"
                f"Лотов: {executed_lots} (акций: {shares})\n"
                f"Net: <b>{net_pnl:+.2f} ₽</b>"
            )
        else:
            logging.error(f"[csv] {pos.ticker}: log_trade вернул False — повтор на следующем цикле.")


# ============================================================
# ЭТАП 5: ЕДИНАЯ ТОЧКА РЕГИСТРАЦИИ ИСПОЛНЕННОЙ ПОКУПКИ,
#         ОПРОС НЕЗАВЕРШЁННЫХ ЗАЯВОК, ВОССТАНОВЛЕНИЕ RECOVERY
# ============================================================

async def _build_candles_df(client: TinkoffClient, figi: str):
    """
    Свечи для расчёта SL/TP — та же конфигурация, что при входе
    (CANDLES_LOOKBACK_DAYS, 5-минутный интервал). Стратегия не меняется.
    Возвращает DataFrame или None (мало свечей/ошибка).
    """
    candles = await client.get_historical_candles(
        figi, days=config.CANDLES_LOOKBACK_DAYS,
        interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
    )
    if len(candles) < config.MIN_CANDLES_FOR_SIGNAL:
        return None
    df = pd.DataFrame([{
        'time': c.time,
        'open': float(c.open.units + c.open.nano / 1e9),
        'high': float(c.high.units + c.high.nano / 1e9),
        'low': float(c.low.units + c.low.nano / 1e9),
        'close': float(c.close.units + c.close.nano / 1e9),
        'volume': int(c.volume),
        'is_complete': getattr(c, 'is_complete', None),
    } for c in candles])
    return df.set_index('time').sort_index()


async def register_executed_buy(
    client: TinkoffClient,
    monitor: ExecutionMonitor,
    rm: RiskManager,
    ledger: Optional[OrderLedger],
    *,
    figi: str,
    ticker: str,
    order_id: str,
    trade_id: str,
    reason: str,
    signal_time: str,
    price_before_order: float,
    order_send_time: str,
    requested_lots: int,
    executed_lots: int,
    exec_price,
    lot_size: int,
    entry_commission=None,
    partial: bool = False,
    df=None,
    source: str = "",
    ledger_status: str = "FILL",
):
    """
    ЕДИНАЯ ТОЧКА регистрации исполненной BUY-заявки (этап 5, P0).

    Партия P0-4: рост lots_executed сверх учтённых ПОСЛЕ достоверной
    синхронизации остатка снимком портфеля (entry_snapshot_synced) —
    НЕОДНОЗНАЧЕН: дельта к остатку НЕ добавляется (снимок мог уже учесть
    это исполнение), счётчики догоняются по отчёту заявки, фиксируется
    неопределённость существующей архитектурой (client.uncertain_state +
    UNCERTAIN-запись реестра); остаток актуализируется сверкой reconcile.

    Гарантии:
      - подтверждённое исполнение (executed_lots > 0) не может быть потеряно:
        при отсутствии цены исполнения или SL/TP позиция регистрируется в
        RECOVERY (никогда не «continue молча»);
      - неизвестная цена входа НЕ заменяется произвольной (entry_price=0 —
        явная отметка «неизвестно»);
      - SL/TP рассчитываются ТОЛЬКО штатной стратегией (rm.calculate_sl_tp на
        свечах); без df (опрос/стартовое восстановление) регистрация идёт в
        RECOVERY, а расчёт выполняет promote_recovery_positions после загрузки
        свечей;
      - идемпотентность: активная позиция по FIGI не перезаписывается;
      - RECOVERY не закрывается автоматически.
    """
    if not executed_lots or executed_lots <= 0:
        logging.error(
            f"[register] {ticker}: executed_lots={executed_lots} — исполнение не "
            f"подтверждено, регистрация невозможна (order_id={order_id})"
        )
        return None

    if not figi:
        # Партия P0-2: исполнение с ПУСТЫМ (неопределённым) FIGI не может быть
        # зарегистрировано как обычная BUY-позиция — неизвестно, какой инструмент
        # затронут. Заявка остаётся UNCERTAIN (НЕ отказ), новые BUY запрещены
        # ГЛОБАЛЬНО до разрешения (has_unknown_figi_open_orders в trading_cycle);
        # существующие позиции продолжают мониториться.
        logging.error(
            f"[register] {ticker}: figi отсутствует при подтверждённом исполнении "
            f"{executed_lots} лот. (order_id={order_id}) — авторегистрация позиции "
            f"ЗАПРЕЩЕНА: UNCERTAIN + глобальная блокировка новых BUY"
        )
        client.uncertain_state = True
        if ledger is not None and order_id:
            ledger.record_uncertain(
                order_id, trade_id=trade_id, figi=None, ticker=ticker,
                side="BUY", requested_lots=requested_lots,
                executed_lots=executed_lots, avg_exec_price=exec_price,
                reason="исполнение без FIGI — авторегистрация позиции запрещена, "
                       "требуется разрешение оператором",
                source=source or "register_executed_buy",
            )
        research_logger.log_event("BUY_UNKNOWN_FIGI", {
            "order_id": order_id, "ticker": ticker,
            "executed_lots": executed_lots, "source": source,
        })
        await notify(
            "⛔ Исполнение с НЕИЗВЕСТНЫМ FIGI\n"
            f"Исполнено лотов: {executed_lots} (order_id={order_id or '—'})\n"
            "Позиция НЕ зарегистрирована автоматически (инструмент неизвестен).\n"
            "Новые BUY запрещены ГЛОБАЛЬНО до разрешения. Требуется оператор."
        )
        return None

    # Исправление (дефект №1 — блокировка первичной регистрации BUY).
    # Предыдущий guard считал терминальную запись реестра (FILL с
    # executed_lots, которую record_sent() пишет в trading_cycle ДО
    # регистрации позиции) доказательством применения лотов к учёту:
    # при отсутствии активной позиции (она только должна была быть
    # создана) и executed_lots <= recorded_le первичная регистрация
    # терялась (return None). Теперь блокировка выполняется ТОЛЬКО при
    # ПОЛОЖИТЕЛЬНОМ доказательстве, что соответствующее исполнение уже
    # учтено локально:
    #   (а) есть запись позиции по этому FIGI (любое состояние, включая
    #       CLOSED — активные позиции обрабатываются ниже) с
    #       entry_order_id == order_id и entry_applied_lots >= executed_lots;
    #   (б) order_id входит в компактный реестр закрытых входных заявок
    #       (execution.closed_entry_order_ids, сериализуется в state.json —
    #       признак переживает перезапуск и перезапись CLOSED-записи).
    # Отсутствие позиций и наличие FILL в реестре НЕ считаются
    # доказательством применения: во всех остальных случаях первичная
    # регистрация подтверждённого брокером исполнения разрешена.
    # Заявка, относящаяся к закрытой позиции, не «воскрешает» её; рост
    # executed_lots сверх учтённого при закрытой сделке — UNCERTAIN +
    # сверка (согласовано с poll_open_orders), не новая позиция.
    rec_pre = ledger.latest(order_id) if (ledger is not None and order_id) else None
    if (rec_pre is not None
            and rec_pre.get("status") in _TERMINAL_ORDER_STATUSES
            and not monitor.has_active_position(figi)):
        recorded_le = int(rec_pre.get("executed_lots") or 0)
        # В этой ветке позиция (если есть) может быть только CLOSED
        # (активные состояния отсеяны has_active_position выше).
        pos_closed = monitor.get_position(figi)
        applied_closed = int(pos_closed.entry_applied_lots or 0) if pos_closed is not None else 0
        evidence_applied = (
            pos_closed is not None
            and pos_closed.entry_order_id == order_id
            and applied_closed >= executed_lots
        )
        evidence_closed_entry = (
            (pos_closed is not None and pos_closed.entry_order_id == order_id)
            or order_id in monitor.closed_entry_order_ids
        )
        if evidence_applied:
            logging.warning(
                f"[register] {ticker}: исполнение заявки {order_id} ({executed_lots} лот.) "
                f"уже применено к позиции (entry_applied_lots={applied_closed}, "
                f"state={pos_closed.state.value}) — повторная регистрация не выполняется"
            )
            return None
        if evidence_closed_entry:
            # Заявка относится к уже закрытой сделке (запись позиции
            # перезаписана новой позицией по тому же FIGI либо закрытая
            # запись сохранилась с ростом executed_lots сверх учтённого).
            # Позиция не воскрешается; поздний рост — неопределённость,
            # разрешается сверкой по достоверному снимку брокера.
            if executed_lots > max(recorded_le, applied_closed):
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(
                        order_id, figi=figi, ticker=ticker, side="BUY",
                        executed_lots=int(executed_lots),
                        reason=(f"поздний рост исполнения закрытой BUY-заявки "
                                f"{max(recorded_le, applied_closed)} -> {executed_lots} "
                                f"лот. — позиция не воскрешается, требуется сверка"),
                        source=source or "register_executed_buy",
                    )
                logging.error(
                    f"[register] {ticker}: поздний рост исполнения закрытой заявки "
                    f"{order_id} ({max(recorded_le, applied_closed)} -> {executed_lots} "
                    f"лот.) — UNCERTAIN, требуется сверка (адопция по снимку)"
                )
            else:
                logging.warning(
                    f"[register] {ticker}: заявка {order_id} относится к закрытой "
                    f"позиции ({executed_lots} лот. учтены) — «воскрешение» "
                    f"запрещено, повторная регистрация не выполняется"
                )
            return None

    if monitor.has_active_position(figi):
        existing = monitor.get_position(figi)
        logging.warning(
            f"[register] {ticker}: активная позиция уже есть "
            f"(state={existing.state.value}, order={existing.entry_order_id}) — "
            f"дубль не создаётся (order_id={order_id})"
        )
        # Этап BUY REGISTRATION FIX 3 — исполнение БЕЗ достоверного order_id.
        # Дефект: проверка принадлежности (FIX 2, ниже) выполнялась только
        # при непустом order_id; при order_id=None/""/пробелах она
        # пропускалась, и apply_late_entry_lots() (execution.py допускает
        # order_id=None) применял дельту к существующей OPEN-позиции БЕЗ
        # подтверждения, что исполнение относится именно к этой позиции
        # (та же дыра у record_recovery_entry_lots() для RECOVERY-позиций).
        # Принцип: отсутствие идентификатора — это НЕОПРЕДЕЛЁННОСТЬ, а НЕ
        # разрешение применить исполнение. Пока order_id неизвестен,
        # исполнение не может быть привязано ни к одной активной позиции:
        #   - лоты НЕ применяются: quantity/entry_price/entry_applied_lots
        #     НЕ изменяются (ни в OPEN-, ни в RECOVERY-ветках ниже);
        #   - успешный маркер регистрации НЕ пишется (весь блок записи
        #     реестра ниже требует непустого order_id);
        #   - client.uncertain_state=True — новые BUY запрещены до
        #     устранения неопределённости (trading_cycle: запрет новых
        #     входов при uncertain_state);
        #   - диагностическое событие с причиной отсутствия order_id — в
        #     research_events.jsonl (append-only);
        #   - UNCERTAIN-запись реестра НЕ пишется: record_uncertain()
        #     адресуется order_id, фиктивные идентификаторы запрещены
        #     (согласовано с веткой «исполнение без FIGI» выше: ledger
        #     пишется только при непустом order_id); падение обработки
        #     из-за невозможности записать UNCERTAIN с пустым ID исключено.
        # Повторное получение того же события идемпотентно: позиция не
        # изменяется, новых изменений состояния нет.
        if not str(order_id or "").strip():
            client.uncertain_state = True
            research_logger.log_event("BUY_EXEC_NO_ORDER_ID", {
                "figi": figi, "ticker": ticker, "trade_id": trade_id,
                "order_id_raw": order_id,
                "executed_lots": int(executed_lots),
                "requested_lots": requested_lots,
                "position_state": existing.state.value,
                "entry_order_id": existing.entry_order_id,
                "entry_applied_lots": int(existing.entry_applied_lots or 0),
                "position_quantity": existing.quantity,
                "source": source,
                "note": ("исполнение BUY без достоверного order_id при активной "
                         "позиции: лоты НЕ применены (quantity/entry_price/"
                         "entry_applied_lots не изменены), успешный маркер "
                         "регистрации не пишется, UNCERTAIN — новые BUY "
                         "заблокированы до устранения неопределённости"),
            })
            logging.error(
                f"[register] {ticker}: исполнение {executed_lots} лот. БЕЗ "
                f"достоверного order_id (trade_id={trade_id or '—'}) при "
                f"активной позиции (state={existing.state.value}, "
                f"entry_order_id={existing.entry_order_id or '—'}, "
                f"entry_applied_lots={int(existing.entry_applied_lots or 0)}) — "
                f"лоты НЕ применены (quantity={existing.quantity} не изменено), "
                f"успешный маркер не пишется, UNCERTAIN: новые BUY запрещены "
                f"до устранения неопределённости (order_id отсутствует/пустой)"
            )
            _save_state_checked(monitor, rm)
            return existing
        # Этап 5 + исправление (дефект №2 — слепая привязка RECOVERY).
        # RECOVERY-позиция без entry_order_id связывается с заявкой ТОЛЬКО
        # при подтверждённой принадлежности: оба trade_id известны и
        # совпадают. Поздний отчёт ЧУЖОЙ заявки (тот же FIGI, другой или
        # неизвестный trade_id) НЕ привязывается и её исполнение НЕ
        # применяется к позиции: фиксируется неопределённость существующей
        # архитектурой (client.uncertain_state + UNCERTAIN-запись реестра)
        # — разрешается сверкой/оператором. Совпадение одного FIGI само по
        # себе не доказывает принадлежность заявки этой позиции.
        if (existing.state == PositionState.RECOVERY
                and not existing.entry_order_id and order_id):
            ownership_confirmed = (
                bool(existing.trade_id) and bool(trade_id)
                and str(existing.trade_id) == str(trade_id)
            )
            if ownership_confirmed:
                existing.entry_order_id = order_id
                logging.info(
                    f"[register] {ticker}: RECOVERY-позиция связана с заявкой "
                    f"{order_id} (trade_id совпадает) — восстановление цены "
                    f"входа возможно"
                )
            else:
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(
                        order_id, figi=figi, ticker=ticker, side="BUY",
                        executed_lots=int(executed_lots),
                        reason=("отчёт заявки НЕ привязан к RECOVERY-позиции: "
                                "trade_id заявки не совпадает с trade_id позиции "
                                "(или неизвестен) — принадлежность не подтверждена, "
                                "исполнение к позиции не применяется, требуется "
                                "сверка/решение оператора"),
                        source=source or "register_executed_buy",
                    )
                logging.error(
                    f"[register] {ticker}: заявка {order_id} (trade_id="
                    f"{trade_id or '—'}) НЕ привязана к RECOVERY-позиции "
                    f"(trade_id={existing.trade_id or '—'}) — принадлежность "
                    f"не подтверждена, исполнение не применено, UNCERTAIN"
                )
                _save_state_checked(monitor, rm)
                return existing
        elif (existing.state == PositionState.RECOVERY
                and existing.entry_order_id and order_id
                and str(order_id) != str(existing.entry_order_id)):
            # Дополнительный этап: активная RECOVERY-позиция УЖЕ связана с
            # другой входной заявкой. Раньше отчёт чужой заявки проваливался
            # в общую ветку идемпотентности и помечался обработанным
            # («позиция уже зарегистрирована»), хотя его исполнение к
            # позиции НЕ применялось — реестр утверждал то, чего не было.
            # Теперь несовпадение entry_order_id — явная неопределённость:
            # исполнение НЕ применяется, чужая заявка НЕ отмечается
            # обработанной (терминальный статус не пишется), фиксируется
            # client.uncertain_state + UNCERTAIN-запись реестра; разрешается
            # сверкой/оператором. Даже совпадение trade_id не перепривязывает
            # заявку: позиция уже связана с другой заявкой.
            client.uncertain_state = True
            if ledger is not None:
                ledger.record_uncertain(
                    order_id, figi=figi, ticker=ticker, side="BUY",
                    executed_lots=int(executed_lots),
                    reason=(f"RECOVERY-позиция связана с другой входной заявкой "
                            f"(entry_order_id={existing.entry_order_id}, trade_id="
                            f"{existing.trade_id or '—'}): заявка {order_id} "
                            f"(trade_id={trade_id or '—'}) чужая — исполнение "
                            f"к позиции не применяется, требуется сверка/"
                            f"решение оператора"),
                    source=source or "register_executed_buy",
                )
            logging.error(
                f"[register] {ticker}: заявка {order_id} НЕ совпадает с "
                f"entry_order_id={existing.entry_order_id} активной "
                f"RECOVERY-позиции — исполнение не применено, чужая заявка "
                f"НЕ отмечена обработанной, UNCERTAIN"
            )
            _save_state_checked(monitor, rm)
            return existing
        qty_before = existing.quantity
        applied = False
        contradiction = False
        snapshot_covered_growth = False
        if existing.state == PositionState.RECOVERY:
            # Партия P0-3: повторная обработка накопительного lots_executed
            # BUY-заявки для RECOVERY-позиции — НЕ sync_recovery_quantity():
            # lots_executed заявки и остаток позиции у брокера — разные
            # величины (оператор мог продать часть позиции). Применяются
            # только действительно новые лоты (сверх учтённых и сверх уже
            # виденных у этой заявки); повторный ответ с тем же значением
            # ничего не меняет. Связь заявки с позицией проверяется по
            # entry_order_id (и FIGI — ветка найдена по FIGI позиции).
            seen_before = None
            if rec_pre is not None:
                try:
                    seen_before = int(rec_pre.get("executed_lots") or 0)
                except (TypeError, ValueError):
                    seen_before = None
            baseline_before = max(
                int(existing.entry_applied_lots or 0),
                seen_before if seen_before is not None else 0,
            )
            # Партия P0-4: рост lots_executed сверх учтённых ПОСЛЕ достоверной
            # синхронизации остатка снимком портфеля (entry_snapshot_synced)
            # НЕОДНОЗНАЧЕН: дельта могла быть уже отражена в остатке снимком
            # (исполнение до снимка) или быть новой (исполнение после
            # снимка) — по этим значениям неразличимо. Обрабатывается
            # консервативно: record_recovery_entry_lots догоняет ТОЛЬКО
            # счётчики, остаток не меняется; ниже фиксируется
            # неопределённость существующей архитектурой.
            snapshot_covered_growth = (
                bool(existing.entry_snapshot_synced)
                and int(executed_lots) > baseline_before
            )
            applied = monitor.record_recovery_entry_lots(
                existing, executed_lots, order_id=order_id,
                already_seen_lots=seen_before,
            )
            if (not applied
                    and int(executed_lots) < baseline_before):
                # Противоречие/устаревший ответ: исполнение не убывает,
                # позиция и счётчики не изменяются; фиксируется
                # неопределённость и блокировка новых BUY по FIGI
                # существующей архитектурой (uncertain_state + реестр).
                contradiction = True
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(
                        order_id, figi=figi, ticker=ticker,
                        side="BUY", executed_lots=int(executed_lots),
                        reason=(f"противоречие: lots_executed={executed_lots} меньше "
                                f"учтённых {baseline_before} — исполнение не убывает, "
                                f"позиция не изменена, требуется проверка оператором"),
                        source=source or "register_executed_buy",
                    )
                logging.error(
                    f"[register] {ticker}: устаревший/противоречивый ответ заявки "
                    f"{order_id} (lots_executed={executed_lots} < учтённых "
                    f"{baseline_before}) — UNCERTAIN, новые BUY по {figi} заблокированы"
                )
            elif snapshot_covered_growth:
                # Партия P0-4: дельта к остатку НЕ добавлена (снимок мог уже
                # учесть это исполнение — повторное применение после
                # reconcile исключено), счётчики исполнения догнаны по
                # отчёту заявки. Неопределённость фиксируется СУЩЕСТВУЮЩЕЙ
                # архитектурой: client.uncertain_state + UNCERTAIN-запись
                # реестра (новые BUY по FIGI блокированы); остаток
                # актуализируется сверкой reconcile по достоверному снимку.
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(
                        order_id, figi=figi, ticker=ticker,
                        side="BUY", executed_lots=int(executed_lots),
                        reason=(f"рост lots_executed={int(executed_lots)} сверх учтённых "
                                f"{baseline_before} после достоверной синхронизации остатка "
                                f"по снимку портфеля — дельта к остатку не добавлена "
                                f"(снимок мог уже учесть это исполнение), требуется сверка"),
                        source=source or "register_executed_buy",
                    )
                logging.warning(
                    f"[register] {ticker}: рост исполнения заявки {order_id} "
                    f"({baseline_before} -> {int(executed_lots)} лот.) после "
                    f"синхронизации остатка снимком портфеля — к остатку НЕ "
                    f"добавлен (исключено повторное применение после reconcile), "
                    f"счётчики догнаны, quantity={existing.quantity}; "
                    f"UNCERTAIN, новые BUY по {figi} заблокированы"
                )
                _save_state_checked(monitor, rm)
        elif existing.state == PositionState.OPEN:
            # Дефект P1 (этап BUY REGISTRATION FIX 2): проверка принадлежности
            # исполнения заявке, которой соответствует OPEN-позиция. Совпадение
            # FIGI само по себе НЕ доказывает принадлежность: отчёт старой/
            # чужой заявки A (тот же FIGI, другой order_id) НЕ применяется к
            # позиции B — лоты НЕ добавляются, цена входа НЕ изменяется,
            # успешный маркер («позиция уже зарегистрирована —
            # идемпотентность» и любой терминальный статус) НЕ пишется, даже
            # если trade_id совпадает: принадлежность заявки и сделки
            # проверяется согласованно, совпадение trade_id при несовпадении
            # order_id применением не разрешается. Вместо этого — явная
            # неопределённость (зеркало RECOVERY-ветки выше):
            # client.uncertain_state + UNCERTAIN-запись реестра с причиной
            # "order_id mismatch" и диагностикой (FIGI, старый order_id,
            # текущий entry_order_id, trade_id, количество исполнения);
            # новые BUY по FIGI блокированы существующей архитектурой
            # (figi_blocks_buy) до устранения расхождения. Если
            # entry_order_id позиции неизвестен — безопасное сопоставление
            # невозможно: UNCERTAIN, а не угадывание. Повторная обработка
            # того же события идемпотентна: лоты не удваиваются, повторные
            # записи успешного применения не создаются, неопределённость
            # самопроизвольно не снимается.
            if (order_id and (not existing.entry_order_id
                              or str(order_id) != str(existing.entry_order_id))):
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(
                        order_id, trade_id=trade_id, figi=figi, ticker=ticker,
                        side="BUY", requested_lots=requested_lots,
                        executed_lots=int(executed_lots),
                        reason=(
                            f"order_id mismatch: отчёт заявки {order_id} "
                            f"(trade_id={trade_id or '—'}, {executed_lots} лот.) "
                            f"не совпадает с входной заявкой "
                            f"{existing.entry_order_id or '—'} (trade_id="
                            f"{existing.trade_id or '—'}) активной OPEN-позиции "
                            f"{ticker} — исполнение к позиции НЕ применено "
                            f"(лоты/цена входа не изменены), успешный маркер "
                            f"не пишется; требуется сверка/решение оператора"
                        ),
                        source=source or "register_executed_buy",
                    )
                logging.error(
                    f"[register] {ticker}: исполнение заявки {order_id} "
                    f"(trade_id={trade_id or '—'}, {executed_lots} лот.) НЕ "
                    f"совпадает с entry_order_id={existing.entry_order_id or '—'} "
                    f"активной OPEN-позиции (trade_id={existing.trade_id or '—'}, "
                    f"figi={figi}) — лоты НЕ применены, заявка НЕ отмечена "
                    f"обработанной, UNCERTAIN (order_id mismatch)"
                )
                _save_state_checked(monitor, rm)
                return existing
            # Партия P0-2: поздние дополнительные лоты ПОСЛЕ перехода в OPEN
            # (например, отчёт брокера обновился после терминального CANCELLED).
            # Сюда попадают только исполнения СВОЕЙ заявки (order_id совпадает
            # с entry_order_id) — идемпотентность по entry_applied_lots.
            applied = monitor.apply_late_entry_lots(existing, executed_lots, order_id)
        if ledger is not None and order_id:
            rec = ledger.latest(order_id)
            # Партия P0-5: действующая (ещё не разрешённая подтверждённой
            # сверкой портфеля) UNCERTAIN-запись заявки. Обычный повторный
            # отчёт брокера (тот же lots_executed, состояние учёта не
            # изменилось) НЕ снимает её и НЕ перезаписывает терминальным
            # статусом: блокировка новых BUY по FIGI сохраняется до
            # подтверждённого безопасного разрешения — успешного reconcile
            # по достоверному снимку портфеля ПОСЛЕ появления UNCERTAIN.
            uncertainty_pending = (
                rec is not None
                and rec["status"] == STATUS_UNCERTAIN
                and not _uncertainty_resolved_by_reconcile(rec)
            )
            if applied and not snapshot_covered_growth:
                # Дополнительный этап: состояние сохраняется ДО записи
                # подтверждения в реестр; подтверждение пишется ТОЛЬКО если
                # state.json сохранён (несохранённое применение реестром
                # не подтверждается — иначе startup_recovery при перезапуске
                # пропускал заявку по reason-маркеру, и несохранённая дельта
                # терялась бы безвозвратно).
                state_saved_after_apply = _save_state_checked(monitor, rm)
                if uncertainty_pending:
                    # Партия P0-5: исполнение применено к позиции (реально
                    # новые лоты, подтверждённые брокером), но действующая
                    # UNCERTAIN-запись сохраняется до подтверждённой сверки:
                    # терминальный статус не пишется, блокировка FIGI
                    # остаётся. Изменения позиции сохраняются немедленно.
                    logging.warning(
                        f"[register] {ticker}: исполнение заявки {order_id} "
                        f"применено к позиции (+{existing.quantity - qty_before} лот.), "
                        f"но действующая UNCERTAIN-запись сохранена до "
                        f"подтверждённой сверки портфеля — терминальный статус "
                        f"не пишется, новые BUY по {figi} остаются заблокированы"
                    )
                elif state_saved_after_apply:
                    ledger.record_status(
                        order_id, ledger_status, executed_lots=executed_lots,
                        avg_exec_price=exec_price,
                        reason=f"позднее исполнение применено к позиции "
                               f"(+{existing.quantity - qty_before} лот., итого {existing.quantity})",
                        source=source or "register_executed_buy",
                    )
                    research_logger.log_event("POSITION_ENTRY_LATE_FILL", {
                        "figi": figi, "ticker": ticker, "order_id": order_id,
                        "added_lots": existing.quantity - qty_before,
                        "quantity": existing.quantity,
                        "state": existing.state.value,
                        "entry_price": existing.entry_price,
                        "sl": existing.sl_price, "tp": existing.tp_price,
                        "source": source,
                    })
                    if existing.state == PositionState.OPEN:
                        await notify(
                            f"⚠️ <b>{ticker}</b>: позднее исполнение входной заявки\n"
                            f"Добавлено лотов: +{existing.quantity - qty_before} "
                            f"(итого {existing.quantity})\n"
                            f"Цена входа: {existing.entry_price:.4f} (не менялась)\n"
                            f"SL: {existing.sl_price:.4f}\nTP: {existing.tp_price:.4f} — сохранены"
                        )
                else:
                    # Дополнительный этап: state.json НЕ сохранён —
                    # подтверждение «исполнение применено» НЕ пишется.
                    # Без него прежнее подтверждение регистрации маскировало
                    # бы несохранённую дельту при перезапуске (startup_
                    # recovery пропустил бы заявку по reason-маркеру).
                    # Фиксируется неопределённость существующей архитектурой:
                    # client.uncertain_state + UNCERTAIN-запись реестра —
                    # заявка остаётся доступной опросу/сверке, дельта
                    # применяется повторно после перезапуска.
                    client.uncertain_state = True
                    if ledger is not None:
                        ledger.record_uncertain(
                            order_id, figi=figi, ticker=ticker, side="BUY",
                            executed_lots=int(executed_lots),
                            reason=(f"исполнение применено к позиции в памяти "
                                    f"(+{existing.quantity - qty_before} лот., "
                                    f"итого {existing.quantity}), но state.json НЕ "
                                    f"сохранён — подтверждение не пишется, "
                                    f"требуется повторное применение/сверка"),
                            source=source or "register_executed_buy",
                        )
                    logging.error(
                        f"[register] {ticker}: state.json НЕ сохранён после "
                        f"применения позднего исполнения заявки {order_id} "
                        f"(+{existing.quantity - qty_before} лот.) — "
                        f"подтверждение в реестр не пишется, UNCERTAIN"
                    )
            elif contradiction or snapshot_covered_growth:
                # Реестр уже переведён в UNCERTAIN выше — перезапись запрещена:
                # противоречие/неоднозначность должны оставаться видимыми
                # до разрешения.
                pass
            elif uncertainty_pending:
                # Партия P0-5: повторный обычный отчёт с тем же lots_executed
                # НЕ снимает ранее установленную UNCERTAIN и НЕ пишет
                # терминальный статус: запись сохраняется до подтверждённой
                # сверки портфеля, блокировка новых BUY по FIGI не отпускается
                # простым повторным терминальным статусом заявки.
                logging.warning(
                    f"[register] {ticker}: повторный отчёт заявки {order_id} "
                    f"(status={ledger_status}, lots_executed={executed_lots}) при "
                    f"действующей UNCERTAIN-записи — запись сохранена, терминальный "
                    f"статус не пишется; новые BUY по {figi} остаются заблокированы "
                    f"до подтверждённой сверки портфеля"
                )
            elif rec is not None and rec["status"] == STATUS_UNCERTAIN:
                # Партия P0-5: неопределённость разрешена подтверждённой
                # сверкой портфеля (успешный reconcile ПОСЛЕ появления
                # UNCERTAIN) — штатное закрытие записи повторным терминальным
                # отчётом брокера, совпадающим с учтённым исполнением.
                ledger.record_status(
                    order_id, ledger_status, executed_lots=executed_lots,
                    avg_exec_price=exec_price,
                    reason=("неопределённость разрешена подтверждённой сверкой "
                            "портфеля; повторный терминальный отчёт совпадает "
                            "с учтённым исполнением — идемпотентность"),
                    source=source or "register_executed_buy",
                )
            elif rec is not None and rec["status"] not in _TERMINAL_ORDER_STATUSES:
                ledger.record_status(
                    order_id, ledger_status, executed_lots=executed_lots,
                    avg_exec_price=exec_price,
                    reason="позиция уже зарегистрирована — идемпотентность",
                    source=source or "register_executed_buy",
                )
        return existing

    pos = None
    recovery_source = None
    sl_price = None
    tp_price = None
    if exec_price is not None and exec_price > 0:
        if df is not None:
            # Дефект №7: исключение rm.calculate_sl_tp() НЕ срывает регистрацию
            # подтверждённого исполнения — позиция создаётся как RECOVERY
            # (без фиктивной цены входа и без фиктивных SL/TP).
            try:
                sl_price, tp_price = rm.calculate_sl_tp(exec_price, "BUY", df)
            except Exception as e:
                logging.error(
                    f"[register] {ticker}: ОШИБКА расчёта SL/TP: {type(e).__name__}: {e} — "
                    f"исполнение {executed_lots} лот. регистрируется как RECOVERY "
                    f"(без фиктивных цены/SL/TP, автозакрытие запрещено)"
                )
                research_logger.log_event("RECOVERY_SLTP_ERROR", {
                    "figi": figi, "ticker": ticker, "order_id": order_id,
                    "executed_lots": executed_lots, "partial": partial,
                    "error": f"{type(e).__name__}: {e}",
                })
                sl_price, tp_price = None, None
        else:
            # Свечей нет (опрос/старт): SL/TP считаются только по стратегии на
            # свечах — сначала RECOVERY, расчёт после загрузки свечей.
            sl_price, tp_price = None, None
        if sl_price is not None and tp_price is not None:
            try:
                pos = monitor.register(
                    figi=figi, ticker=ticker, direction="BUY",
                    entry_price=exec_price, quantity=executed_lots,
                    sl_price=sl_price, tp_price=tp_price, lot_size=lot_size,
                    trade_id=trade_id, signal_reason=reason,
                    signal_time=signal_time, price_before_order=price_before_order,
                    order_send_time=order_send_time,
                    entry_requested_lots=requested_lots,
                    entry_applied_lots=executed_lots,
                    entry_order_id=order_id,
                )
                pos.execution_price = exec_price
                pos.executed_lots = executed_lots
                pos.partial = partial
                pos.entry_actual_commission = entry_commission
            except Exception as e:
                # Дефект №7 (защита в глубину): даже сбой register() не должен
                # терять подтверждённое исполнение — fallback в RECOVERY.
                logging.error(
                    f"[register] {ticker}: ОШИБКА register(): {type(e).__name__}: {e} — "
                    f"исполнение {executed_lots} лот. регистрируется как RECOVERY"
                )
                research_logger.log_event("RECOVERY_REGISTER_ERROR", {
                    "figi": figi, "ticker": ticker, "order_id": order_id,
                    "executed_lots": executed_lots,
                    "error": f"{type(e).__name__}: {e}",
                })
                pos = None
                recovery_source = "register_error"
        else:
            recovery_source = "no_sl_tp"
    else:
        recovery_source = "no_price"

    if pos is None:
        pos = monitor.register_recovery(
            figi=figi, ticker=ticker, direction="BUY",
            quantity=executed_lots, lot_size=lot_size,
            trade_id=trade_id, order_id=order_id,
            signal_reason=reason, signal_time=signal_time,
            price_before_order=price_before_order, order_send_time=order_send_time,
            requested_lots=requested_lots, partial=partial,
            entry_commission=entry_commission,
            execution_price=exec_price if (exec_price is not None and exec_price > 0) else None,
            recovery_source=recovery_source,
        )

    rm.record_trade(figi)
    state_saved = _save_state_checked(monitor, rm)

    if pos.state == PositionState.OPEN:
        logging.info(
            f"🚀 [BUY] {ticker} {executed_lots} лот. @ {exec_price:.4f} "
            f"SL={pos.sl_price:.4f} TP={pos.tp_price:.4f} "
            f"entry_comm={entry_commission if entry_commission is not None else 'model'} "
            f"(reason: {reason})"
        )
        research_logger.log_event("POSITION_OPENED", {
            "trade_id": trade_id, "ticker": ticker, "figi": figi,
            "order_id": order_id, "exec_price": exec_price,
            "exec_lots": executed_lots, "sl": pos.sl_price, "tp": pos.tp_price,
            "entry_commission": entry_commission,
        })
        # Дефект P2 (этап BUY REGISTRATION FIX 2): уведомление должно
        # соответствовать результату сохранения state.json. При
        # state_saved=False нельзя создавать у пользователя впечатление
        # надёжно сохранённой позиции — к сообщению добавляется явное
        # критическое предупреждение (позиция только в оперативной памяти,
        # сохранение не подтверждено, новые BUY заблокированы). Ошибка
        # сохранения не скрывается: STATE_SAVE_FAILED + лог уже записаны
        # _save_state_checked, новые входы блокированы state_save_failed.
        if partial:
            open_msg = (
                f"⚠️ <b>{ticker}</b> частичное исполнение BUY\n"
                f"Исполнено: {executed_lots} лот. @ {exec_price:.4f}\n"
                f"SL: {pos.sl_price:.4f}\nTP: {pos.tp_price:.4f}\n"
                f"<i>{reason}</i>"
            )
        else:
            open_msg = (
                f"🚀 <b>{ticker}</b> BUY\n"
                f"Исполнено: {executed_lots} лот. @ {exec_price:.4f}\n"
                f"SL: {pos.sl_price:.4f}\nTP: {pos.tp_price:.4f}\n"
                f"<i>{reason}</i>"
            )
        if state_saved:
            await notify(open_msg)
        else:
            await notify(
                open_msg + "\n\n"
                "⚠️ КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ: позиция зарегистрирована "
                "только в оперативной памяти. Сохранение state.json НЕ "
                "подтверждено. После перезапуска состояние может быть "
                "потеряно. Новые BUY заблокированы. Требуется проверить "
                "сохранение state.json и сверить позицию с брокером."
            )
    else:
        research_logger.log_event("POSITION_RECOVERED", {
            "trade_id": trade_id, "ticker": ticker, "figi": figi,
            "order_id": order_id, "exec_lots": executed_lots,
            "recovery_source": pos.recovery_source, "partial": partial,
            "source": source, "state_saved": state_saved,
        })
        # Дефект P2 (этап BUY REGISTRATION FIX 2): сообщение «контроль
        # сохранён» отправлялось безусловно, даже если _save_state_checked()
        # вернул False, — создавалось впечатление надёжно восстановленной
        # позиции при несохранённом state.json. Теперь при state_saved=True
        # отправляется прежнее уведомление; при state_saved=False —
        # отдельное критическое предупреждение (без утверждений о
        # сохранённом контроле; новые BUY остаются заблокированы
        # state_save_failed; ошибка сохранения не скрыта в логах).
        if state_saved:
            await notify(
                f"⚠️ <b>{ticker}</b> BUY исполнен ({executed_lots} лот.), "
                f"цена входа/SL/TP уточняются\n"
                f"Позиция зарегистрирована как RECOVERY — контроль сохранён, "
                f"автозакрытие отключено."
            )
        else:
            await notify(
                f"⚠️ <b>{ticker}</b> BUY исполнен ({executed_lots} лот.), "
                f"цена входа/SL/TP уточняются\n"
                f"⚠️ КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ: позиция RECOVERY обнаружена "
                f"и зарегистрирована только в оперативной памяти. "
                f"Сохранение state.json НЕ подтверждено. После перезапуска "
                f"состояние может быть потеряно. Новые BUY заблокированы. "
                f"Требуется проверить сохранение state.json и сверить "
                f"позицию с брокером."
            )

    if ledger is not None and order_id:
        # Дополнительный этап: подтверждение регистрации («позиция
        # зарегистрирована») пишется в реестр ТОЛЬКО после успешного
        # сохранения state.json. Реестр и state.json должны утверждать
        # одно и то же: если state.json не сохранён, позиция существует
        # только в памяти и будет потеряна при перезапуске — подтверждающий
        # reason-маркер заставил бы второй проход startup_recovery пропустить
        # заявку («позиция зарегистрирована» = «применение уже учтено»),
        # и подтверждённое исполнение потерялось бы. Без маркера заявка
        # остаётся доступной доведению до регистрации при перезапуске
        # (штатный сценарий восстановления после краха между record_sent
        # и регистрацией). Ошибка сохранения уже зафиксирована
        # _save_state_checked (STATE_SAVE_FAILED + блокировка новых входов).
        if state_saved:
            ledger.record_status(
                order_id, ledger_status, trade_id=trade_id, figi=figi, ticker=ticker,
                side="BUY", requested_lots=requested_lots,
                executed_lots=executed_lots, avg_exec_price=exec_price,
                reason=("позиция зарегистрирована; RECOVERY: " + str(pos.recovery_source))
                       if pos.state == PositionState.RECOVERY else "позиция зарегистрирована",
                source=source or "register_executed_buy",
            )
        else:
            logging.error(
                f"[register] {ticker}: state.json НЕ сохранён — подтверждение "
                f"регистрации заявки {order_id} в реестр НЕ пишется "
                f"(позиция в памяти; при перезапуске заявка будет доведена "
                f"до регистрации повторным восстановлением)"
            )
    return pos


async def poll_open_orders(client: TinkoffClient, monitor: ExecutionMonitor,
                           rm: RiskManager, ledger: OrderLedger):
    """
    Пошаговый (по одной заявке за цикл, без блокировки цикла) контроль
    незавершённых BUY-заявок из реестра (этап 5).

    Правила разрешения:
      - терминальный статус с исполнением -> регистрация через единую точку
        (RECOVERY при неполных данных); для CANCELLED/EXPIRED с исполненными
        лотами статус заявки сохраняется как CANCELLED/EXPIRED с частичным
        исполнением (дефект №2: статус и факт исполнения учитываются
        раздельно, в позицию идёт только реально исполненное количество);
      - FILL с lots_executed=0 — противоречие: UNCERTAIN, НЕ терминальный
        FILL, НЕ отказ (дефект №8);
      - терминальный статус БЕЗ исполнения (достоверное подтверждение брокера)
        -> заявка закрыта в реестре, BUY по FIGI разблокируется;
      - статус недоступен/заявка не найдена -> НЕ отказ: заявка остаётся в
        реестре (UNCERTAIN-логика), новые BUY по FIGI блокированы;
      - PARTIALLYFILL или NEW старше ORDER_POLL_MAX_AGE_CYCLES -> сначала
        регистрация подтверждённых лотов, затем отмена остатка (дефект №1);
      - заявки с неизвестной стороной (side=None) опрашиваются: завершение
        без исполнения разрешает блокировку; признаки исполнения при
        неизвестной стороне остаются UNCERTAIN для оператора (дефект №4);
      - партия P0-2: терминальные заявки С ИСПОЛНЕНИЕМ (CANCELLED/EXPIRED с
        executed_lots>0, FILL при исполнении меньше запрошенного) остаются
        доступны для повторной сверки (order_ledger.open_orders) — поздний
        рост lots_executed после терминального статуса не теряется;
        повторная регистрация уже применённого исполнения без активной
        позиции запрещена («воскрешение» закрытой позиции).
    """
    for rec in list(ledger.open_orders(side="BUY")):
        order_id = rec["order_id"]
        figi = rec.get("figi") or ""
        ticker = rec.get("ticker") or figi or "?"

        state = await client.get_sandbox_order_state(order_id)
        if state is None:
            logging.warning(
                f"[poll] {ticker}: статус заявки {order_id} недоступен — заявка "
                f"остаётся в реестре (неопределённость, НЕ отказ)"
            )
            continue

        status = _status_name(getattr(state, "execution_report_status", None))
        le = int(getattr(state, "lots_executed", 0) or 0)

        # Достоверный отказ без исполнения (согласовано с владельцем)
        if status in ("REJECTED", "CANCELLED", "EXPIRED") and le <= 0:
            logging.warning(f"[poll] {ticker}: заявка {order_id} отклонена/отменена без исполнения — закрыта в реестре")
            ledger.record_status(order_id, status, executed_lots=le,
                                 reason="достоверный отказ без исполнения", source="poll")
            continue

        # Дефект №8: FILL без подтверждённых исполненных лотов — противоречие.
        # НЕ терминальный FILL и НЕ отказ: заявка остаётся UNCERTAIN в реестре
        # (блокирует BUY по FIGI), разрешается повторным запросом состояния.
        if status == "FILL" and le <= 0:
            logging.error(
                f"[poll] {ticker}: FILL, но lots_executed={le} — противоречие, "
                f"UNCERTAIN (заявка остаётся в реестре для повторного разрешения)"
            )
            client.uncertain_state = True
            ledger.record_uncertain(order_id, figi=figi, ticker=ticker,
                                    side="BUY", executed_lots=le,
                                    reason="FILL противоречие: lots_executed=0 при опросе",
                                    source="poll")
            continue

        # Партия P0-2: терминальная запись реестра при терминальном статусе
        # брокера и БЕЗ активной позиции. Исполнение, уже применённое к учёту
        # (executed_lots записи), повторно не регистрируется («воскрешение»
        # закрытой позиции запрещено). Поздний рост lots_executed сверх
        # учтённого — расхождение учёта с брокером: разрешается СВЕРКОЙ
        # (адопция по ДОСТОВЕРНОМУ снимку брокера показывает фактический
        # остаток), а не автоматической регистрацией по отчёту заявки.
        if (rec.get("status") in _TERMINAL_ORDER_STATUSES
                and status in _TERMINAL_ORDER_STATUSES
                and not monitor.has_active_position(figi)):
            recorded_le = int(rec.get("executed_lots") or 0)
            if le <= recorded_le:
                continue
            logging.error(
                f"[poll] {ticker}: поздний рост исполнения заявки {order_id} "
                f"({recorded_le} -> {le} лот.) при закрытой/отсутствующей позиции — "
                f"требуется сверка с брокером (адопция по снимку), "
                f"авторегистрация по отчёту заявки не выполняется"
            )
            client.uncertain_state = True
            continue

        if status == "FILL" or le > 0 or status == "PARTIALLYFILL":
            info = await client.get_instrument_info(figi) if figi else None
            lot = int(info.get("lot")) if info and info.get("lot") else 1
            exec_price = ExecutionMonitor._extract_price(state, le, lot)
            entry_commission = ExecutionMonitor._extract_commission(state)
            partial_like = status in ("PARTIALLYFILL", "NEW") or (status is None and le > 0)
            terminal_cancelled = status in ("CANCELLED", "EXPIRED")
            if partial_like:
                # Живой остаток (частичное исполнение или активная NEW): отмена по возрасту
                already_cancel = "cancel_requested" in (rec.get("reason") or "")
                age = _age_in_cycles(rec)
                need_cancel = status == "PARTIALLYFILL" or age >= ORDER_POLL_MAX_AGE_CYCLES
                if need_cancel and not already_cancel:
                    logging.warning(
                        f"[poll] {ticker}: заявка {order_id} жива "
                        f"({le} лот., возраст {age:.0f} циклов, status={status}) — отмена остатка"
                    )
                    # Дефект №1 (принцип): подтверждённое исполнение регистрируется
                    # ДО отмены остатка — отмена не должна потерять исполненные лоты.
                    if le > 0:
                        await register_executed_buy(
                            client, monitor, rm, ledger,
                            figi=figi, ticker=ticker, order_id=order_id,
                            trade_id=rec.get("trade_id") or "",
                            reason=rec.get("reason") or "",
                            signal_time=rec.get("created_at_utc") or rec.get("timestamp_utc") or "",
                            price_before_order=0.0,
                            order_send_time=rec.get("timestamp_utc") or "",
                            requested_lots=int(rec.get("requested_lots") or 0),
                            executed_lots=le, exec_price=exec_price, lot_size=lot,
                            entry_commission=entry_commission, partial=True, df=None,
                            source="poll", ledger_status=status or "PARTIALLYFILL",
                        )
                    cancel_resp = await client.cancel_order(order_id)
                    if cancel_resp is None:
                        ledger.record_uncertain(order_id, figi=figi, ticker=ticker,
                                                side="BUY", executed_lots=le,
                                                reason="cancel не подтверждён", source="poll")
                    else:
                        ledger.record_status(order_id, status or "PARTIALLYFILL",
                                             executed_lots=le,
                                             reason="cancel_requested: ожидание терминального статуса",
                                             source="poll")
                    continue
                if need_cancel and already_cancel:
                    # отменяли — ждём терминальный статус в следующих циклах
                    continue
                # Молодая NEW с исполнением: регистрируем исполненную часть сразу
                await register_executed_buy(
                    client, monitor, rm, ledger,
                    figi=figi, ticker=ticker, order_id=order_id,
                    trade_id=rec.get("trade_id") or "",
                    reason=rec.get("reason") or "",
                    signal_time=rec.get("created_at_utc") or rec.get("timestamp_utc") or "",
                    price_before_order=0.0,
                    order_send_time=rec.get("timestamp_utc") or "",
                    requested_lots=int(rec.get("requested_lots") or 0),
                    executed_lots=le, exec_price=exec_price, lot_size=lot,
                    entry_commission=entry_commission, partial=True, df=None,
                    source="poll", ledger_status=status or "PARTIALLYFILL",
                )
                continue

            if terminal_cancelled:
                # Дефект №2: CANCELLED/EXPIRED с исполненными лотами — это
                # отменённая/истёкшая заявка С ЧАСТИЧНЫМ ИСПОЛНЕНИЕМ, а не
                # «полное исполнение». Статус заявки и факт исполнения
                # учитываются раздельно: в позицию регистрируется только
                # реально исполненное количество (le), неисполненный остаток
                # НЕ попадает в позицию; повторный опрос не дублирует позицию
                # и не прибавляет уже учтённые лоты (идемпотентность
                # register_executed_buy + sync для RECOVERY — покрывает и
                # позднее исполнение после отмены).
                logging.warning(
                    f"[poll] {ticker}: заявка {order_id} {status} с частичным "
                    f"исполнением {le} лот — регистрирую исполненную часть "
                    f"(статус заявки сохраняется как {status})"
                )
                await register_executed_buy(
                    client, monitor, rm, ledger,
                    figi=figi, ticker=ticker, order_id=order_id,
                    trade_id=rec.get("trade_id") or "",
                    reason=rec.get("reason") or "",
                    signal_time=rec.get("created_at_utc") or rec.get("timestamp_utc") or "",
                    price_before_order=0.0,
                    order_send_time=rec.get("timestamp_utc") or "",
                    requested_lots=int(rec.get("requested_lots") or 0),
                    executed_lots=le, exec_price=exec_price, lot_size=lot,
                    entry_commission=entry_commission, partial=True, df=None,
                    source="poll", ledger_status=status,
                )
                continue

            # FILL: полная регистрация через единую точку
            await register_executed_buy(
                client, monitor, rm, ledger,
                figi=figi, ticker=ticker, order_id=order_id,
                trade_id=rec.get("trade_id") or "",
                reason=rec.get("reason") or "",
                signal_time=rec.get("created_at_utc") or rec.get("timestamp_utc") or "",
                price_before_order=0.0,
                order_send_time=rec.get("timestamp_utc") or "",
                requested_lots=int(rec.get("requested_lots") or 0),
                executed_lots=le, exec_price=exec_price, lot_size=lot,
                entry_commission=entry_commission, partial=False, df=None,
                source="poll", ledger_status="FILL",
            )
            continue

        # NEW без исполнения — заявка жива: наблюдение, при старении отмена остатка
        if status == "NEW":
            already_cancel = "cancel_requested" in (rec.get("reason") or "")
            age = _age_in_cycles(rec)
            if age >= ORDER_POLL_MAX_AGE_CYCLES and not already_cancel:
                logging.warning(
                    f"[poll] {ticker}: заявка {order_id} не исполняется "
                    f"(возраст {age:.0f} циклов) — отмена остатка"
                )
                cancel_resp = await client.cancel_order(order_id)
                if cancel_resp is None:
                    ledger.record_uncertain(order_id, figi=figi, ticker=ticker,
                                            side="BUY", executed_lots=le,
                                            reason="cancel не подтверждён", source="poll")
                else:
                    ledger.record_status(order_id, "NEW", executed_lots=le,
                                         reason="cancel_requested: ожидание терминального статуса",
                                         source="poll")
                continue
            ledger.record_status(order_id, "NEW", executed_lots=le,
                                 reason="активна, под наблюдением опроса", source="poll")
            continue

        # Неизвестный/нестандартный статус — не теряем заявку
        logging.warning(f"[poll] {ticker}: нестандартный статус {status!r} заявки {order_id} — остаётся в реестре")
        ledger.record_status(order_id, "NEW" if status is None else status, executed_lots=le,
                             reason=f"нестандартный статус {status!r}", source="poll")

    # Дефект №4: заявки с НЕИЗВЕСТНОЙ стороной (side=None, например из
    # стартовой сверки с брокером) тоже опрашиваются — иначе неопределённость
    # не разрешится никогда. Позицию из такой заявки автоматически НЕ
    # регистрируем (направление неизвестно); терминальное завершение БЕЗ
    # исполнения — достоверное разрешение (блокировка снимается); признаки
    # исполнения при неизвестной стороне остаются UNCERTAIN (решение — оператор).
    for rec in list(ledger.open_orders()):
        if rec.get("side") is not None:
            continue
        order_id = rec["order_id"]
        figi = rec.get("figi") or ""
        ticker = rec.get("ticker") or figi or "?"

        state = await client.get_sandbox_order_state(order_id)
        if state is None:
            # статус недоступен — остаётся в неопределённости, блокировка сохраняется
            continue

        status = _status_name(getattr(state, "execution_report_status", None))
        le = int(getattr(state, "lots_executed", 0) or 0)

        if status in ("REJECTED", "CANCELLED", "EXPIRED") and le <= 0:
            ledger.record_status(order_id, status, executed_lots=le,
                                 reason="достоверное завершение без исполнения "
                                        "(сторона неизвестна) — блокировка снята",
                                 source="poll_unknown")
            continue
        if status == "FILL" or le > 0 or status == "PARTIALLYFILL":
            logging.error(
                f"[poll] {ticker}: заявка {order_id} с НЕИЗВЕСТНОЙ стороной имеет "
                f"признаки исполнения (status={status}, lots_executed={le}) — "
                f"остаётся UNCERTAIN, требуется разрешение оператором; "
                f"BUY заблокированы, позиции продолжают мониториться"
            )
            client.uncertain_state = True
            ledger.record_uncertain(order_id, figi=figi, ticker=ticker, side=None,
                                    executed_lots=le,
                                    reason="исполнение при неизвестной стороне — "
                                           "авторегистрация запрещена, требуется оператор",
                                    source="poll_unknown")
            continue
        ledger.record_status(order_id, status or "NEW", executed_lots=le,
                             reason="активна (сторона неизвестна)", source="poll_unknown")


def _try_bind_recovery_entry(monitor: ExecutionMonitor, rm: RiskManager,
                             ledger: Optional[OrderLedger], pos) -> bool:
    """Пакет В4: автопривязка entry_order_id к адоптированной RECOVERY-позиции.

    Условия (все обязательны — иначе отказ и прежнее поведение «оператор»):
      - позиция RECOVERY, direction BUY, без entry_order_id;
      - в реестре ровно ОДНА BUY-заявка по этому FIGI с исполнением > 0
        (любая вторая BUY с исполнением — неоднозначность);
      - executed_lots этой заявки == quantity позиции;
      - статус записи НЕ REJECTED и НЕ INTENT (отказ/неотправленная заявка
        не может быть источником позиции);
      - order_id не входит в closed_entry_order_ids (защита от «воскрешения»).

    Запись может быть терминальной (FILL/CANCELLED/EXPIRED) или UNCERTAIN
    (реальный кейс: адоптированная позиция + «отчёт заявки НЕ привязан...»).
    Совпадение quantity (подтверждённого брокером при адопции) с единственным
    исполненным BUY по FIGI — достаточное доказательство принадлежности.

    При успехе: entry_order_id записывается, state.json сохраняется немедленно.
    Реестр: терминальный статус с маркером автопривязки пишется, если запись
    уже терминальна ИЛИ неопределённость разрешена подтверждённой сверкой
    (_uncertainty_resolved_by_reconcile, партия P0-5). Действующая
    UNCERTAIN-запись НЕ перезаписывается — она снимется штатным путём P0-5
    (после успешной сверки повторным терминальным отчётом); блокировка BUY
    по FIGI сохраняется до этого момента, привязка от этого не зависит.
    """
    if pos.state != PositionState.RECOVERY or pos.direction != "BUY":
        return False
    if pos.entry_order_id or ledger is None:
        return False
    try:
        qty = int(pos.quantity)
    except (TypeError, ValueError):
        return False
    if qty <= 0:
        return False
    buy_with_execution = []
    for oid, rec in ledger.latest_all().items():
        if rec.get("side") != "BUY" or rec.get("figi") != pos.figi:
            continue
        try:
            executed = int(rec.get("executed_lots") or 0)
        except (TypeError, ValueError):
            executed = 0
        if executed > 0:
            buy_with_execution.append((str(oid), rec, executed))
    if len(buy_with_execution) != 1:
        return False
    oid, rec, executed = buy_with_execution[0]
    rec_status = str(rec.get("status") or "")
    if rec_status in ("REJECTED", "INTENT"):
        return False
    if executed != qty:
        return False
    if oid in monitor.closed_entry_order_ids:
        return False
    pos.entry_order_id = oid
    state_saved = _save_state_checked(monitor, rm)
    # Реестр: терминальный статус пишется только если он не сотрёт
    # действующую (не разрешённую сверкой) UNCERTAIN-запись (P0-5).
    rec_uncertainty_pending = (
        rec_status == STATUS_UNCERTAIN
        and not _uncertainty_resolved_by_reconcile(rec)
    )
    if rec_status in _TERMINAL_ORDER_STATUSES or not rec_uncertainty_pending:
        ledger.record_status(
            oid, rec_status if rec_status in _TERMINAL_ORDER_STATUSES else "FILL",
            trade_id=rec.get("trade_id") or "",
            figi=pos.figi, ticker=pos.ticker, side="BUY",
            requested_lots=int(rec.get("requested_lots") or 0),
            executed_lots=executed,
            avg_exec_price=rec.get("avg_exec_price"),
            reason=(f"автопривязка к RECOVERY-позиции {pos.ticker}: единственный "
                    f"BUY по FIGI с executed_lots == quantity ({executed}); "
                    f"неопределённость разрешена (пакет В4)"),
            source="recovery_autobind",
        )
    else:
        logging.warning(
            f"[recovery] {pos.ticker}: UNCERTAIN-запись заявки {oid} сохранена "
            f"до подтверждённой сверки (P0-5); привязка выполнена, промоция "
            f"продолжится, BUY по FIGI остаются заблокированы до разрешения"
        )
    logging.warning(
        f"[recovery] {pos.ticker}: RECOVERY-позиция привязана к заявке {oid} "
        f"(единственный BUY с исполнением, {executed} лот., "
        f"ledger_status={rec_status}, state_saved={state_saved})"
    )
    research_logger.log_event("RECOVERY_ENTRY_BOUND", {
        "figi": pos.figi, "ticker": pos.ticker, "order_id": oid,
        "executed_lots": executed, "quantity": qty,
        "ledger_status": rec_status, "state_saved": state_saved,
        "uncertainty_pending": rec_uncertainty_pending,
    })
    return True


async def promote_recovery_positions(client: TinkoffClient, monitor: ExecutionMonitor,
                                     rm: RiskManager,
                                     ledger: Optional[OrderLedger] = None):
    """
    Восстановление цены входа для RECOVERY-позиций (этап 5).

    Если у позиции есть entry_order_id — запрашивается статус заявки; при
    ТЕРМИНАЛЬНОМ статусе заявки (партия P0-2: только FILL/CANCELLED/REJECTED/
    EXPIRED — пока заявка жива (NEW/PARTIALLYFILL), возможны дополнительные
    исполнения и промоция зафиксировала бы неполное количество) и появлении
    цены исполнения загружаются свечи, SL/TP рассчитываются штатной
    стратегией, позиция переводится в OPEN.

    Партия P0-3: финальное накопительное lots_executed входной заявки — это
    НЕ остаток позиции у брокера. Оно НЕ подставляется в quantity через
    sync_recovery_quantity (тот предназначен для достоверных снимков
    портфеля): обрабатывается record_recovery_entry_lots — обновляются
    счётчики исполнения, в позицию добавляются только ДЕЙСТВИТЕЛЬНО новые
    лоты (сверх учтённых и сверх уже виденных у этой заявки по реестру).
    Устаревший меньший lots_executed — противоречие: промоция откладывается,
    количество не уменьшается, фиксируется неопределённость. Остаток позиции
    остаётся равным последнему достоверно подтверждённому брокерскому
    остатку (сверка/адопция).

    Позиция БЕЗ order_id (адоптированная из reconcile/старта) остаётся в
    RECOVERY под контролем оператора: цена не заменяется произвольной,
    автоматическое закрытие ЗАПРЕЩЕНО.

    Партия P0-4: рост lots_executed ПОСЛЕ достоверной синхронизации остатка
    снимком портфеля (entry_snapshot_synced) — неоднозначен: дельта к
    остатку не добавляется (снимок мог уже учесть исполнение), счётчики
    догоняются по отчёту заявки, промоция ОТКЛАДЫВАЕТСЯ до сверки —
    RECOVERY -> OPEN выполняется только с остатком, подтверждённым
    достоверным снимком брокерского портфеля.
    """
    for pos in list(monitor.positions.values()):
        if pos.state != PositionState.RECOVERY:
            continue
        if not pos.entry_order_id:
            # Пакет В4: автопривязка адоптированной RECOVERY-позиции к
            # единственному терминальному BUY-отчёту по FIGI с
            # executed_lots == quantity. Не удалось — прежнее поведение
            # (цена не восстанавливается автоматически, решение за оператором).
            if not _try_bind_recovery_entry(monitor, rm, ledger, pos):
                _promote_throttle["n"] += 1
                if _promote_throttle["n"] % 20 == 1:
                    logging.warning(
                        f"[recovery] {pos.ticker}: RECOVERY без order_id "
                        f"(source={pos.recovery_source}) — цена входа не восстанавливается "
                        f"автоматически; закрытие только вручную оператором"
                    )
                continue
            # привязка выполнена — промоция продолжается в этом же цикле
        if pos.direction != "BUY":
            # Партия P0-3: накопительное исполнение входной BUY-заявки
            # применимо только к BUY (LONG)-позициям. SHORT-RECOVERY остаётся
            # под контролем оператора/сверки (количество — только по
            # достоверным снимкам брокера).
            _promote_throttle["alive"] += 1
            if _promote_throttle["alive"] % 20 == 1:
                logging.warning(
                    f"[recovery] {pos.ticker}: SHORT-RECOVERY с order_id — "
                    f"промоция по отчёту входной заявки не выполняется"
                )
            continue

        state = await client.get_sandbox_order_state(pos.entry_order_id)
        if state is None:
            continue

        # Партия P0-2: промоция ТОЛЬКО при терминальном статусе заявки.
        # Пока заявка жива (NEW/PARTIALLYFILL), возможны дополнительные
        # исполнения: количество продолжает синхронизироваться опросом/
        # reconcile, позиция остаётся в RECOVERY.
        status = _status_name(getattr(state, "execution_report_status", None))
        if status not in _TERMINAL_ORDER_STATUSES:
            _promote_throttle["alive"] += 1
            if _promote_throttle["alive"] % 20 == 1:
                logging.info(
                    f"[recovery] {pos.ticker}: заявка {pos.entry_order_id} ещё активна "
                    f"(status={status}) — возможны дополнительные исполнения, "
                    f"промоция отложена до терминального статуса"
                )
            continue

        broker_le = int(getattr(state, "lots_executed", 0) or 0)
        # Партия P0-3: финальное накопительное lots_executed входной заявки —
        # НЕ готовое значение текущего остатка позиции (остаток у брокера мог
        # измениться внешними операциями). Применяется только ДЕЙСТВИТЕЛЬНО
        # новый рост исполнения (сверх учтённых и сверх уже виденных у этой
        # заявки): счётчики исполнения обновляются, дельта новых лотов
        # добавляется к остатку. Устаревший меньший lots_executed —
        # противоречие: количество НЕ уменьшается, промоция откладывается.
        # Партия P0-4: рост ПОСЛЕ достоверной синхронизации остатка снимком
        # (entry_snapshot_synced) — неоднозначен: дельта к остатку НЕ
        # добавляется (снимок мог уже учесть исполнение), промоция
        # откладывается до сверки.
        if broker_le > 0:
            seen = None
            rec = ledger.latest(pos.entry_order_id) if ledger is not None else None
            if rec is not None:
                try:
                    seen = int(rec.get("executed_lots") or 0)
                except (TypeError, ValueError):
                    seen = None
            baseline = max(
                int(pos.entry_applied_lots or 0),
                seen if seen is not None else 0,
            )
            if broker_le < baseline:
                logging.error(
                    f"[recovery] {pos.ticker}: lots_executed={broker_le} заявки "
                    f"{pos.entry_order_id} меньше учтённых/виденных {baseline} — "
                    f"противоречие (исполнение не убывает), промоция отложена"
                )
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(
                        pos.entry_order_id, trade_id=pos.trade_id,
                        figi=pos.figi, ticker=pos.ticker, side="BUY",
                        executed_lots=broker_le,
                        reason=(f"противоречие при промоции RECOVERY: lots_executed="
                                f"{broker_le} меньше учтённых {baseline} — "
                                f"исполнение не убывает"),
                        source="promote_recovery",
                    )
                continue
            snapshot_covered_growth = (
                bool(pos.entry_snapshot_synced) and broker_le > baseline
            )
            if monitor.record_recovery_entry_lots(
                    pos, broker_le, order_id=pos.entry_order_id,
                    already_seen_lots=seen):
                # Изменение позиции/счётчиков сохраняется немедленно (D6)
                _save_state_checked(monitor, rm)
            if snapshot_covered_growth:
                # Партия P0-4: дельта к остатку НЕ добавлена (снимок мог уже
                # учесть это исполнение), счётчики догнаны по отчёту заявки.
                # Промоция ОТКЛАДЫВАЕТСЯ до сверки: RECOVERY -> OPEN выполняется
                # только с остатком, подтверждённым достоверным снимком.
                # Неопределённость — существующей архитектурой.
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(
                        pos.entry_order_id, trade_id=pos.trade_id,
                        figi=pos.figi, ticker=pos.ticker, side="BUY",
                        executed_lots=broker_le,
                        reason=(f"рост lots_executed={broker_le} сверх учтённых "
                                f"{baseline} после достоверной синхронизации остатка "
                                f"по снимку портфеля — промоция отложена до сверки"),
                        source="promote_recovery",
                    )
                logging.warning(
                    f"[recovery] {pos.ticker}: рост исполнения заявки "
                    f"{pos.entry_order_id} ({baseline} -> {broker_le} лот.) после "
                    f"синхронизации остатка снимком портфеля — промоция "
                    f"отложена до сверки (quantity={pos.quantity} сохранён)"
                )
                continue

        le = broker_le or pos.executed_lots or pos.quantity
        exec_price = ExecutionMonitor._extract_price(state, le, pos.lot_size)
        if not exec_price or exec_price <= 0:
            continue

        df = await _build_candles_df(client, pos.figi)
        if df is None:
            logging.warning(f"[recovery] {pos.ticker}: цена восстановлена ({exec_price:.4f}), свечей мало — остаётся RECOVERY")
            continue

        sl_price, tp_price = rm.calculate_sl_tp(exec_price, pos.direction, df)
        if sl_price is None or tp_price is None:
            logging.warning(f"[recovery] {pos.ticker}: SL/TP не рассчитаны — остаётся RECOVERY")
            continue

        if monitor.promote_recovery(pos, exec_price, sl_price, tp_price, execution_price=exec_price):
            logging.warning(
                f"[recovery] {pos.ticker}: RECOVERY→OPEN entry={exec_price:.4f} "
                f"SL={sl_price:.4f} TP={tp_price:.4f} qty={pos.quantity}"
            )
            research_logger.log_event("POSITION_RECOVERY_RESOLVED", {
                "ticker": pos.ticker, "figi": pos.figi, "order_id": pos.entry_order_id,
                "exec_price": exec_price, "exec_lots": pos.quantity,
                "sl": sl_price, "tp": tp_price,
            })
            _save_state_checked(monitor, rm)
            await notify(
                f"✅ <b>{pos.ticker}</b>: RECOVERY разрешена\n"
                f"Цена входа: {exec_price:.4f}\nSL: {sl_price:.4f}\nTP: {tp_price:.4f}\n"
                f"Лотов: {pos.quantity} — позиция под штатным контролем."
            )


async def startup_recovery(client: TinkoffClient, monitor: ExecutionMonitor,
                           rm: RiskManager, ledger: OrderLedger):
    """
    Стартовая четырёхсторонняя сверка (этап 5, P1-1/P1-5):
    OrderLedger × state.json (уже загружен в monitor) × активные заявки
    брокера × позиции брокера.

    - заявка реестра без терминального статуса -> запрос статуса по order_id;
      исполнена -> регистрация позиции (RECOVERY при неполных данных);
      достоверный отказ без исполнения -> заявка закрыта (BUY разблокируется);
      статус недоступен/заявка не найдена -> UNCERTAIN (НЕ отказ), BUY по FIGI
      блокирован до разрешения;
    - активная заявка брокера без записи в реестре -> UNCERTAIN в реестре
      (неизвестная заявка), BUY по её FIGI блокирован;
    - позиция брокера без локального учёта -> RECOVERY (адопция).
    Торговых действий не выполняет — только восстановление учёта.
    """
    summary = {
        "positions_adopted": 0,
        "positions_unavailable": False,
        "unknown_broker_orders": 0,
        "orders_unavailable": False,
        "orders_executed": 0,
        "orders_resolved": 0,
        "orders_uncertain": 0,
        "orders_active": 0,
    }

    # 1. Нетерминальные заявки реестра: разрешение по статусу брокера
    #    (сначала заявки — чтобы связать позицию с order_id через единую точку)
    for rec in list(ledger.open_orders()):
        oid = rec["order_id"]
        side = rec.get("side")
        figi = rec.get("figi") or ""
        ticker = rec.get("ticker") or figi or "?"

        # Дефект №4: сторона неизвестна — автоматическая регистрация позиции
        # как BUY ЗАПРЕЩЕНА (нельзя разрешать неопределённость «по умолчанию»
        # в сторону покупки). Заявка остаётся UNCERTAIN: BUY по её FIGI (или
        # глобально, если FIGI неизвестен) блокированы до разрешения.
        if side is None:
            ledger.record_uncertain(
                oid, figi=figi, ticker=ticker, side=None,
                requested_lots=int(rec.get("requested_lots") or 0),
                executed_lots=int(rec.get("executed_lots") or 0),
                reason="сторона заявки неизвестна — авторегистрация запрещена, "
                       "требуется разрешение оператором",
                source="startup",
            )
            summary["orders_uncertain"] += 1
            continue

        state = await client.get_sandbox_order_state(oid)
        if state is None:
            ledger.record_uncertain(
                oid, figi=figi, ticker=ticker, side=side,
                requested_lots=int(rec.get("requested_lots") or 0),
                reason="статус при старте недоступен/заявка не найдена — отказ НЕ фиксируется",
                source="startup",
            )
            summary["orders_uncertain"] += 1
            continue

        status = _status_name(getattr(state, "execution_report_status", None))
        le = int(getattr(state, "lots_executed", 0) or 0)

        if status in ("REJECTED", "CANCELLED", "EXPIRED") and le <= 0:
            ledger.record_status(oid, status, executed_lots=le,
                                 reason="достоверный отказ без исполнения", source="startup")
            summary["orders_resolved"] += 1
            continue

        if status == "FILL" or le > 0 or status == "PARTIALLYFILL":
            if side == "SELL":
                ledger.record_status(oid, status or "SENT", executed_lots=le,
                                     reason="закрытие подтверждено брокером", source="startup")
                summary["orders_resolved"] += 1
                continue
            info = await client.get_instrument_info(figi) if figi else None
            lot = int(info.get("lot")) if info and info.get("lot") else 1
            exec_price = ExecutionMonitor._extract_price(state, le, lot)
            await register_executed_buy(
                client, monitor, rm, ledger,
                figi=figi, ticker=ticker, order_id=oid,
                trade_id=rec.get("trade_id") or "",
                reason=rec.get("reason") or "",
                signal_time=rec.get("timestamp_utc") or "",
                price_before_order=0.0,
                order_send_time=rec.get("timestamp_utc") or "",
                requested_lots=int(rec.get("requested_lots") or 0),
                executed_lots=le, exec_price=exec_price, lot_size=lot,
                entry_commission=ExecutionMonitor._extract_commission(state),
                partial=(status != "FILL"), df=None,
                source="startup_recovery",
                ledger_status=status or "PARTIALLYFILL",
            )
            summary["orders_executed"] += 1
            continue

        ledger.record_status(oid, status or "NEW", executed_lots=le,
                             reason="активна при старте — под наблюдением опроса", source="startup")
        summary["orders_active"] += 1

    # 1.5 Исправление (дефект №1 — восстановление после краха между
    #     record_sent() и регистрацией позиции). Терминальные FILL-заявки
    #     с подтверждённым исполнением НЕ попадают в open_orders()
    #     (полный FILL не является residual), поэтому первый проход их не
    #     разрешает: после перезапуска позиция не создавалась, бумаги
    #     обнаруживались только поздней адопцией reconcile как RECOVERY
    #     (без entry_order_id/цены входа). Второй проход доводит их до
    #     регистрации через единую точку register_executed_buy:
    #     - заявки с признаком применения (reason содержит
    #       «позиция зарегистрирована» / «исполнение применено») НЕ
    #       переобрабатываются — исполнение уже учтено ранее;
    #     - принадлежность к закрытой позиции отсекает guard
    #       register_executed_buy (entry_order_id/closed_entry_order_ids);
    #     - UNCERTAIN-записи не разрешаются автоматически.
    for rec in list(ledger.latest_all().values()):
        if rec.get("side") != "BUY" or rec.get("status") != STATUS_FILL:
            continue
        le = int(rec.get("executed_lots") or 0)
        if le <= 0:
            continue
        rec_reason = str(rec.get("reason") or "")
        if ("позиция зарегистрирована" in rec_reason
                or "исполнение применено" in rec_reason):
            continue  # применение к позиции уже подтверждено реестром
        oid = rec["order_id"]
        figi = rec.get("figi") or ""
        ticker = rec.get("ticker") or figi or "?"
        # Пытаемся получить актуальные данные заявки (цена исполнения);
        # при недоступности — данные из записи реестра (record_sent
        # сохраняет executed_lots и avg_exec_price).
        state = await client.get_sandbox_order_state(oid)
        if state is not None:
            le = int(getattr(state, "lots_executed", 0) or 0) or le
            info = await client.get_instrument_info(figi) if figi else None
            lot = int(info.get("lot")) if info and info.get("lot") else 1
            exec_price = ExecutionMonitor._extract_price(state, le, lot)
            entry_commission = ExecutionMonitor._extract_commission(state)
        else:
            info = None
            lot = 1
            exec_price = rec.get("avg_exec_price")
            entry_commission = None
        await register_executed_buy(
            client, monitor, rm, ledger,
            figi=figi, ticker=ticker, order_id=oid,
            trade_id=rec.get("trade_id") or "",
            reason=rec.get("reason") or "",
            signal_time=rec.get("created_at_utc") or rec.get("timestamp_utc") or "",
            price_before_order=0.0,
            order_send_time=rec.get("timestamp_utc") or "",
            requested_lots=int(rec.get("requested_lots") or 0),
            executed_lots=le, exec_price=exec_price, lot_size=lot,
            entry_commission=entry_commission,
            partial=(le != int(rec.get("requested_lots") or 0)), df=None,
            source="startup_recovery_fill",
            ledger_status="FILL",
        )
        summary["orders_executed"] += 1

    # 2. Активные заявки брокера, неизвестные реестру
    orders = await client.get_sandbox_orders()
    if orders is None:
        summary["orders_unavailable"] = True
        client.uncertain_state = True
        logging.error("[startup] Список активных заявок недоступен.")
    else:
        for o in orders:
            oid = getattr(o, "order_id", None)
            if not oid or ledger.has_order(str(oid)):
                continue
            figi = getattr(o, "figi", None)
            raw_dir = getattr(o, "direction", None)
            side = None
            try:
                side = "BUY" if int(raw_dir) == int(OrderDirection.ORDER_DIRECTION_BUY) else "SELL"
            except (TypeError, ValueError):
                side = None
            ledger.record_uncertain(
                str(oid), figi=figi, side=side,
                requested_lots=int(getattr(o, "lots_requested", 0) or 0),
                executed_lots=int(getattr(o, "lots_executed", 0) or 0),
                reason="активная заявка брокера без записи в реестре", source="startup",
            )
            summary["unknown_broker_orders"] += 1
            logging.error(f"[startup] Неизвестная заявка брокера {oid} (figi={figi}) — UNCERTAIN")
        if summary["unknown_broker_orders"]:
            await notify(
                f"⚠️ При старте обнаружены заявки брокера без записи в реестре: "
                f"{summary['unknown_broker_orders']} — переведены в UNCERTAIN. "
                f"BUY по их FIGI заблокированы; заявки БЕЗ FIGI блокируют ВСЕ новые BUY "
                f"глобально до разрешения. Позиции продолжают мониториться."
            )

    # 3. Позиции брокера (адопция того, что не зарегистрировано через заявки)
    positions = await client.get_positions()
    if positions is None:
        summary["positions_unavailable"] = True
        client.uncertain_state = True
        logging.error("[startup] Портфель недостоверен — адопция позиций отложена до reconcile.")
    else:
        broker = {f: p for f, p in positions.items()
                  if not f.startswith(("RUB", "USD", "EUR"))}
        for figi, pdata in broker.items():
            if monitor.has_active_position(figi):
                continue
            if pdata.direction == "UNKNOWN":
                continue
            info = await client.get_instrument_info(figi)
            ticker = info.get("ticker") if info else figi
            direction = "BUY" if pdata.direction == "LONG" else "SELL"
            lot = int(info.get("lot")) if info and info.get("lot") else 1
            monitor.register_recovery(
                figi=figi, ticker=ticker, direction=direction,
                quantity=abs(pdata.lots), lot_size=lot,
                recovery_source="adopted",
            )
            summary["positions_adopted"] += 1
            logging.error(
                f"[startup] {ticker}: позиция брокера без локального учёта — RECOVERY "
                f"({abs(pdata.lots)} лот, {direction})"
            )
            research_logger.log_event("POSITION_RECOVERED", {
                "figi": figi, "ticker": ticker, "direction": direction,
                "lots": abs(pdata.lots), "source": "startup_recovery",
            })
            await notify(
                f"⚠️ <b>{ticker}</b>: при старте обнаружена позиция брокера без локального учёта\n"
                f"Зарегистрирована как RECOVERY: {abs(pdata.lots)} лот ({direction})\n"
                f"Новые BUY по инструменту заблокированы. Автозакрытие отключено."
            )

    research_logger.log_event("STARTUP_RECOVERY", summary)
    logging.info(f"[startup] Восстановление завершено: {summary}")


# ============================================================
# TRADING CYCLE
# ============================================================

async def trading_cycle(client: TinkoffClient, rm: RiskManager, sm: StrategyManager, monitor: ExecutionMonitor, ledger: Optional[OrderLedger] = None):
    global reconcile_done
    if ledger is None:
        ledger = ledger_ref

    # 1. Цены: TOP-10 + открытые позиции
    figi_set = set(p.figi for p in monitor.positions.values())
    figi_set.update(i['figi'] for i in top_instruments)
    figi_to_monitor = list(figi_set)
    prices = await client.get_last_prices(figi_to_monitor)

    if prices is None:
        logging.error("[cycle] prices=None — monitoring_error=True, мониторинг продолжается")
        monitor.monitoring_error = True
        prices = {}

    # 2. SL/TP
    # С1: свечные экстремумы и ATR — ТОЛЬКО при включённом безубытке/трейлинге
    # (config.BE_TRAIL_ENABLED, по умолчанию False — ноль доп. вызовов API).
    extremes, atrs = {}, {}
    if getattr(config, "BE_TRAIL_ENABLED", False):
        open_figis = [p.figi for p in monitor.positions.values()
                      if p.state == PositionState.OPEN]
        if open_figis:
            extremes, atrs = await _fetch_extremes_and_atrs(client, open_figis)
    to_close = await monitor.check_all(prices, extremes=extremes or None, atrs=atrs or None)
    for pos in to_close:
        price = prices.get(pos.figi)
        if price is None or price <= 0:
            logging.error(f"[cycle] {pos.ticker}: нет цены для закрытия — пропуск (UNCERTAIN)")
            pos.state = PositionState.UNCERTAIN
            client.uncertain_state = True
            continue
        await close_position(client, pos, price)

    # 3. Обработка закрытий и UNCERTAIN
    await process_closing(client, monitor, rm)

    # 3.5 Этап 5: пошаговый контроль незавершённых BUY-заявок (не блокирует цикл)
    if ledger is not None:
        await poll_open_orders(client, monitor, rm, ledger)

    # 3.6 Этап 5: восстановление цены входа для RECOVERY-позиций
    if ledger is not None:
        await promote_recovery_positions(client, monitor, rm, ledger)

    # 4. Если uncertain — reconcile
    if client.is_uncertain():
        logging.warning("[cycle] uncertain_state=True — пробую reconcile.")
        await reconcile(client, monitor, ledger, rm)
        if client.is_uncertain():
            logging.warning("[cycle] reconcile не снял UNCERTAIN — новые входы запрещены.")
            _save_state_checked(monitor, rm)
            return

    # 5. Блокировки входов
    if not is_trading_enabled():
        logging.info("[cycle] Торговля выключена (нет trading.flag).")
        _save_state_checked(monitor, rm)
        return
    if not reconcile_done:
        logging.warning("[cycle] reconcile не завершён.")
        _save_state_checked(monitor, rm)
        return
    if monitor.monitoring_error:
        logging.warning("[cycle] monitoring_error=True — новые входы запрещены.")
        _save_state_checked(monitor, rm)
        return
    if state_save_failed:
        # Этап 5 (D6): незаписанное состояние не дополняется новыми рисками
        logging.error("[cycle] state.json не сохранён ранее — новые входы запрещены до успешного сохранения.")
        _save_state_checked(monitor, rm)
        return
    if ledger is not None and not ledger.is_reliable():
        # Дефект №5: недостоверный реестр (повреждение/ошибка чтения журнала) —
        # робот может «забыть» ранее созданные заявки. Новые BUY запрещены
        # до восстановления целостности ЖУРНАЛОМ ОПЕРАТОРА (журнал не
        # обрезается и не перезаписывается автоматически). Позиции
        # продолжают мониториться (SL/TP и закрытия работают).
        logging.error(
            "[cycle] Реестр заявок НЕДОСТОВЕРЕН — новые BUY запрещены до "
            f"восстановления целостности. Причина: {ledger.unreliable_reason}"
        )
        _save_state_checked(monitor, rm)
        return
    if ledger is not None and ledger.has_unknown_figi_open_orders():
        # Дефект №4: незавершённая заявка без FIGI — неизвестно, какой
        # инструмент она затрагивает. Новые BUY запрещены ГЛОБАЛЬНО до
        # разрешения неопределённости. Существующие позиции мониторятся.
        logging.error(
            "[cycle] Незавершённая заявка БЕЗ FIGI в реестре — новые BUY "
            "запрещены ГЛОБАЛЬНО до разрешения неопределённости."
        )
        _save_state_checked(monitor, rm)
        return

    # 6. Свежий портфель перед входами
    positions = await client.get_positions()
    if positions is None:
        logging.error("[cycle] Портфель недостоверен перед входом — новые входы запрещены.")
        client.uncertain_state = True
        reconcile_done = False
        _save_state_checked(monitor, rm)
        return

    # 7. Входы
    for inst in top_instruments:
        figi = inst['figi']
        price = prices.get(figi)
        if price is None or price <= 0:
            logging.info(f"{inst['ticker']}: нет цены, пропуск")
            continue
        pos_data = positions.get(figi)
        if (pos_data is not None and pos_data.lots != 0) or monitor.has_active_position(figi):
            logging.info(f"{inst['ticker']}: позиция уже открыта/в неопределённости, пропуск")
            continue
        if ledger is not None and ledger.figi_blocks_buy(figi):
            # Дефект №4: вход блокирует ЛЮБАЯ незавершённая/неопределённая
            # заявка по FIGI: незавершённая BUY, заявка с неизвестной стороной
            # (side=None) и UNCERTAIN любой стороны. Нельзя разрешать BUY
            # только потому, что сторона заявки неизвестна.
            logging.warning(f"{inst['ticker']}: в реестре есть незавершённая/неопределённая заявка по FIGI — вход заблокирован")
            continue

        df = await _build_candles_df(client, figi)
        if df is None:
            logging.info(f"{inst['ticker']}: мало свечей (<{config.MIN_CANDLES_FOR_SIGNAL}), пропуск")
            continue

        signal, reason = sm.get_signal(df, float(price), drop_incomplete=True)

        if signal == "HOLD":
            logging.info(f"{inst['ticker']}: HOLD ({reason})")
            _log_strategy_diagnostic(
                event_type="STRATEGY_HOLD", ticker=inst['ticker'], figi=figi,
                signal=signal, reason=reason, sm=sm, df=df, price=float(price),
            )
            continue

        if signal == "SELL":
            logging.info(f"{inst['ticker']}: SELL без позиции — шорт запрещён")
            _log_strategy_diagnostic(
                event_type="STRATEGY_SELL_IGNORED", ticker=inst['ticker'], figi=figi,
                signal=signal, reason=reason, sm=sm, df=df, price=float(price),
            )
            continue

        lots = await rm.calculate_position_size(figi, float(price), df)
        if lots <= 0:
            logging.info(f"{inst['ticker']}: размер позиции 0, пропуск")
            continue

        info = await client.get_instrument_info(figi)
        if not info:
            logging.error(f"{inst['ticker']}: instrument_info=None — пропуск")
            continue

        trade_id = uuid.uuid4().hex[:12]
        # Этап 5 (P0-1): order_id генерируется ДО записи SIGNAL и до сетевого вызова —
        # единый идентификатор намерения и заявки (контракт SDK 1.51.0 подтверждён)
        order_id = str(uuid.uuid4())
        order_send_time = datetime.now(timezone.utc).isoformat()
        research_logger.log_event("SIGNAL", {
            "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
            "signal": signal, "reason": reason, "price_before_order": float(price),
            "requested_lots": lots, "lot_size": info['lot'],
            "order_id": order_id,
        })

        # Этап 5 (P0-2): INTENT в реестр ДО сетевого вызова.
        # Запись не удалась — заявка НЕ отправляется (fail-closed).
        if ledger is None:
            logging.error(
                f"{inst['ticker']}: реестр заявок недоступен — вход заблокирован (fail-closed)"
            )
            _save_state_checked(monitor, rm)
            continue
        intent_ok = ledger.record_intent(
            order_id, trade_id=trade_id, figi=figi, ticker=inst['ticker'],
            side="BUY", requested_lots=lots,
        )
        if not intent_ok:
            logging.error(
                f"{inst['ticker']}: INTENT не сохранён — заявка НЕ отправляется (fail-closed)"
            )
            research_logger.log_event("ORDER_INTENT_FAILED", {
                "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
                "order_id": order_id, "requested_lots": lots,
            })
            _save_state_checked(monitor, rm)
            continue
        research_logger.log_event("ORDER_INTENT", {
            "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
            "order_id": order_id, "requested_lots": lots,
        })

        result = await client.post_market_order(figi, lots, OrderDirection.ORDER_DIRECTION_BUY, order_id=order_id)

        sent_status_name = _status_name(getattr(result.state, "execution_report_status", None)) if result.state is not None else None
        sent_le = int(getattr(result.state, "lots_executed", 0) or 0) if result.state is not None else None

        if result.state is None:
            logging.error(
                f"{inst['ticker']}: state=None (order_id={result.order_id}) — UNCERTAIN. "
                f"Повторная отправка запрещена."
            )
            client.uncertain_state = True
            if ledger is not None:
                ledger.record_uncertain(
                    order_id, trade_id=trade_id, figi=figi, ticker=inst['ticker'],
                    side="BUY", requested_lots=lots,
                    reason="post: ответ не получен — заявка могла быть принята брокером",
                    source="post_market_order",
                    error=str(result.error) if result.error else None,
                )
            research_logger.log_event("BUY_UNKNOWN", {
                "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
                "order_id": result.order_id,
            })
            _save_state_checked(monitor, rm)
            continue

        # Этап 5: SENT сразу после ответа (post-ответ уже содержит статус и исполнение)
        if ledger is not None:
            ledger.record_sent(
                order_id, trade_id=trade_id, figi=figi, ticker=inst['ticker'],
                side="BUY", requested_lots=lots, executed_lots=sent_le,
                status_hint=sent_status_name, source="post_response",
            )

        status = getattr(result.state, 'execution_report_status', None)
        order_id = result.order_id

        # --- PARTIALLYFILL (этап 5: без потери заявки/позиции) ---
        if status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL:
            logging.error(
                f"{inst['ticker']}: ЧАСТИЧНОЕ исполнение покупки — отмена остатка "
                f"(order_id={order_id})"
            )
            research_logger.log_event("PARTIAL_FILL", {"trade_id": trade_id, "order_id": order_id})
            client.uncertain_state = True

            cancel_resp = await client.cancel_order(order_id)
            if cancel_resp is None:
                logging.error(f"{inst['ticker']}: cancel_order вернул None — UNCERTAIN; заявка остаётся в реестре")
                if ledger is not None:
                    ledger.record_uncertain(order_id, trade_id=trade_id, figi=figi,
                                            ticker=inst['ticker'], side="BUY", requested_lots=lots,
                                            reason="cancel не подтверждён", source="partial_branch")
                _save_state_checked(monitor, rm)
                continue

            final_state = await client.get_sandbox_order_state(order_id)
            if final_state is None:
                logging.error(f"{inst['ticker']}: final state=None — UNCERTAIN; заявка остаётся в реестре")
                if ledger is not None:
                    ledger.record_uncertain(order_id, trade_id=trade_id, figi=figi,
                                            ticker=inst['ticker'], side="BUY", requested_lots=lots,
                                            reason="статус после отмены недоступен", source="partial_branch")
                _save_state_checked(monitor, rm)
                continue

            fresh_positions = await client.get_positions()
            if fresh_positions is None:
                # Снимок портфеля недостоверен. Но это НЕ отменяет подтверждённое
                # брокером исполнение (Дефект №1): если final_state показывает
                # исполненные лоты — регистрируем их; иначе заявка остаётся в
                # реестре (неопределённость) под опросом.
                fs_status_np = _status_name(getattr(final_state, "execution_report_status", None))
                le_fs = int(getattr(final_state, "lots_executed", 0) or 0)
                if le_fs > 0:
                    logging.warning(
                        f"{inst['ticker']}: снимок портфеля недоступен, но брокер подтвердил "
                        f"{le_fs} исполненных лот — регистрация исполнения (Дефект №1)"
                    )
                    exec_price = ExecutionMonitor._extract_price(final_state, le_fs, info['lot'])
                    await register_executed_buy(
                        client, monitor, rm, ledger,
                        figi=figi, ticker=inst['ticker'], order_id=order_id, trade_id=trade_id,
                        reason=reason, signal_time=order_send_time,
                        price_before_order=float(price), order_send_time=order_send_time,
                        requested_lots=lots, executed_lots=le_fs, exec_price=exec_price,
                        lot_size=info['lot'],
                        entry_commission=ExecutionMonitor._extract_commission(final_state),
                        partial=True, df=df,
                        source="partial_branch_no_snapshot",
                        ledger_status=fs_status_np or "PARTIALLYFILL",
                    )
                else:
                    logging.error(
                        f"{inst['ticker']}: fresh positions=None, исполнение не подтверждено — "
                        f"заявка остаётся в реестре (неопределённость)"
                    )
                _save_state_checked(monitor, rm)
                continue

            pos_data = fresh_positions.get(figi)
            if pos_data is None or pos_data.lots == 0:
                le_after = int(getattr(final_state, "lots_executed", 0) or 0)
                if le_after <= 0:
                    logging.warning(f"{inst['ticker']}: после отмены позиции нет, исполнение не подтверждено брокером — заявка закрыта в реестре")
                    if ledger is not None:
                        ledger.record_status(order_id, "CANCELLED", executed_lots=0,
                                             reason="остаток отменён; достоверный отказ без исполнения",
                                             source="partial_branch")
                    _save_state_checked(monitor, rm)
                    continue
                # Дефект №1: брокер подтвердил исполненные лоты, но свежий снимок
                # портфеля позицию пока НЕ показывает (задержка маппинга сделок).
                # Отсутствие позиции в снимке НЕ отменяет подтверждённое
                # исполнение: регистрируем ровно исполненное количество; при
                # недоступной цене — RECOVERY. Статус заявки в реестре —
                # фактический статус брокера (частичное исполнение отменённой
                # заявки, Дефект №2), а не «исполнение не подтвердилось».
                fs_status = _status_name(getattr(final_state, "execution_report_status", None))
                logging.warning(
                    f"{inst['ticker']}: остаток отменён, портфель пока не показывает позицию, "
                    f"но брокер подтвердил {le_after} исполненных лот — регистрация (Дефект №1)"
                )
                exec_price = ExecutionMonitor._extract_price(final_state, le_after, info['lot'])
                await register_executed_buy(
                    client, monitor, rm, ledger,
                    figi=figi, ticker=inst['ticker'], order_id=order_id, trade_id=trade_id,
                    reason=reason, signal_time=order_send_time,
                    price_before_order=float(price), order_send_time=order_send_time,
                    requested_lots=lots, executed_lots=le_after, exec_price=exec_price,
                    lot_size=info['lot'],
                    entry_commission=ExecutionMonitor._extract_commission(final_state),
                    partial=True, df=df, source="partial_branch_confirmed",
                    ledger_status=fs_status or "PARTIALLYFILL",
                )
                _save_state_checked(monitor, rm)
                continue

            le = pos_data.lots
            exec_price = ExecutionMonitor._extract_price(final_state, le, info['lot'])

            # Этап 5: единая точка регистрации — исполненная часть не теряется:
            # при exec_price=None/SL-TP недоступных позиция попадает в RECOVERY
            await register_executed_buy(
                client, monitor, rm, ledger,
                figi=figi, ticker=inst['ticker'], order_id=order_id, trade_id=trade_id,
                reason=reason, signal_time=order_send_time,
                price_before_order=float(price), order_send_time=order_send_time,
                requested_lots=lots, executed_lots=le, exec_price=exec_price,
                lot_size=info['lot'],
                entry_commission=ExecutionMonitor._extract_commission(final_state),
                partial=True, df=df, source="partial_branch", ledger_status="CANCELLED",
            )
            _save_state_checked(monitor, rm)
            continue

        # --- НЕ FILL (этап 4.1: проверка фактического исполнения; этап 5: без потери заявки) ---
        if status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
            logging.warning(
                f"{inst['ticker']}: не исполнено ({status}) — проверка фактического "
                f"исполнения (order_id={order_id})"
            )
            research_logger.log_event("ORDER_NOT_FILLED", {
                "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
                "order_id": order_id, "status": str(status),
                "requested_lots": lots,
            })

            if not order_id:
                logging.error(f"{inst['ticker']}: нет order_id — UNCERTAIN")
                client.uncertain_state = True
                _save_state_checked(monitor, rm)
                continue

            # Запросить фактическое состояние заявки
            state = await client.get_sandbox_order_state(order_id)
            if state is None:
                logging.error(
                    f"{inst['ticker']}: state=None после не-FILL (order_id={order_id}) — UNCERTAIN; "
                    f"заявка остаётся в реестре"
                )
                client.uncertain_state = True
                if ledger is not None:
                    ledger.record_uncertain(order_id, trade_id=trade_id, figi=figi,
                                            ticker=inst['ticker'], side="BUY", requested_lots=lots,
                                            reason="статус после не-FILL недоступен", source="not_fill_branch")
                _save_state_checked(monitor, rm)
                continue

            le = int(getattr(state, "lots_executed", 0) or 0)
            if le <= 0:
                # Этап 5 (D4): заявка ещё жива — НЕ выбрасываем из учёта:
                # остаётся в реестре, опрос продолжится; при старении остаток отменяется
                logging.warning(
                    f"{inst['ticker']}: lots_executed=0 — заявка активна, остаётся под наблюдением"
                )
                if ledger is not None:
                    ledger.record_status(order_id, _status_name(getattr(state, "execution_report_status", None)) or "NEW",
                                         executed_lots=0, source="not_fill_branch")
                _save_state_checked(monitor, rm)
                continue

            # Исполнено > 0 -> регистрация через единую точку (как при FILL)
            exec_price = ExecutionMonitor._extract_price(state, le, info['lot'])
            await register_executed_buy(
                client, monitor, rm, ledger,
                figi=figi, ticker=inst['ticker'], order_id=order_id, trade_id=trade_id,
                reason=reason, signal_time=order_send_time,
                price_before_order=float(price), order_send_time=order_send_time,
                requested_lots=lots, executed_lots=le, exec_price=exec_price,
                lot_size=info['lot'],
                entry_commission=ExecutionMonitor._extract_commission(state),
                partial=False, df=df, source="not_fill_branch",
                ledger_status=_status_name(getattr(state, "execution_report_status", None)) or "SENT",
            )
            _save_state_checked(monitor, rm)
            continue

        # --- FILL (этап 5: подтверждённое исполнение не может быть потеряно; D1 устранён) ---
        state = await client.get_sandbox_order_state(order_id) if order_id else None
        if state is None:
            # Ответ post_sandbox_order уже подтвердил FILL. Дефект D1(а) устранён:
            # регистрируем позицию по данным post-ответа (RECOVERY при неполных данных),
            # цена/SL-TP доуточняются опросом (promote_recovery_positions).
            logging.error(
                f"{inst['ticker']}: state=None после FILL — регистрация по данным post-ответа "
                f"(order_id={order_id})"
            )
            research_logger.log_event("ORDER_STATE_UNKNOWN", {
                "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
                "order_id": order_id,
            })
            client.uncertain_state = True
            post_le = sent_le if sent_le else int(getattr(result.state, "lots_executed", 0) or 0)
            post_price = ExecutionMonitor._extract_price(result.state, post_le, info['lot']) if post_le > 0 else None
            await register_executed_buy(
                client, monitor, rm, ledger,
                figi=figi, ticker=inst['ticker'], order_id=order_id, trade_id=trade_id,
                reason=reason, signal_time=order_send_time,
                price_before_order=float(price), order_send_time=order_send_time,
                requested_lots=lots, executed_lots=post_le, exec_price=post_price,
                lot_size=info['lot'],
                entry_commission=ExecutionMonitor._extract_commission(result.state),
                partial=False, df=df, source="fill_branch_post_response", ledger_status="FILL",
            )
            _save_state_checked(monitor, rm)
            continue

        le = int(getattr(state, 'lots_executed', 0) or 0)
        if le <= 0:
            # FILL без исполнения — противоречие: заявку не теряем, опрос продолжится
            logging.error(f"{inst['ticker']}: FILL, но lots_executed={le} — UNCERTAIN (заявка в реестре)")
            client.uncertain_state = True
            if ledger is not None:
                ledger.record_uncertain(order_id, trade_id=trade_id, figi=figi,
                                        ticker=inst['ticker'], side="BUY", requested_lots=lots,
                                        reason="FILL противоречие: lots_executed=0", source="fill_branch")
            _save_state_checked(monitor, rm)
            continue

        exec_price = ExecutionMonitor._extract_price(state, le, info['lot'])

        # Единая точка регистрации: полные данные -> OPEN, неполные -> RECOVERY.
        # Ни один выход не оставляет исполнение без регистрации.
        await register_executed_buy(
            client, monitor, rm, ledger,
            figi=figi, ticker=inst['ticker'], order_id=order_id, trade_id=trade_id,
            reason=reason, signal_time=order_send_time,
            price_before_order=float(price), order_send_time=order_send_time,
            requested_lots=lots, executed_lots=le, exec_price=exec_price,
            lot_size=info['lot'],
            entry_commission=ExecutionMonitor._extract_commission(state),
            partial=False, df=df, source="fill_branch", ledger_status="FILL",
        )
        _save_state_checked(monitor, rm)

    # Пакет В1: безусловное сохранение в КОНЦЕ КАЖДОГО цикла. Раньше тихий
    # цикл (HOLD, без входов/ошибок) не сохранял CLOSED/CLOSING-переходы:
    # рестарт возвращал проданные позиции как фантомные OPEN ->
    # вечный RECONCILE_MISMATCH. Теперь окно устаревания state.json —
    # максимум один цикл (~TRADING_CYCLE_SECONDS).
    _save_state_checked(monitor, rm)


# ============================================================
# MAIN
# ============================================================

async def main():
    global telegram_bot, main_client, top_instruments, monitor_ref, rm_ref, ledger_ref

    config.validate()

    if config.ENABLE_TELEGRAM and TelegramBot:
        try:
            telegram_bot = TelegramBot(
                config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID,
                config.TELEGRAM_ALLOWED_USER_ID,
                {'start_trading': start_trading, 'stop_trading': stop_trading, 'get_status': get_status},
            )
            await asyncio.wait_for(telegram_bot.application.initialize(), timeout=20.0)
            await asyncio.wait_for(telegram_bot.application.start(), timeout=20.0)
            await asyncio.wait_for(telegram_bot.application.updater.start_polling(), timeout=20.0)
            await telegram_bot.register_commands()
            logging.info("Telegram-бот запущен (initialize → start → polling → commands).")
            await notify("🤖 Tsclp_v2.2_volatile запущен. Управление — через trading.flag.")
        except Exception as e:
            logging.warning(
                f"Telegram недоступен ({type(e).__name__}: {e!r}). "
                f"Робот работает БЕЗ уведомлений."
            )
            await _stop_telegram_bot()
        asyncio.create_task(_telegram_watchdog())
        logging.info("[tg-watchdog] Сторож запущен (60 сек, полный перезапуск при падении).")

    async with TinkoffClient(config.TINKOFF_API_TOKEN, config.USE_SANDBOX) as client:
        main_client = client
        rm = RiskManager(client)
        rm_ref = rm
        sm = StrategyManager()
        monitor = ExecutionMonitor(client)
        monitor_ref = monitor
        screener = Screener()

        # Этап 5: реестр заявок — до старта циклов (рядом с state.json)
        ledger = OrderLedger(LEDGER_FILE)
        ledger.load()
        ledger_ref = ledger

        # Дефект №5: недостоверный реестр (повреждение/ошибка чтения журнала).
        # Новые BUY запрещены до восстановления целостности оператором;
        # существующие позиции продолжают мониториться.
        if not ledger.is_reliable():
            _msg = (
                "⛔ Реестр заявок НЕДОСТОВЕРЕН (повреждение/ошибка чтения "
                f"{LEDGER_FILE}).\nНовые BUY ЗАПРЕЩЕНЫ до восстановления "
                f"целостности журнала.\nЖурнал не обрезался и не перезаписывался "
                f"(автовосстановление отключено — восстановление вручную).\n"
                f"Причина: {ledger.unreliable_reason}\n"
                f"Позиции продолжают мониториться."
            )
            logging.error(f"[main] {_msg}")
            research_logger.log_event("LEDGER_UNRELIABLE", {
                "reason": ledger.unreliable_reason, "file": LEDGER_FILE,
            })
            await notify(_msg)

        state_loaded = _load_state(monitor, rm)
        if not state_loaded:
            client.uncertain_state = True
            logging.warning(
                "[main] state.json отсутствует/повреждён — новые входы запрещены до сверки."
            )

        # Этап 5 (P1-1/P1-5): четырёхсторонняя стартовая сверка
        # (реестр × state.json × активные заявки брокера × позиции брокера).
        # Неизвестная позиция/заявка не исчезают из учёта: RECOVERY/UNCERTAIN
        # с блокировкой BUY по FIGI до разрешения.
        await startup_recovery(client, monitor, rm, ledger)

        instruments = await screener.select_top_volatile()
        if not instruments:
            client.uncertain_state = True
            msg = "⚠️ Скрининг не удался. Новые входы ЗАПРЕЩЕНЫ."
            logging.error(msg)
            research_logger.log_event("SCREEN_FAILED", {})
            await notify(msg)
            while True:
                await asyncio.sleep(60)

        top_instruments = instruments
        logging.info(f"[screener] Инструменты: {', '.join(i['ticker'] for i in instruments)}")

        await reconcile(client, monitor, ledger, rm)

        if not reconcile_done:
            logging.warning("[main] reconcile не завершён — новые входы запрещены.")
            client.uncertain_state = True

        while True:
            try:
                await trading_cycle(client, rm, sm, monitor, ledger)
            except Exception as e:
                logging.exception(f"Ошибка цикла: {type(e).__name__}: {e}")
                research_logger.log_event("CYCLE_ERROR", {"error": str(e)})
            await asyncio.sleep(config.TRADING_CYCLE_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Остановлено пользователем.")
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {type(e).__name__}: {e}")