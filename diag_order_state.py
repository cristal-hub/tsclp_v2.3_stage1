# diag_order_state.py
# Tsclp_v2.2_volatile — ЭТАП 0: read-only диагностика OrderState.
#
# НАЗНАЧЕНИЕ: получить фактические значения executed_order_price,
#             lots_executed, lots_requested, lots_left, lots_cancelled,
#             direction, stages[], lot_size для СУЩЕСТВУЮЩЕЙ заявки.
#
# ЗАПРЕЩЕНО: отправлять/отменять заявки, пополнять счёт, менять позиции.
# ТОЛЬКО ЧТЕНИЕ. ТОЛЬКО SANDBOX.
#
# НЕ ИМПОРТИРУЕТ main.py. НЕ ВЫЗЫВАЕТ post_market_order, cancel_order, pay_in.
# НЕ ВЫВОДИТ токен, полный account_id, секреты.
# НЕ ВЫВОДИТ str(exception) — только type(exception).__name__.
#
# Запуск: .\.venv\Scripts\python.exe diag_order_state.py
#
# Если подходящей заявки нет — скрипт сообщит об этом и завершится.
# НЕ создавать новую заявку ради диагностики.

import asyncio
import csv
import json
import logging
import os
from decimal import Decimal
from typing import Optional

from t_tech.invest import InstrumentIdType
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import quotation_to_decimal

import config

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Ограничение числа API-запросов get_sandbox_order_state.
MAX_STATE_REQUESTS = 12

# Разделитель research_trades.csv — подтверждено по logger.py: delimiter=";".
TRADES_DELIMITER = ";"


def _mask_account(acc: Optional[str]) -> str:
    """Маскировка account_id: первые 6 символов + '...'."""
    if not acc:
        return "<None>"
    return acc[:6] + "..." if len(acc) > 6 else "***"


def _mask_order_id(oid: Optional[str]) -> str:
    """Маскировка order_id в общем списке: первые 8 символов + '...'."""
    if not oid:
        return "<None>"
    return oid[:8] + "..." if len(oid) > 8 else oid


def _safe_error(e: Exception) -> str:
    """Только тип исключения, без str(e) — чтобы не вывести чувствительные детали."""
    return type(e).__name__


def _money_to_dict(money) -> Optional[dict]:
    """MoneyValue/Quotation -> dict с точными units и nano (без округления)."""
    if money is None:
        return None
    try:
        return {
            "units": int(getattr(money, "units", 0)),
            "nano": int(getattr(money, "nano", 0)),
            "currency": getattr(money, "currency", None),
        }
    except Exception:
        return None


def _money_to_decimal_str(money) -> Optional[str]:
    """Строковое представление Decimal без потери точности."""
    if money is None:
        return None
    try:
        return str(quotation_to_decimal(money))
    except Exception:
        return None


def _direction_name(raw) -> str:
    """
    Человекочитаемое направление по фактическому полю SDK.
    OrderDirection: 0=UNSPECIFIED, 1=BUY, 2=SELL (подтверждено документацией).
    """
    if raw is None:
        return "None"
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return f"unknown({raw!r})"
    mapping = {0: "UNSPECIFIED", 1: "BUY", 2: "SELL"}
    return mapping.get(value, f"unknown({value})")


def _collect_order_ids_from_event(rec: dict) -> list[str]:
    """
    Извлекает order_id из корневых полей и вложенного payload.
    Возвращает список (может быть пустым).
    """
    found: list[str] = []
    keys = ("order_id", "entry_order_id", "close_order_id", "orderId")
    sources = [rec, rec.get("payload", {}) if isinstance(rec.get("payload"), dict) else {}]
    for src in sources:
        for key in keys:
            val = src.get(key)
            if val and isinstance(val, str):
                found.append(val)
    return found


def _load_candidates_from_logs() -> list[str]:
    """
    Поиск order_id в research_events.jsonl и research_trades.csv.
    Возвращает уникальные order_id в порядке появления (свежие — позже).
    """
    candidates: list[str] = []
    seen: set[str] = set()

    # research_events.jsonl
    events_file = "research_events.jsonl"
    if os.path.isfile(events_file):
        try:
            with open(events_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    for oid in _collect_order_ids_from_event(rec):
                        if oid not in seen:
                            seen.add(oid)
                            candidates.append(oid)
        except Exception as e:
            print(f"  ⚠️ events: {_safe_error(e)}")

    # research_trades.csv — разделитель ";" (подтверждено по logger.py)
    trades_file = "research_trades.csv"
    if os.path.isfile(trades_file):
        try:
            with open(trades_file, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f, delimiter=TRADES_DELIMITER)
                for row in reader:
                    val = row.get("order_id")
                    if val and isinstance(val, str) and val not in seen:
                        seen.add(val)
                        candidates.append(val)
        except Exception as e:
            print(f"  ⚠️ trades: {_safe_error(e)}")

    return candidates


async def _get_lot_size(client, figi: str) -> Optional[int]:
    """Получить lot_size через get_instrument_by. Read-only."""
    try:
        resp = await client.instruments.get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
            id=figi,
        )
        lot = getattr(resp.instrument, "lot", None)
        return int(lot) if lot is not None else None
    except Exception as e:
        print(f"  ⚠️ get_instrument_info({figi}): {_safe_error(e)}")
        return None


async def _fetch_active_order_ids(client, account_id: str) -> list[str]:
    """Получить ID активных заявок (read-only)."""
    active: list[str] = []
    try:
        if hasattr(client.sandbox, "get_sandbox_orders"):
            resp = await client.sandbox.get_sandbox_orders(account_id=account_id)
            orders = getattr(resp, "orders", []) or []
            for o in orders:
                oid = getattr(o, "order_id", None)
                if oid:
                    active.append(oid)
        else:
            print("  ⚠️ get_sandbox_orders отсутствует в SDK. Пропуск.")
    except Exception as e:
        print(f"  ⚠️ get_sandbox_orders: {_safe_error(e)}")
    return active


async def main():
    print("=" * 70)
    print("ЭТАП 0: READ-ONLY ДИАГНОСТИКА OrderState")
    print("ТОЛЬКО ЧТЕНИЕ. НИКАКИХ ЗАЯВОК. ТОЛЬКО SANDBOX.")
    print("=" * 70)

    config.validate()

    if not config.USE_SANDBOX:
        print("❌ USE_SANDBOX=False. Скрипт работает только в песочнице.")
        return

    # 1. Кандидаты из логов
    log_candidates = _load_candidates_from_logs()
    print(f"\n[1] Найдено order_id в логах: {len(log_candidates)}")
    for i, oid in enumerate(log_candidates[:20]):
        print(f"    {i+1}. {_mask_order_id(oid)}")
    if len(log_candidates) > 20:
        print(f"    ... и ещё {len(log_candidates) - 20}")

    # 2. Подключение
    async with AsyncSandboxClient(config.TINKOFF_API_TOKEN) as client:
        account_id = config.TINKOFF_ACCOUNT_ID
        if not account_id:
            accounts = await client.users.get_accounts()
            if not accounts.accounts:
                print("❌ Нет песочных счетов.")
                return
            account_id = accounts.accounts[0].id
            print(f"\n[!] TINKOFF_ACCOUNT_ID не задан. Используется первый счёт: {_mask_account(account_id)}")
        else:
            print(f"\n[2] Счёт: {_mask_account(account_id)}")

        # 3. Портфель (read-only)
        print("\n" + "=" * 70)
        print("[3] ПОРТФЕЛЬ (read-only)")
        print("=" * 70)
        try:
            portfolio = await client.sandbox.get_sandbox_portfolio(account_id=account_id)
            for pos in portfolio.positions:
                qty_dec = quotation_to_decimal(pos.quantity) if pos.quantity else None
                lots_attr = getattr(pos, "quantity_lots", None)
                lots_dec = quotation_to_decimal(lots_attr) if lots_attr is not None else None
                print(f"  figi={pos.figi[:12]}... "
                      f"quantity={qty_dec} (штуки) "
                      f"quantity_lots={lots_dec} (deprecated)")
        except Exception as e:
            print(f"  ❌ Ошибка портфеля: {_safe_error(e)}")

        # 4. Активные заявки (read-only)
        print("\n" + "=" * 70)
        print("[4] АКТИВНЫЕ ЗАЯВКИ (read-only)")
        print("=" * 70)
        active_ids = await _fetch_active_order_ids(client, account_id)
        if not active_ids:
            print("  (нет активных заявок)")
        else:
            for oid in active_ids:
                print(f"  order_id={_mask_order_id(oid)}")

        # 5. Объединение: приоритет — активные, затем логи (свежие в конце — первыми)
        #    Свежие заявки из логов ставим в начало (обратный порядок log_candidates).
        ordered: list[str] = []
        seen: set[str] = set()
        for oid in active_ids:
            if oid not in seen:
                seen.add(oid)
                ordered.append(oid)
        for oid in reversed(log_candidates):
            if oid not in seen:
                seen.add(oid)
                ordered.append(oid)

        if not ordered:
            print("\n" + "=" * 70)
            print("❌ НЕТ КАНДИДАТОВ ДЛЯ ДИАГНОСТИКИ")
            print("=" * 70)
            print("В логах и активных заявках нет order_id.")
            print("НЕ создавайте новую заявку ради диагностики.")
            print("Формула _extract_price остаётся НЕПОДТВЕРЖДЁННОЙ.")
            return

        # Ограничение API-запросов
        to_check = ordered[:MAX_STATE_REQUESTS]
        skipped = len(ordered) - len(to_check)
        print("\n" + "=" * 70)
        print(f"[5] ДИАГНОСТИКА OrderState (к проверке: {len(to_check)}, пропущено: {skipped})")
        print("=" * 70)

        found_with_execution = False
        for idx, oid in enumerate(to_check, 1):
            print(f"\n--- [{idx}/{len(to_check)}] order_id: {oid} ---")
            try:
                state = await client.sandbox.get_sandbox_order_state(
                    account_id=account_id, order_id=oid,
                )
            except Exception as e:
                print(f"  ⚠️ Ошибка запроса: {_safe_error(e)}")
                continue

            if state is None:
                print("  ⚠️ state=None (заявка не найдена или ошибка API)")
                continue

            status = getattr(state, "execution_report_status", None)
            direction_raw = getattr(state, "direction", None)
            lots_req = getattr(state, "lots_requested", None)
            lots_exec = getattr(state, "lots_executed", None)
            lots_left = getattr(state, "lots_left", None)
            lots_cancelled = getattr(state, "lots_cancelled", None)
            figi = getattr(state, "figi", None)

            print(f"  status={status}")
            print(f"  direction={_direction_name(direction_raw)} (raw={direction_raw!r})")
            print(f"  figi={figi}")
            print(f"  lots_requested={lots_req}")
            print(f"  lots_executed={lots_exec}")
            print(f"  lots_left={lots_left}")
            print(f"  lots_cancelled={lots_cancelled}")

            eop = getattr(state, "executed_order_price", None)
            eop_dict = _money_to_dict(eop)
            eop_dec = _money_to_decimal_str(eop)
            print(f"  executed_order_price: {eop_dict}")
            print(f"  executed_order_price (Decimal): {eop_dec}")

            lot_size = await _get_lot_size(client, figi) if figi else None
            print(f"  lot_size: {lot_size}")

            stages = getattr(state, "stages", None) or []
            print(f"  stages: {len(stages)}")
            for i, st in enumerate(stages):
                st_price = _money_to_dict(getattr(st, "price", None))
                st_qty = getattr(st, "quantity", None)
                print(f"    stage[{i}]: price={st_price} quantity={st_qty}")

            if lots_exec and int(lots_exec) > 0:
                found_with_execution = True
                print(f"  ✅ ЕСТЬ ИСПОЛНЕНИЕ: lots_executed={lots_exec}")

                if eop_dict and lot_size and int(lots_exec) > 0:
                    eop_total = Decimal(eop_dict["units"]) + Decimal(eop_dict["nano"]) / Decimal(10**9)
                    le = Decimal(int(lots_exec))
                    ls = Decimal(int(lot_size))
                    print(f"\n  ПРЕДВАРИТЕЛЬНЫЕ ГИПОТЕЗЫ (только для вывода):")
                    if le > 0:
                        print(f"    H1 (цена за штуку, /(lots*lot)): {eop_total / (le * ls)}")
                    if ls > 0:
                        print(f"    H2 (цена за лот, /lot):          {eop_total / ls}")
                    print(f"    H3 (как есть):                   {eop_total}")
                    print(f"    (lot_size={lot_size}, lots_executed={lots_exec})")

                # Ранний выход: нашли исполнение — дальше не перебираем
                if skipped > 0:
                    print(f"\n  ℹ️ Остановка: найдено исполнение. Пропущено {skipped} кандидатов.")
                break

        print("\n" + "=" * 70)
        if found_with_execution:
            print("✅ Найдена заявка с исполнением. Пришлите вывод выше.")
        else:
            print("⚠️ Заявок с lots_executed > 0 не найдено.")
            print("   Формула _extract_price остаётся НЕПОДТВЕРЖДЁННОЙ.")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())