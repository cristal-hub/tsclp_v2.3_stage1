# diag_operations.py
# Tsclp_v2.2_volatile — ЭТАП 12.1: read-only диагностика ops.operations.
#
# ТОЛЬКО ЧТЕНИЕ. НЕ МЕНЯЕТ ПОРТФЕЛЬ, НЕ ВЫСТАВЛЯЕТ ЗАЯВКИ.
# Не печатает токены, account_id, персональные данные.
#
# Запуск: .\.venv\Scripts\python.exe diag_operations.py

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import quotation_to_decimal

import config

logging.basicConfig(level=logging.WARNING)


def _safe_str(v):
    """Строковое представление без секретов."""
    if v is None:
        return "None"
    return str(v)[:100]


async def main():
    config.validate()

    async with AsyncSandboxClient(config.TINKOFF_API_TOKEN) as client:
        account_id = config.TINKOFF_ACCOUNT_ID
        if not account_id:
            accounts = await client.users.get_accounts()
            if not accounts.accounts:
                print("❌ Нет песочных счетов.")
                return
            account_id = accounts.accounts[0].id

        now = datetime.now()
        from_ = now - timedelta(days=30)

        print("=" * 70)
        print("ДИАГНОСТИКА ops.operations (READ-ONLY)")
        print("=" * 70)

        ops = await client.sandbox.get_sandbox_operations(
            account_id=account_id, from_=from_, to=now,
        )

        print(f"\nВсего операций: {len(ops.operations)}\n")

        # Сводка по типам
        type_counts = defaultdict(int)
        type_payments = defaultdict(float)
        type_commissions = defaultdict(float)
        with_figi = 0
        without_figi = 0
        types_seen = set()

        for i, op in enumerate(ops.operations):
            op_type = getattr(op, "operation_type", None)
            type_attr = getattr(op, "type", None)
            figi = getattr(op, "figi", None)
            payment = getattr(op, "payment", None)
            commission = getattr(op, "commission", None)
            quantity = getattr(op, "quantity", None)
            price = getattr(op, "price", None)
            state = getattr(op, "state", None)
            date = getattr(op, "date", None)
            instrument_type = getattr(op, "instrument_type", None)
            description = getattr(op, "description", None)

            # Сводка
            key = _safe_str(op_type)
            types_seen.add(key)
            type_counts[key] += 1

            try:
                if payment is not None:
                    type_payments[key] += abs(float(quotation_to_decimal(payment)))
            except Exception:
                pass

            try:
                if commission is not None:
                    type_commissions[key] += abs(float(quotation_to_decimal(commission)))
            except Exception:
                pass

            if figi:
                with_figi += 1
            else:
                without_figi += 1

            # Детальный вывод (первые 10 + все с figi)
            if i < 10 or figi:
                print(f"--- op[{i}] ---")
                print(f"  operation_type: {op_type!r} (python: {type(op_type).__name__})")
                if type_attr is not None:
                    print(f"  type: {type_attr!r} (python: {type(type_attr).__name__})")
                else:
                    print(f"  type: <отсутствует>")
                print(f"  figi: {_safe_str(figi)}")
                print(f"  payment: {payment!r}")
                print(f"  commission: {commission!r}")
                print(f"  quantity: {quantity!r}")
                print(f"  price: {price!r}")
                print(f"  state: {state!r}")
                print(f"  date: {date!r}")
                print(f"  instrument_type: {instrument_type!r}")
                print(f"  description: {_safe_str(description)}")
                print()

        # Итоговая сводка
        print("=" * 70)
        print("СВОДКА ПО ТИПАМ")
        print("=" * 70)
        for key in sorted(types_seen):
            print(f"  {key}: count={type_counts[key]}, "
                  f"sum_payment={type_payments[key]:.2f}, "
                  f"sum_commission={type_commissions[key]:.4f}")

        print(f"\nС FIGI: {with_figi}, БЕЗ FIGI: {without_figi}")

        # Попытка определить BUY/SELL по документации T-Банка
        print("\n" + "=" * 70)
        print("ПРОВЕРКА: OPERATION_TYPE_BUY=15, OPERATION_TYPE_SELL=22 (документация)")
        print("=" * 70)
        buy_sell_count = 0
        for op in ops.operations:
            op_type = getattr(op, "operation_type", None)
            try:
                val = int(op_type)
                if val in (15, 22):
                    buy_sell_count += 1
            except (TypeError, ValueError):
                pass
        print(f"Операций с operation_type in (15, 22): {buy_sell_count}")


if __name__ == "__main__":
    asyncio.run(main())