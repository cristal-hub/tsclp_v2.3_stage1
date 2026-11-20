# diag_unknown_position.py
# Tsclp_v2.2_volatile — READ-ONLY диагностика неизвестной позиции BBG004S68CP5.
#
# ТОЛЬКО ЧТЕНИЕ. НЕ ОТПРАВЛЯЕТ ЗАЯВКИ. НЕ МЕНЯЕТ ПОРТФЕЛЬ.
#
# Запуск: .\.venv\Scripts\python.exe diag_unknown_position.py

import asyncio
import logging
from datetime import datetime, timedelta

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import quotation_to_decimal

import config

logging.basicConfig(level=logging.WARNING)

TARGET_FIGI = "BBG004S68CP5"


async def main():
    print("=" * 70)
    print(f"ДИАГНОСТИКА {TARGET_FIGI} (READ-ONLY)")
    print("=" * 70)

    config.validate()

    async with AsyncSandboxClient(config.TINKOFF_API_TOKEN) as client:
        account_id = config.TINKOFF_ACCOUNT_ID
        if not account_id:
            accounts = await client.users.get_accounts()
            account_id = accounts.accounts[0].id

        # 1. Портфель
        print(f"\n[1] ПОРТФЕЛЬ (счёт: {account_id[:8]}...)")
        portfolio = await client.sandbox.get_sandbox_portfolio(account_id=account_id)
        found = False
        for pos in portfolio.positions:
            if pos.figi == TARGET_FIGI:
                found = True
                qty = quotation_to_decimal(pos.quantity)
                avg = quotation_to_decimal(pos.average_position_price) if pos.average_position_price else None
                cur = quotation_to_decimal(pos.current_price) if pos.current_price else None
                ey = quotation_to_decimal(pos.expected_yield) if getattr(pos, "expected_yield", None) else None
                blocked = getattr(pos, "blocked_lots", None)
                print(f"  ✅ НАЙДЕНА:")
                print(f"     figi: {pos.figi}")
                print(f"     quantity: {qty} (штуки)")
                print(f"     average_position_price: {avg}")
                print(f"     current_price: {cur}")
                print(f"     expected_yield: {ey}")
                print(f"     blocked_lots: {blocked}")
                print(f"     instrument_type: {getattr(pos, 'instrument_type', None)}")
                print(f"     quantity_lots: {getattr(pos, 'quantity_lots', None)}")

        if not found:
            print(f"  ❌ {TARGET_FIGI} НЕ НАЙДЕН в портфеле.")

        # 2. Инструмент
        print(f"\n[2] ИНСТРУМЕНТ")
        try:
            from t_tech.invest import InstrumentIdType
            resp = await client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id=TARGET_FIGI,
            )
            inst = resp.instrument
            print(f"  ticker: {inst.ticker}")
            print(f"  name: {inst.name}")
            print(f"  lot: {inst.lot}")
            print(f"  instrument_type: {getattr(inst, 'instrument_type', None)}")
        except Exception as e:
            print(f"  ❌ Ошибка: {type(e).__name__}: {e}")

        # 3. Операции за 7 дней
        print(f"\n[3] ОПЕРАЦИИ (7 дней)")
        now = datetime.now()
        from_ = now - timedelta(days=7)
        ops = await client.sandbox.get_sandbox_operations(
            account_id=account_id, from_=from_, to=now,
        )
        found_ops = 0
        for op in ops.operations:
            if getattr(op, "figi", None) != TARGET_FIGI:
                continue
            found_ops += 1
            op_type = getattr(op, "operation_type", None)
            payment = quotation_to_decimal(op.payment) if op.payment else None
            quantity = getattr(op, "quantity", None)
            price = quotation_to_decimal(op.price) if getattr(op, "price", None) else None
            date = getattr(op, "date", None)
            print(f"  op_type={op_type!r} date={date} quantity={quantity} "
                  f"payment={payment} price={price} "
                  f"operation_id={getattr(op, 'id', None)}")

        if found_ops == 0:
            print(f"  ❌ Операций по {TARGET_FIGI} за 7 дней нет.")

        # 4. Заявки (все)
        print(f"\n[4] ЗАЯВКИ (активные)")
        try:
            orders = await client.sandbox.get_sandbox_orders(account_id=account_id)
            found_orders = 0
            for o in getattr(orders, "orders", []) or []:
                if getattr(o, "figi", None) != TARGET_FIGI:
                    continue
                found_orders += 1
                print(f"  order_id={getattr(o, 'order_id', None)} "
                      f"status={getattr(o, 'execution_report_status', None)} "
                      f"lots_requested={getattr(o, 'lots_requested', None)} "
                      f"lots_executed={getattr(o, 'lots_executed', None)} "
                      f"direction={getattr(o, 'direction', None)}")
            if found_orders == 0:
                print(f"  ❌ Активных заявок по {TARGET_FIGI} нет.")
        except Exception as e:
            print(f"  ❌ Ошибка: {type(e).__name__}: {e}")

    print("\n" + "=" * 70)
    print("ГОТОВО.")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())