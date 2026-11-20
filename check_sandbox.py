# check_sandbox.py
# Tsclp_v2.2_volatile — отчётность по песочнице.
#
# ИЗМЕНЕНИЯ (этап 12.2):
#   - _is_trade_operation: BUY=15, SELL=22 (подтверждено диагностикой SDK 1.51.0).
#   - _is_broker_fee: BROKER_FEE=19 (фактическая комиссия).
#   - turnover — только BUY/SELL (15, 22).
#   - actual_commission — сумма abs(payment) по BROKER_FEE (19).
#   - model_commission = turnover * COMMISSION_PER_SIDE (без * 2).
#   - Вывод: model, actual, diff (actual - model).
#   - Диагностика: sum_buy, sum_sell, sum_broker_fee, capital_change, unexplained.
#   - realized/unrealized — без изменений (фактическая комиссия НЕ вычитается повторно).
#   - CSV — существующие поля, без новых столбцов.
#
# READ-ONLY: get_sandbox_portfolio, get_sandbox_operations. Заявок не отправляет.
#
# Запуск: .\.venv\Scripts\python.exe check_sandbox.py

import asyncio
import csv
import logging
import os
from datetime import datetime, timedelta

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import quotation_to_decimal

import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CSV_FILE = "sandbox_pnl.csv"

# OperationType (подтверждено диагностикой SDK 1.51.0)
OP_BUY = 15
OP_SELL = 22
OP_BROKER_FEE = 19


def save_to_csv(row: dict):
    file_exists = os.path.isfile(CSV_FILE)
    fieldnames = [
        "timestamp", "equity_rub", "starting_balance_rub",
        "realized_pnl_rub", "unrealized_pnl_rub", "estimated_commission_rub",
        "positions_count",
    ]
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _op_type_int(op) -> int:
    """Извлекает operation_type как int. -1 при ошибке."""
    raw = getattr(op, "operation_type", None)
    if raw is None:
        return -1
    try:
        return int(raw)
    except (TypeError, ValueError):
        return -1


def _is_trade_operation(op) -> bool:
    """BUY=15 или SELL=22."""
    return _op_type_int(op) in (OP_BUY, OP_SELL)


def _is_broker_fee(op) -> bool:
    """BROKER_FEE=19."""
    return _op_type_int(op) == OP_BROKER_FEE


def _payment_float(op) -> float:
    """payment как float (со знаком). 0.0 при отсутствии."""
    p = getattr(op, "payment", None)
    if p is None:
        return 0.0
    try:
        return float(quotation_to_decimal(p))
    except Exception:
        return 0.0


async def main():
    config.validate()

    if not config.STARTING_BALANCE:
        print("❌ STARTING_BALANCE не задан в .env. Укажите начальный баланс песочницы.")
        return
    try:
        starting_balance = float(config.STARTING_BALANCE)
    except ValueError:
        print(f"❌ STARTING_BALANCE должен быть числом, получено: {config.STARTING_BALANCE}")
        return

    async with AsyncSandboxClient(config.TINKOFF_API_TOKEN) as client:
        now = datetime.now()
        account_id = config.TINKOFF_ACCOUNT_ID
        if not account_id:
            accounts = await client.users.get_accounts()
            if not accounts.accounts:
                print("❌ Нет песочных счетов.")
                return
            account_id = accounts.accounts[0].id

        print("=" * 60)
        print(f"📊 ПЕСОЧНИЦА T-BANK | Счёт: {account_id[:12]}...")
        print(f"🕐 {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        portfolio = await client.sandbox.get_sandbox_portfolio(account_id=account_id)
        equity = float(quotation_to_decimal(portfolio.total_amount_portfolio))

        positions = []
        unrealized = 0.0
        for pos in portfolio.positions:
            qty = float(quotation_to_decimal(pos.quantity))
            if qty == 0:
                continue
            price = float(quotation_to_decimal(pos.current_price)) if pos.current_price else 0
            avg = float(quotation_to_decimal(pos.average_position_price)) if pos.average_position_price else 0
            unrealized += (price - avg) * qty
            positions.append((pos.figi, qty, price, avg))

        print(f"\n💰 ПОРТФЕЛЬ:")
        print(f"   Стоимость: {equity:,.2f} ₽")
        print(f"   Стартовый баланс: {starting_balance:,.2f} ₽")

        # Операции за 30 дней
        from_ = now - timedelta(days=30)
        ops = await client.sandbox.get_sandbox_operations(
            account_id=account_id, from_=from_, to=now,
        )

        # --- Сбор диагностики ---
        turnover = 0.0
        sum_buy = 0.0
        sum_sell = 0.0
        sum_broker_fee = 0.0
        trades_count = 0
        fees_count = 0
        skipped_count = 0
        buy_count = 0
        sell_count = 0

        for op in ops.operations:
            if _is_trade_operation(op):
                payment = _payment_float(op)
                turnover += abs(payment)
                trades_count += 1
                op_t = _op_type_int(op)
                if op_t == OP_BUY:
                    sum_buy += abs(payment)
                    buy_count += 1
                elif op_t == OP_SELL:
                    sum_sell += abs(payment)
                    sell_count += 1
            elif _is_broker_fee(op):
                payment = _payment_float(op)
                sum_broker_fee += abs(payment)  # расход — положительная величина
                fees_count += 1
            else:
                skipped_count += 1

        # --- Комиссии ---
        model_commission = turnover * config.COMMISSION_PER_SIDE
        actual_commission = sum_broker_fee
        commission_diff = actual_commission - model_commission

        # --- P&L (без изменений) ---
        realized = equity - starting_balance - unrealized
        capital_change = equity - starting_balance

        # --- Диагностическая сверка ---
        # Ожидаемое изменение капитала (упрощённо): (SELL - BUY) - BROKER_FEE
        # Фактическое: equity - starting_balance
        # unexplained = фактическое - ожидаемое
        expected_capital = (sum_sell - sum_buy) - sum_broker_fee
        unexplained = capital_change - expected_capital

        print(f"\n📈 P&L:")
        print(f"   Реализованный (оценка): {realized:+,.2f} ₽")
        print(f"   Нереализованный: {unrealized:+,.2f} ₽")
        print(f"   Итого: {capital_change:+,.2f} ₽")
        print(f"   ⚠️ P&L НЕ включает комиссию (в песочнице не рассчитывается).")

        print(f"\n💸 КОМИССИИ:")
        print(f"   Модельная (0.05% × оборот): {model_commission:,.2f} ₽")
        print(f"   Фактическая (BROKER_FEE):   {actual_commission:,.2f} ₽")
        print(f"   Разница (факт - модель):    {commission_diff:+,.2f} ₽")

        print(f"\n🔍 ДИАГНОСТИКА ОПЕРАЦИЙ:")
        print(f"   BUY  (15): {buy_count} операций, сумма {sum_buy:,.2f} ₽")
        print(f"   SELL (22): {sell_count} операций, сумма {sum_sell:,.2f} ₽")
        print(f"   BROKER_FEE (19): {fees_count} операций, сумма {sum_broker_fee:,.2f} ₽")
        print(f"   Пропущено (не торговые/не комиссии): {skipped_count}")
        print(f"   Изменение капитала (equity - start): {capital_change:+,.2f} ₽")
        print(f"   Ожидаемое (SELL - BUY - FEE): {expected_capital:+,.2f} ₽")
        print(f"   Необъяснённое расхождение: {unexplained:+,.2f} ₽")

        print(f"\n📦 ПОЗИЦИИ ({len(positions)}):")
        for figi, qty, price, avg in positions:
            print(f"   • {figi[:12]}... {qty:g} шт. × {price:.2f} (средняя {avg:.2f})")

        save_to_csv({
            "timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
            "equity_rub": f"{equity:.2f}",
            "starting_balance_rub": f"{starting_balance:.2f}",
            "realized_pnl_rub": f"{realized:.2f}",
            "unrealized_pnl_rub": f"{unrealized:.2f}",
            "estimated_commission_rub": f"{model_commission:.2f}",  # модельная (как раньше)
            "positions_count": len(positions),
        })
        print(f"\n💾 Сохранено в {os.path.abspath(CSV_FILE)}")


if __name__ == "__main__":
    asyncio.run(main())