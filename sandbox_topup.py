
import asyncio
from decimal import Decimal, ROUND_HALF_UP

import config
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.schemas import MoneyValue
from t_tech.invest.utils import quotation_to_decimal


ACCOUNT_ID = "538f670d-fdc1-449f-86bf-3afbedef7962"
TARGET = Decimal("100000.00")


async def main():
    config.validate()

    if config.TINKOFF_ACCOUNT_ID and config.TINKOFF_ACCOUNT_ID != ACCOUNT_ID:
        raise RuntimeError("ID счёта в config.py не совпадает с целевым!")

    async with AsyncSandboxClient(config.TINKOFF_API_TOKEN) as client:
        positions = await client.sandbox.get_sandbox_positions(
            account_id=ACCOUNT_ID
        )

        balance = Decimal("0")
        for item in positions.money:
            if getattr(item, "currency", "").lower() == "rub":
                balance += Decimal(str(quotation_to_decimal(item)))

        print(f"Текущий остаток: {balance:.2f} ₽")
        print(f"Цель:             {TARGET:.2f} ₽")

        if balance >= TARGET:
            print("Пополнение не требуется. Ничего не отправлено.")
            return

        amount = (TARGET - balance).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        print(f"Сумма пополнения: {amount:.2f} ₽")
        print("\nВНИМАНИЕ: сейчас будет отправлена операция SandboxPayIn.")

        confirmation = input(
            f"Для подтверждения введи ТОЧНО: ПОПОЛНИТЬ {amount:.2f}\n> "
        )

        if confirmation != f"ПОПОЛНИТЬ {amount:.2f}":
            print("Отменено. Пополнение не выполнялось.")
            return

        rubles = int(amount)
        nano = int((amount - Decimal(rubles)) * Decimal("1000000000"))

        money = MoneyValue(currency="rub", units=rubles, nano=nano)

        result = await client.sandbox.sandbox_pay_in(
            account_id=ACCOUNT_ID,
            amount=money,
        )

        new_balance = Decimal(str(quotation_to_decimal(result.balance)))

        print("\nОтвет SandboxPayIn получен.")
        print(f"Баланс по ответу сервера: {new_balance:.2f} ₽")
        print(f"Отклонение от цели:       {new_balance - TARGET:+.2f} ₽")


if __name__ == "__main__":
    asyncio.run(main())
