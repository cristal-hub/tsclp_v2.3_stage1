
import asyncio
from decimal import Decimal

import config
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import quotation_to_decimal


TARGET = Decimal("100000")


async def main():
    config.validate()

    account_id = config.TINKOFF_ACCOUNT_ID
    if not account_id:
        print("ОШИБКА: TINKOFF_ACCOUNT_ID не задан.")
        return

    async with AsyncSandboxClient(config.TINKOFF_API_TOKEN) as client:
        response = await client.sandbox.get_sandbox_positions(
            account_id=account_id
        )

        print("\n=== SANDBOX: ДЕНЕЖНЫЕ ОСТАТКИ ===")
        print(f"Счёт: {account_id}")

        total_rub = Decimal("0")
        blocked_rub = Decimal("0")

        for item in response.money:
            amount = Decimal(str(quotation_to_decimal(item)))
            currency = getattr(item, "currency", "неизвестная валюта")

            print(f"{currency}: {amount:,.2f}")

            if currency.lower() == "rub":
                total_rub += amount

        for item in response.blocked:
            amount = Decimal(str(quotation_to_decimal(item)))
            currency = getattr(item, "currency", "неизвестная валюта")

            if currency.lower() == "rub":
                blocked_rub += amount

        available = total_rub - blocked_rub
        difference = TARGET - total_rub

        print("\n=== ИТОГО ===")
        print(f"Денежный остаток: {total_rub:,.2f} ₽")
        print(f"Заблокировано:    {blocked_rub:,.2f} ₽")
        print(f"Доступно:         {available:,.2f} ₽")
        print(f"До 100 000 ₽:     {difference:,.2f} ₽")

        print("\nREAD ONLY: пополнение и торговые операции не выполнялись.")


if __name__ == "__main__":
    asyncio.run(main())
