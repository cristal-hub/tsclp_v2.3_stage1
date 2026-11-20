# diag_max_lots.py
# Tsclp_v2.2_volatile — ЭТАП ИНТЕГРАЦИОННЫХ ПРОВЕРОК: read-only диагностика GetSandboxMaxLots.
#
# ТОЛЬКО ЧТЕНИЕ. НЕ ОТПРАВЛЯЕТ ЗАЯВКИ. НЕ МЕНЯЕТ ПОРТФЕЛЬ.
# ТОЛЬКО SANDBOX. НЕ ИМПОРТИРУЕТ main.py.
#
# НАЗНАЧЕНИЕ: вывести ФАКТИЧЕСКИЙ ответ GetSandboxMaxLots для всех FIGI
# в Sandbox-портфеле. Без критериев сравнения (семантика sell_max_lots /
# buy_max_lots не подтверждена). buy_max_lots для SHORT не используется.
#
# Запуск: .\.venv\Scripts\python.exe diag_max_lots.py

import asyncio
import logging

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import quotation_to_decimal

import config
from tinkoff_client import TinkoffClient

logging.basicConfig(level=logging.WARNING)


def _mask_figi(figi: str) -> str:
    return figi[:8] + "..." if len(figi) > 8 else figi


async def main():
    print("=" * 70)
    print("ДИАГНОСТИКА GetSandboxMaxLots (READ-ONLY)")
    print("ТОЛЬКО ЧТЕНИЕ. ТОЛЬКО SANDBOX. БЕЗ ЗАЯВОК.")
    print("=" * 70)

    config.validate()

    if not config.USE_SANDBOX:
        print("❌ USE_SANDBOX=False. Скрипт работает только в песочнице.")
        return 1

    async with TinkoffClient(config.TINKOFF_API_TOKEN, use_sandbox=True) as client:
        # 1. Портфель (PositionData, лоты)
        positions = await client.get_positions()
        if positions is None:
            print("❌ get_positions()=None (ошибка API) — остановка.")
            return 1

        # Убираем валютные позиции (кэш)
        broker = {f: p for f, p in positions.items()
                  if not f.startswith(("RUB", "USD", "EUR"))}

        if not broker:
            print("\nℹ️ Нет невалютных позиций в портфеле.")
            print("   GetSandboxMaxLots не для чего вызывать.")
            return 0

        print(f"\nПозиций (невалютных): {len(broker)}\n")

        # 2. Для каждой позиции — фактический ответ GetSandboxMaxLots
        for figi, pd in broker.items():
            print("=" * 70)
            print(f"FIGI: {_mask_figi(figi)}")
            print(f"  Direction: {pd.direction}")
            print(f"  Lots (PositionData): {pd.lots}")
            print(f"  Raw shares: {pd.raw_shares}")

            # Информация об инструменте (read-only)
            info = await client.get_instrument_info(figi)
            if info:
                print(f"  ticker: {info.get('ticker', '?')}")
                print(f"  lot_size: {info.get('lot', '?')}")
            else:
                print(f"  instrument_info: None")

            # GetSandboxMaxLots (read-only)
            max_lots = await client.get_sandbox_max_lots(figi)
            if max_lots is None:
                print(f"  GetSandboxMaxLots: None (ошибка/неполный ответ)")
                print()
                continue

            sell_max = max_lots.get("sell_max_lots")
            buy_max = max_lots.get("buy_max_lots")

            print(f"  sell_max_lots: {sell_max!r} (python: {type(sell_max).__name__})")
            print(f"  buy_max_lots:  {buy_max!r} (python: {type(buy_max).__name__})")

            # ТОЛЬКО ВЫВОД, БЕЗ КРИТЕРИЯ СРАВНЕНИЯ
            # (семантика не подтверждена — не устанавливаем sell_max >= lots или <= lots)
            print(f"  [Сравнение с pos.lots: НЕ выполняется — семантика не подтверждена]")

            if pd.direction == "SHORT":
                print(f"  [SHORT: buy_max_lots НЕ используется для закрытия — семантика не подтверждена]")

            print()

        # 3. Итог
        print("=" * 70)
        print("ВЫВОД")
        print("=" * 70)
        print("Выведен фактический ответ GetSandboxMaxLots для всех позиций.")
        print("Критерии сравнения (sell_max_lots vs pos.lots) НЕ установлены.")
        print("buy_max_lots для SHORT НЕ используется.")
        print("Для интерпретации семантики — требуется официальное подтверждение.")
        return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))