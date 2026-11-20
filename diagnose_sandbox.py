# diagnose_sandbox.py
# Tsclp_v2.2_volatile — РУЧНАЯ диагностика песочницы (READ-ONLY).
#
# ВНИМАНИЕ: скрипт подключается к песочнице T-Invest API.
# Он НЕ отправляет торговых заявок, но выполняет read-only запросы:
#  - портфель (positions: quantity и quantityLots)
#  - свечи (is_complete последней свечи) — если передан --tickers
#  - стоп-заявки (если есть)
#
# Запускать ТОЛЬКО вручную, после локальных тестов.
# По умолчанию отключён: требует --yes.
#
# ИЗМЕНЕНИЯ (этап 13):
#   - --tickers: список тикеров через запятую (SBER,GAZP).
#     Если не передан — пустой список (только портфель и стоп-заявки).
#   - --class-code: код класса (по умолчанию TQBR), применяется ко всем.
#   - Неизвестный тикер: сообщить, продолжить; в конце — «НЕПОЛНАЯ ПРОВЕРКА».
#   - Неверный формат --tickers: parser.error (не частичная диагностика).
#   - Не импортирует main.py, не вызывает screener.
#   - READ-ONLY. Заявок не отправляет.
#
# Запуск:
#   .\.venv\Scripts\python.exe diagnose_sandbox.py --yes
#   .\.venv\Scripts\python.exe diagnose_sandbox.py --yes --tickers SBER,GAZP
#   .\.venv\Scripts\python.exe diagnose_sandbox.py --yes --tickers SBER --class-code TQBR

import argparse
import asyncio
import logging
from datetime import timedelta

from t_tech.invest import CandleInterval, InstrumentIdType
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import now, quotation_to_decimal

import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _parse_tickers(value: str) -> list:
    """
    Парсит строку тикеров через запятую.
    Пустые элементы, пробелы внутри — ошибка.
    """
    if not value:
        raise argparse.ArgumentTypeError("Пустой список тикеров")
    parts = value.split(",")
    result = []
    for p in parts:
        t = p.strip()
        if not t:
            raise argparse.ArgumentTypeError(f"Пустой тикер в списке: {value!r}")
        if " " in t:
            raise argparse.ArgumentTypeError(f"Тикер содержит пробел: {t!r}")
        result.append(t)
    if not result:
        raise argparse.ArgumentTypeError("Пустой список тикеров")
    return result


async def main():
    parser = argparse.ArgumentParser(
        description="Read-only диагностика песочницы T-Invest.",
    )
    parser.add_argument("--yes", action="store_true", help="Подтвердить подключение")
    parser.add_argument(
        "--tickers", type=_parse_tickers, default=[],
        help="Список тикеров через запятую (например, SBER,GAZP). "
             "Если не передан — свечи не загружаются.",
    )
    parser.add_argument(
        "--class-code", type=str, default="TQBR",
        help="Код класса инструмента (по умолчанию TQBR). Применяется ко всем тикерам.",
    )
    args = parser.parse_args()

    if not args.yes:
        print("Диагностика отключена. Запустите: python diagnose_sandbox.py --yes")
        print("Скрипт выполняет ТОЛЬКО чтение, но подключается к песочнице.")
        return 0

    config.validate()

    # Счётчик ошибок поиска тикеров
    tickers_not_found = 0
    tickers_found = 0
    tickers_checked = 0

    async with AsyncSandboxClient(config.TINKOFF_API_TOKEN) as client:
        account_id = config.TINKOFF_ACCOUNT_ID
        if not account_id:
            accounts = await client.users.get_accounts()
            if not accounts.accounts:
                print("Нет песочных счетов. Создайте через основной робот.")
                return 1
            account_id = accounts.accounts[0].id
            print(f"Используется первый счёт: {account_id[:12]}...")

        print("=" * 60)
        print("1. ПОРТФЕЛЬ (единицы quantity vs quantityLots)")
        print("=" * 60)
        try:
            portfolio = await client.sandbox.get_sandbox_portfolio(account_id=account_id)
            if not portfolio.positions:
                print("  (портфель пуст)")
            for pos in portfolio.positions:
                qty = float(quotation_to_decimal(pos.quantity))
                lots_attr = getattr(pos, "quantity_lots", None)
                lots = float(quotation_to_decimal(lots_attr)) if lots_attr is not None else None
                print(f"  figi={pos.figi[:12]}... quantity={qty} quantityLots={lots}")
        except Exception as e:
            print(f"  ❌ Ошибка портфеля: {type(e).__name__}: {e}")

        # Свечи — только если --tickers передан
        if args.tickers:
            print()
            print("=" * 60)
            print(f"2. СВЕЧИ (is_complete последней свечи), class_code={args.class_code}")
            print("=" * 60)
            for ticker in args.tickers:
                tickers_checked += 1
                try:
                    resp = await client.instruments.get_instrument_by(
                        id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                        id=ticker,
                        class_code=args.class_code,
                    )
                except Exception as e:
                    tickers_not_found += 1
                    print(f"  {ticker}: НЕ НАЙДЕН (class_code={args.class_code}) — {type(e).__name__}: {e}")
                    print(f"    Проверьте тикер и код класса (--class-code).")
                    continue
                tickers_found += 1
                figi = resp.instrument.figi
                candles = []
                try:
                    async for c in client.get_all_candles(
                        figi=figi,
                        from_=now() - timedelta(days=1),
                        interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
                    ):
                        candles.append(c)
                except Exception as e:
                    print(f"  {ticker}: ошибка свечей — {type(e).__name__}: {e}")
                    continue
                if candles:
                    last = candles[-1]
                    is_complete = getattr(last, "is_complete", None)
                    print(f"  {ticker}: last candle time={last.time} is_complete={is_complete}")
                else:
                    print(f"  {ticker}: нет свечей")
        else:
            print()
            print("=" * 60)
            print("2. СВЕЧИ — не загружаются (--tickers не передан)")
            print("=" * 60)

        print()
        print("=" * 60)
        print("3. СТОП-ЗАЯВКИ")
        print("=" * 60)
        try:
            stops = await client.sandbox.get_sandbox_stop_orders(account_id=account_id)
            stop_orders = getattr(stops, "stop_orders", []) or []
            if not stop_orders:
                print("  (нет стоп-заявок)")
            else:
                for o in stop_orders:
                    print(
                        f"  id={o.stop_order_id} figi={o.figi[:12]}... "
                        f"type={o.stop_order_type} status={getattr(o, 'status', None)}"
                    )
        except Exception as e:
            print(f"  ❌ Ошибка стоп-заявок: {type(e).__name__}: {e}")

        # Итоговый статус
        print()
        print("=" * 60)
        if args.tickers:
            if tickers_not_found > 0:
                print(
                    f"⚠️ НЕПОЛНАЯ ПРОВЕРКА: тикеров проверено {tickers_checked}, "
                    f"найдено {tickers_found}, НЕ НАЙДЕНО {tickers_not_found}."
                )
                print("   Диагностика свечей неполная. См. ошибки выше.")
                return 2
            else:
                print(f"✅ ПОЛНАЯ ПРОВЕРКА: тикеров {tickers_checked}, все найдены.")
                return 0
        else:
            print("ℹ️ Свечи не проверялись (--tickers не передан).")
            return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))