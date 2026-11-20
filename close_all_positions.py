# close_all_positions.py
# Tsclp_v2.2_volatile — РУЧНОЕ аварийное закрытие позиций (ТОЛЬКО SANDBOX).
#
# НАЗНАЧЕНИЕ: закрыть все LONG-позиции после явного подтверждения YES.
# SHORT-позиции: обнаруживаются, но BUY НЕ отправляется (семантика
#   buyMaxLots для закрытия шорта не подтверждена).
#
# БЕЗОПАСНОСТЬ:
#   - Lock-файл (O_CREAT | O_EXCL), чужой lock не удаляется.
#   - Активные заявки: отменяются только относящиеся к нашим позициям.
#     Чужие/неоднозначные -> остановка.
#   - GetSandboxMaxLots(figi) -> sellMaxLots. None -> остановка.
#   - OrderResult.state is None -> остановка, не повторять.
#   - Частичное исполнение -> пересчёт остатка по портфелю, не повторять.
#   - Финальная сверка: если позиции остались -> сообщить, не успех.
#   - YES — обязательно.
#
# НЕ ИМПОРТИРУЕТ main.py. НЕ ЗАПУСКАЕТСЯ автоматически.
# ТОЛЬКО SANDBOX. РЕАЛЬНЫЕ ОРДЕРА НЕ ОТПРАВЛЯЮТСЯ.
#
# Запуск: .\.venv\Scripts\python.exe close_all_positions.py

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

from t_tech.invest import OrderDirection

import config
from tinkoff_client import TinkoffClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

LOCK_FILE = "close_all_positions.lock"
CURRENCY_PREFIXES = ("RUB", "USD", "EUR")


def _acquire_lock() -> bool:
    """
    Эксклюзивное создание lock-файла (O_CREAT | O_EXCL).
    Возвращает True при успехе, False если файл уже существует.
    Чужой lock НЕ удаляется.
    """
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(datetime.now(timezone.utc).isoformat())
        return True
    except FileExistsError:
        return False
    except Exception as e:
        logging.error(f"[lock] Ошибка создания: {type(e).__name__}: {e}")
        return False


def _release_lock() -> None:
    """Удаляет lock-файл, созданный этим процессом."""
    try:
        if os.path.isfile(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception as e:
        logging.warning(f"[lock] Ошибка удаления: {type(e).__name__}: {e}")


def _mask_figi(figi: str) -> str:
    return figi[:8] + "..." if len(figi) > 8 else figi


async def _cancel_active_orders(client: TinkoffClient, our_figis: set) -> bool:
    """
    Отменяет активные заявки, относящиеся к our_figis.
    Если есть активная заявка по чужому FIGI или неоднозначная —
    возвращает False (остановка).
    """
    try:
        resp = await client.client.sandbox.get_sandbox_orders(account_id=client.account_id)
    except Exception as e:
        logging.error(f"[orders] Ошибка получения активных заявок: {type(e).__name__}: {e}")
        return False

    orders = getattr(resp, "orders", []) or []
    if not orders:
        logging.info("[orders] Активных заявок нет.")
        return True

    to_cancel = []
    for o in orders:
        oid = getattr(o, "order_id", None)
        figi = getattr(o, "figi", None)
        if not oid or not figi:
            logging.error(f"[orders] Неоднозначная заявка (order_id={oid}, figi={figi}) — остановка")
            return False
        if figi not in our_figis:
            logging.error(
                f"[orders] Активная заявка по чужому FIGI {_mask_figi(figi)} "
                f"(order_id={oid}) — остановка, не отменяем."
            )
            return False
        to_cancel.append((oid, figi))

    logging.info(f"[orders] К отмене: {len(to_cancel)} заявок (наши FIGI).")

    for oid, figi in to_cancel:
        logging.info(f"[orders] Отмена {oid} ({_mask_figi(figi)})...")
        cancel_resp = await client.cancel_order(oid)
        if cancel_resp is None:
            logging.error(f"[orders] cancel_order({oid}) = None — остановка")
            return False
        # Проверить финальный статус
        state = await client.get_sandbox_order_state(oid)
        if state is None:
            logging.error(f"[orders] get_sandbox_order_state({oid}) = None — остановка")
            return False
        status = getattr(state, "execution_report_status", None)
        lots_exec = int(getattr(state, "lots_executed", 0) or 0)
        lots_left = int(getattr(state, "lots_left", 0) or 0)
        logging.info(
            f"[orders] {oid}: status={status}, lots_executed={lots_exec}, lots_left={lots_left}"
        )
        # CANCELLED не гарантирует отсутствие исполнения — проверяем lots_executed
        if lots_exec > 0:
            logging.warning(
                f"[orders] {oid}: частичное исполнение {lots_exec} лот. — "
                f"позиция могла измениться, продолжаем со свежим портфелем"
            )
    return True


async def main() -> int:
    print("=" * 70)
    print("АВАРИЙНОЕ ЗАКРЫТИЕ ПОЗИЦИЙ (ТОЛЬКО SANDBOX)")
    print("Только LONG. SHORT — обнаруживается, BUY не отправляется.")
    print("=" * 70)

    if not _acquire_lock():
        print(f"❌ Lock-файл {LOCK_FILE} уже существует. Другой процесс запущен?")
        print("   Чужой lock не удаляется. Проверьте вручную.")
        return 1

    try:
        config.validate()

        if not config.USE_SANDBOX:
            print("❌ USE_SANDBOX=False. Только песочница.")
            return 1

        async with TinkoffClient(config.TINKOFF_API_TOKEN, use_sandbox=True) as client:
            # 1. Портфель
            positions = await client.get_positions()
            if positions is None:
                print("❌ get_positions()=None (ошибка API) — остановка.")
                return 1

            broker = {f: p for f, p in positions.items()
                      if not f.startswith(CURRENCY_PREFIXES)}

            if not broker:
                print("✅ Позиций нет (достоверно).")
                return 0

            long_figis = {f for f, p in broker.items() if p.direction == "LONG"}
            short_figis = {f for f, p in broker.items() if p.direction == "SHORT"}
            unknown_figis = {f for f, p in broker.items() if p.direction == "UNKNOWN"}

            print(f"\n📊 Портфель: LONG={len(long_figis)}, SHORT={len(short_figis)}, "
                  f"UNKNOWN={len(unknown_figis)}")

            if unknown_figis:
                print(f"❌ Неизвестное направление: {', '.join(_mask_figi(f) for f in unknown_figis)}")
                print("   Остановка. Не угадываем.")
                return 1

            # 2. Активные заявки (только наши FIGI)
            our_figis = set(broker.keys())
            ok = await _cancel_active_orders(client, our_figis)
            if not ok:
                print("❌ Не удалось безопасно обработать активные заявки — остановка.")
                return 1

            # 3. Свежий портфель после отмен
            positions = await client.get_positions()
            if positions is None:
                print("❌ get_positions()=None после отмен — остановка.")
                return 1
            broker = {f: p for f, p in positions.items()
                      if not f.startswith(CURRENCY_PREFIXES)}

            long_figis = {f for f, p in broker.items() if p.direction == "LONG"}
            short_figis = {f for f, p in broker.items() if p.direction == "SHORT"}

            if not long_figis and not short_figis:
                print("✅ После отмен позиций нет.")
                return 0

            # 4. Доступное количество для LONG (sellMaxLots)
            plans = []  # (figi, lots, sell_max, ticker)
            for figi in sorted(long_figis):
                p = broker[figi]
                info = await client.get_instrument_info(figi)
                if not info:
                    print(f"❌ {_mask_figi(figi)}: instrument_info=None — остановка.")
                    return 1

                max_lots = await client.get_sandbox_max_lots(figi)
                if max_lots is None:
                    print(f"❌ {_mask_figi(figi)}: get_sandbox_max_lots=None — остановка.")
                    return 1
                sell_max = max_lots.get("sell_max_lots")
                if sell_max is None:
                    print(f"❌ {_mask_figi(figi)}: sell_max_lots=None — остановка.")
                    return 1
                if sell_max <= 0:
                    print(f"⚠️ {_mask_figi(figi)}: sell_max_lots={sell_max} — пропуск.")
                    continue

                lots_to_close = min(p.lots, sell_max)
                if lots_to_close < p.lots:
                    print(
                        f"⚠️ {_mask_figi(figi)}: доступно {sell_max} из {p.lots} лот. "
                        f"(заблокировано {p.lots - sell_max}) — будет закрыто {lots_to_close}"
                    )
                plans.append((figi, lots_to_close, p.lots, info.get("ticker", "?")))

            # 5. SHORT — только сообщение
            if short_figis:
                print(f"\n⚠️ SHORT-позиции ({len(short_figis)}) — BUY НЕ отправляется:")
                for figi in sorted(short_figis):
                    p = broker[figi]
                    print(f"   • {_mask_figi(figi)}: {p.lots} лот. (SHORT) — пропуск")

            if not plans:
                print("\n❌ Нет LONG-позиций для безопасного закрытия — остановка.")
                return 1 if short_figis else 0

            # 6. Подтверждение
            print(f"\n🔍 План закрытия LONG ({len(plans)}):")
            for figi, lots_to_close, total_lots, ticker in plans:
                print(f"   • {ticker} ({_mask_figi(figi)}): SELL {lots_to_close} "
                      f"из {total_lots} лот.")

            if short_figis:
                print(f"\n⚠️ SHORT останутся открытыми ({len(short_figis)}).")

            answer = input("\nВведите YES для подтверждения: ").strip()
            if answer != "YES":
                print("Отменено.")
                return 0

            # 7. Отправка SELL
            print("\n📤 Отправка заявок...")
            sent = []
            for figi, lots_to_close, total_lots, ticker in plans:
                result = await client.post_market_order(
                    figi, lots_to_close, OrderDirection.ORDER_DIRECTION_SELL
                )
                if result.state is None:
                    print(
                        f"❌ {ticker}: state=None (order_id={result.order_id}) — "
                        f"неизвестный результат, НЕ повторяем. Остановка."
                    )
                    print("   Сохраните order_id для ручной проверки.")
                    return 1

                status = getattr(result.state, "execution_report_status", None)
                lots_exec = int(getattr(result.state, "lots_executed", 0) or 0)
                print(f"✅ {ticker}: order_id={result.order_id}, status={status}, "
                      f"lots_executed={lots_exec}")
                sent.append((figi, ticker, result.order_id, lots_to_close))

                # Частичное исполнение — не повторяем автоматически
                if lots_exec < lots_to_close:
                    print(
                        f"⚠️ {ticker}: частичное исполнение {lots_exec}/{lots_to_close} — "
                        f"не повторяем, остаток проверим в финальной сверке."
                    )

            # 8. Финальная сверка
            print("\n🔍 Финальная сверка...")
            final_positions = await client.get_positions()
            if final_positions is None:
                print("❌ Финальный get_positions()=None — состояние неизвестно.")
                print("   Не заявляем об успехе. Проверьте портфель вручную.")
                return 1

            final_broker = {f: p for f, p in final_positions.items()
                            if not f.startswith(CURRENCY_PREFIXES)}

            remaining_long = {f: p for f, p in final_broker.items() if p.direction == "LONG"}
            remaining_short = {f: p for f, p in final_broker.items() if p.direction == "SHORT"}
            remaining_unknown = {f: p for f, p in final_broker.items() if p.direction == "UNKNOWN"}

            print("\n" + "=" * 70)
            print("ИТОГ:")
            print("=" * 70)

            if not remaining_long and not remaining_short and not remaining_unknown:
                print("✅ Все позиции закрыты (подтверждено портфелем).")
                return 0

            if remaining_long:
                print(f"❌ Остались LONG ({len(remaining_long)}):")
                for f, p in remaining_long.items():
                    print(f"   • {_mask_figi(f)}: {p.lots} лот.")
            if remaining_short:
                print(f"⚠️ SHORT не закрывались ({len(remaining_short)}):")
                for f, p in remaining_short.items():
                    print(f"   • {_mask_figi(f)}: {p.lots} лот. (BUY не отправлялся)")
            if remaining_unknown:
                print(f"❌ UNKNOWN ({len(remaining_unknown)}):")
                for f, p in remaining_unknown.items():
                    print(f"   • {_mask_figi(f)}: направление неизвестно")

            print("\n❌ Не все позиции закрыты. См. выше.")
            return 1

    finally:
        _release_lock()


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
        _release_lock()
        sys.exit(130)
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {type(e).__name__}: {e}")
        _release_lock()
        sys.exit(1)