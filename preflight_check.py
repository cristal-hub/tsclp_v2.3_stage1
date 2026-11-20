# preflight_check.py
# Tsclp_v2.2_volatile — ПРЕДСТАРТОВАЯ ПРОВЕРКА (READ-ONLY).
#
# ТОЛЬКО ЧТЕНИЕ. НЕ ОТПРАВЛЯЕТ ЗАЯВКИ. НЕ СОЗДАЁТ trading.flag.
# НЕ МЕНЯЕТ ПОРТФЕЛЬ. ТОЛЬКО SANDBOX.
#
# Проверяет:
#   1. Sandbox-конфигурация (USE_SANDBOX, validate).
#   2. Подключённый тип счёта.
#   3. Состояние портфеля (PositionData, валютные пропущены).
#   4. Открытые заявки.
#   5. state.json (загрузка в изолированный monitor/rm).
#   6. Reconcile (read-only, не меняет портфель).
#   7. Отсутствие trading.flag.
#
# Запуск: .\.venv\Scripts\python.exe preflight_check.py

import asyncio
import logging
import os

import config
import main
from execution import ExecutionMonitor
from risk_manager import RiskManager
from tinkoff_client import TinkoffClient

logging.basicConfig(level=logging.WARNING)


def _mask(s: str) -> str:
    return s[:6] + "..." if len(s) > 6 else "***"


async def main_preflight():
    print("=" * 70)
    print("ПРЕДСТАРТОВАЯ ПРОВЕРКА (READ-ONLY)")
    print("ТОЛЬКО ЧТЕНИЕ. ТОЛЬКО SANDBOX. БЕЗ ЗАЯВОК.")
    print("=" * 70)

    issues = []

    # 1. Sandbox-конфигурация
    print("\n[1] SANDBOX-КОНФИГУРАЦИЯ")
    try:
        config.validate()
        print(f"  ✅ config.validate() — OK")
    except Exception as e:
        print(f"  ❌ config.validate(): {type(e).__name__}: {e}")
        issues.append("config.validate() failed")
        return issues

    if config.USE_SANDBOX:
        print(f"  ✅ USE_SANDBOX = True")
    else:
        print(f"  ❌ USE_SANDBOX = False (ЗАПРЕЩЕНО)")
        issues.append("USE_SANDBOX=False")
        return issues

    print(f"  TINKOFF_ACCOUNT_ID: {'задан' if config.TINKOFF_ACCOUNT_ID else 'не задан (первый счёт)'}")

    async with TinkoffClient(config.TINKOFF_API_TOKEN, use_sandbox=True) as client:
        print(f"  ✅ Подключение к Sandbox OK")
        print(f"  account_id: {_mask(client.account_id or '?')}")

        # 2. Тип счёта
        print("\n[2] ТИП СЧЁТА")
        try:
            accounts = await client.client.users.get_accounts()
            for acc in accounts.accounts:
                acc_type = getattr(acc, "type", None)
                acc_status = getattr(acc, "status", None)
                acc_access = getattr(acc, "access_level", None)
                is_current = (acc.id == client.account_id)
                print(f"  {'→' if is_current else ' '} id={_mask(acc.id)} "
                      f"type={acc_type!r} status={acc_status!r} access={acc_access!r}")
                if is_current:
                    # Проверка, что счёт — Sandbox
                    # (в Sandbox все счета sandbox; type может быть "ACCOUNT_TYPE_TINKOFF")
                    pass
        except Exception as e:
            print(f"  ❌ Ошибка: {type(e).__name__}: {e}")
            issues.append(f"get_accounts: {type(e).__name__}")

        # 3. Портфель
        print("\n[3] ПОРТФЕЛЬ")
        positions = await client.get_positions()
        if positions is None:
            print(f"  ❌ get_positions()=None (ошибка API) — UNCERTAIN")
            issues.append("get_positions()=None")
            positions = {}
        else:
            print(f"  Позиций (торговых, без валюты): {len(positions)}")
            for figi, pd in positions.items():
                print(f"    • {_mask(figi)}: {pd.lots} лот. ({pd.direction}, "
                      f"raw_shares={pd.raw_shares})")

        if client.uncertain_state:
            print(f"  ⚠️ client.uncertain_state = True (после get_positions)")
            issues.append("uncertain_state=True после get_positions")

        # 4. Открытые заявки
        print("\n[4] ОТКРЫТЫЕ ЗАЯВКИ")
        try:
            orders_resp = await client.client.sandbox.get_sandbox_orders(
                account_id=client.account_id,
            )
            orders = getattr(orders_resp, "orders", []) or []
            if not orders:
                print(f"  ✅ Активных заявок нет")
            else:
                print(f"  ⚠️ Активных заявок: {len(orders)}")
                for o in orders:
                    print(f"    • {_mask(getattr(o, 'order_id', '?'))} "
                          f"figi={_mask(getattr(o, 'figi', '?'))} "
                          f"status={getattr(o, 'execution_report_status', '?')}")
                issues.append(f"active_orders={len(orders)}")
        except Exception as e:
            print(f"  ❌ Ошибка: {type(e).__name__}: {e}")
            issues.append(f"get_sandbox_orders: {type(e).__name__}")

        # 5. state.json
        print("\n[5] state.json")
        state_file = getattr(main, "STATE_FILE", "state.json")
        print(f"  Путь: {state_file}")
        if not os.path.isfile(state_file):
            print(f"  ⚠️ state.json отсутствует — при запуске будет блокировка входов")
            issues.append("state.json missing")
        else:
            print(f"  ✅ state.json существует")
            # Загрузка в изолированные monitor/rm (read-only)
            mon = ExecutionMonitor(client)
            rm = RiskManager(client)
            ok = main._load_state(mon, rm)
            if ok:
                print(f"  ✅ _load_state = True, позиций: {len(mon.positions)}")
                for figi, p in mon.positions.items():
                    print(f"    • {_mask(figi)}: {p.quantity} лот. ({p.state.value})")
                print(f"    daily_trade_counts: {rm.daily_trade_counts}")
            else:
                print(f"  ❌ _load_state = False (повреждён/нечитаем) — блокировка входов")
                issues.append("_load_state=False")

        # 6. Reconcile (read-only)
        print("\n[6] RECONCILE (read-only)")
        mon = ExecutionMonitor(client)
        # Загружаем state.json в этот monitor, чтобы reconcile сравнил
        if os.path.isfile(state_file):
            main._load_state(mon, RiskManager(client))
        await main.reconcile(client, mon)
        if main.reconcile_done:
            print(f"  ✅ reconcile_done = True")
        else:
            print(f"  ❌ reconcile_done = False (расхождение/ошибка) — блокировка входов")
            issues.append("reconcile_done=False")
        if client.uncertain_state:
            print(f"  ⚠️ client.uncertain_state = True после reconcile")
            issues.append("uncertain_state=True после reconcile")

        # 7. trading.flag
        print("\n[7] trading.flag")
        flag = "trading.flag"
        if os.path.exists(flag):
            print(f"  ⚠️ trading.flag СУЩЕСТВУЕТ — новые входы РАЗРЕШЕНЫ")
            issues.append("trading.flag exists")
        else:
            print(f"  ✅ trading.flag отсутствует — новые входы запрещены (безопасно)")

    # Итог
    print("\n" + "=" * 70)
    print("ИТОГ")
    print("=" * 70)
    if not issues:
        print("✅ Все проверки пройдены. Можно создавать trading.flag.")
    else:
        print(f"⚠️ Обнаружены проблемы ({len(issues)}):")
        for i in issues:
            print(f"   • {i}")
        print("\n❌ НЕ создавать trading.flag до устранения проблем.")
    return issues


if __name__ == "__main__":
    issues = asyncio.run(main_preflight())
    raise SystemExit(0 if not issues else 1)