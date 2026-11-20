# tests/conftest.py
# Изоляция тестовой среды SDK: если реальный SDK t_tech.invest /
# pandas_ta_classic / python-telegram-bot недоступны (например, прогон в
# чистом CI-окружении), ставятся минимальные заглушки. В окружении
# репозитория (SDK установлен) заглушки НЕ используются — поведение
# тестового набора не меняется.

import sys
import types
from datetime import datetime, timezone
from decimal import Decimal


class _EnumVal:
    def __init__(self, name, value=None):
        self.name = name
        self.value = value if value is not None else name

    def __repr__(self):
        return f"<{self.name}>"


def _quotation_to_decimal(q):
    return Decimal(q.units) + Decimal(q.nano) / Decimal(1e9)


def _decimal_to_quotation(d):
    units = int(d)
    nano = int((Decimal(str(d)) - units) * Decimal(1e9))
    return types.SimpleNamespace(units=units, nano=nano)


def _install_stubs():
    try:
        import t_tech.invest  # noqa: F401
        return  # реальный SDK доступен — ничего не подменяем
    except Exception:
        pass

    invest = types.ModuleType("t_tech.invest")
    invest.CandleInterval = types.SimpleNamespace(
        CANDLE_INTERVAL_1_MIN=_EnumVal("1min"),
        CANDLE_INTERVAL_5_MIN=_EnumVal("5min"),
        CANDLE_INTERVAL_DAY=_EnumVal("day"),
    )
    invest.OrderDirection = types.SimpleNamespace(
        ORDER_DIRECTION_BUY=_EnumVal("BUY", 1),
        ORDER_DIRECTION_SELL=_EnumVal("SELL", 2),
    )
    invest.OrderType = types.SimpleNamespace(
        ORDER_TYPE_MARKET=_EnumVal("MARKET", 1),
        ORDER_TYPE_LIMIT=_EnumVal("LIMIT", 2),
    )
    invest.InstrumentIdType = types.SimpleNamespace(
        INSTRUMENT_ID_TYPE_FIGI=_EnumVal("FIGI", 1),
        INSTRUMENT_ID_TYPE_TICKER=_EnumVal("TICKER", 2),
    )
    invest.InstrumentStatus = types.SimpleNamespace(
        INSTRUMENT_STATUS_ALL=_EnumVal("ALL", 1),
    )
    invest.OrderExecutionReportStatus = types.SimpleNamespace(
        EXECUTION_REPORT_STATUS_FILL=_EnumVal("FILL", 1),
        EXECUTION_REPORT_STATUS_REJECTED=_EnumVal("REJECTED", 2),
        EXECUTION_REPORT_STATUS_CANCELLED=_EnumVal("CANCELLED", 3),
        EXECUTION_REPORT_STATUS_NEW=_EnumVal("NEW", 4),
        EXECUTION_REPORT_STATUS_PARTIALLYFILL=_EnumVal("PARTIALLYFILL", 5),
        EXECUTION_REPORT_STATUS_EXPIRED=_EnumVal("EXPIRED", 6),
    )
    invest.GetMaxLotsRequest = _EnumVal("GetMaxLotsRequest")
    invest.AsyncClient = object

    grpc = types.ModuleType("t_tech.invest.grpc")
    grpc.GetMaxLotsRequest = invest.GetMaxLotsRequest

    sandbox_pkg = types.ModuleType("t_tech.invest.sandbox")
    sandbox_ac = types.ModuleType("t_tech.invest.sandbox.async_client")
    sandbox_ac.AsyncSandboxClient = object
    sandbox_pkg.async_client = sandbox_ac

    utils = types.ModuleType("t_tech.invest.utils")
    utils.now = lambda: datetime.now(timezone.utc)
    utils.quotation_to_decimal = _quotation_to_decimal
    utils.decimal_to_quotation = _decimal_to_quotation

    ttech = types.ModuleType("t_tech")
    ttech.invest = invest

    sys.modules.setdefault("t_tech", ttech)
    sys.modules.setdefault("t_tech.invest", invest)
    sys.modules.setdefault("t_tech.invest.grpc", grpc)
    sys.modules.setdefault("t_tech.invest.sandbox", sandbox_pkg)
    sys.modules.setdefault("t_tech.invest.sandbox.async_client", sandbox_ac)
    sys.modules.setdefault("t_tech.invest.utils", utils)

    if "pandas_ta_classic" not in sys.modules:
        try:
            import pandas_ta_classic  # noqa: F401
        except Exception:
            sys.modules["pandas_ta_classic"] = types.ModuleType("pandas_ta_classic")

    try:
        import telegram  # noqa: F401
    except Exception:
        telegram = types.ModuleType("telegram")
        telegram.Update = object
        telegram.BotCommand = object
        ext = types.ModuleType("telegram.ext")
        ext.Application = object
        ext.CommandHandler = object
        ext.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
        request = types.ModuleType("telegram.request")
        request.HTTPXRequest = object
        telegram.ext = ext
        telegram.request = request
        sys.modules.setdefault("telegram", telegram)
        sys.modules.setdefault("telegram.ext", ext)
        sys.modules.setdefault("telegram.request", request)


_install_stubs()
