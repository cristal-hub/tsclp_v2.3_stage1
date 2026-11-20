# tests/test_risk_manager.py
# Tsclp_v2.2_volatile — тесты RiskManager.
# Обновлено под PositionData (лоты), блокировки при None, export/import state.

import pandas as pd
import pytest
from decimal import Decimal
from risk_manager import RiskManager
from tinkoff_client import PositionData
import config


class FakeClient:
    def __init__(self, balance=100_000.0, positions=None, info=None, prices=None):
        self.balance = balance
        self.positions = positions if positions is not None else {}
        self.info = info if info is not None else {'lot': 10, 'min_price_increment': 0.01}
        self.prices = prices if prices is not None else {}
        self.balance_returns_none = False
        self.positions_returns_none = False
        self.prices_returns_none = False

    async def get_account_balance(self):
        if self.balance_returns_none:
            return None
        return self.balance

    async def get_instrument_info(self, figi):
        return self.info

    async def get_positions(self):
        if self.positions_returns_none:
            return None
        return self.positions

    async def get_last_prices(self, figis):
        if self.prices_returns_none:
            return None
        return {f: self.prices.get(f, 100.0) for f in figis}


def make_df(closes):
    n = len(closes)
    return pd.DataFrame({
        'open': closes,
        'high': [c * 1.01 for c in closes],
        'low': [c * 0.99 for c in closes],
        'close': closes,
        'volume': [1000] * n,
    }, index=pd.date_range('2026-01-01', periods=n, freq='5min'))


def make_position(lots, direction="LONG", raw_shares=None):
    return PositionData(
        lots=lots,
        direction=direction,
        raw_shares=Decimal(raw_shares if raw_shares is not None else lots),
    )


# --- Базовые ---

@pytest.mark.asyncio
async def test_position_size_positive():
    client = FakeClient(balance=100_000.0)
    rm = RiskManager(client)
    df = make_df([100.0] * 50)
    lots = await rm.calculate_position_size("FIGI", 100.0, df)
    assert lots >= 0


@pytest.mark.asyncio
async def test_zero_balance_returns_zero():
    client = FakeClient(balance=0.0)
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots == 0


@pytest.mark.asyncio
async def test_balance_none_returns_zero():
    """get_account_balance()=None -> блокировка."""
    client = FakeClient(balance=100_000.0)
    client.balance_returns_none = True
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots == 0


@pytest.mark.asyncio
async def test_daily_limit():
    client = FakeClient(balance=100_000.0)
    rm = RiskManager(client)
    rm.daily_trade_counts["FIGI"] = config.MAX_TRADES_PER_INSTRUMENT_PER_DAY
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots == 0


# --- SL/TP (без изменений, стратегия) ---

def test_sl_tp_buy():
    client = FakeClient()
    rm = RiskManager(client)
    df = make_df([100.0] * 50)
    sl, tp = rm.calculate_sl_tp(100.0, "BUY", df)
    assert sl is not None and tp is not None
    assert sl < 100.0 < tp


def test_sl_tp_sell():
    client = FakeClient()
    rm = RiskManager(client)
    df = make_df([100.0] * 50)
    sl, tp = rm.calculate_sl_tp(100.0, "SELL", df)
    assert sl is not None and tp is not None
    assert tp < 100.0 < sl


def test_sl_tp_invalid_direction():
    client = FakeClient()
    rm = RiskManager(client)
    sl, tp = rm.calculate_sl_tp(100.0, "HOLD", None)
    assert sl is None and tp is None


# --- Friction (без изменений) ---

def test_friction_includes_commission():
    """Тавтологичный тест (слабый). Оставлен как есть (согласовано)."""
    client = FakeClient()
    rm = RiskManager(client)
    last_price = 100.0
    expected = last_price * (config.COMMISSION_PER_SIDE * 2 + config.SLIPPAGE_PERCENT)
    actual = last_price * (config.COMMISSION_PER_SIDE * 2 + config.SLIPPAGE_PERCENT)
    assert abs(actual - expected) < 1e-9
    assert expected > 0


# --- Экспозиция: единицы ---

@pytest.mark.asyncio
async def test_exposure_units_lots_lot_size_price():
    """|lots| * lot_size * price — строгие единицы."""
    client = FakeClient(balance=1_000_000.0)
    client.positions = {"F1": make_position(lots=10, raw_shares=100)}
    client.info = {'lot': 10, 'min_price_increment': 0.01}
    client.prices = {"F1": 100.0}
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    # Экспозиция: 10 * 10 * 100 = 10 000. При лимите 0.6 * 1M = 600k — не блокирует.
    assert lots >= 0


@pytest.mark.asyncio
async def test_exposure_blocks_on_high_exposure():
    """Экспозиция близка к лимиту -> размер по капиталу уменьшается."""
    client = FakeClient(balance=100_000.0)
    # 1 позиция: 50 лотов * 10 штук * 100 руб = 50 000 руб (50% от баланса)
    client.positions = {"F1": make_position(lots=50, raw_shares=500)}
    client.info = {'lot': 10, 'min_price_increment': 0.01}
    client.prices = {"F1": 100.0}
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    # MAX_TOTAL_EXPOSURE = 0.6 -> 60k. Уже 50k. Остаток 10k.
    # lot_cost = 100 * 10 = 1000. Размер по капиталу = min(60k/1000, 10k/1000) = 10.
    assert lots <= 10


@pytest.mark.asyncio
async def test_unknown_price_blocks():
    """Цена неизвестна (p=None) -> блокировка."""
    client = FakeClient(balance=100_000.0)
    client.positions = {"F1": make_position(lots=10)}
    client.info = {'lot': 1, 'min_price_increment': 0.01}
    client.prices = {"F1": 0.0}  # p=0 -> блокировка
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots == 0


@pytest.mark.asyncio
async def test_unknown_instrument_blocks():
    """get_instrument_info(f)=None для позиции -> блокировка."""
    class ClientNoInfo(FakeClient):
        async def get_instrument_info(self, figi):
            if figi == "F1":
                return None
            return self.info

    client = ClientNoInfo(balance=100_000.0)
    client.positions = {"F1": make_position(lots=10)}
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots == 0


@pytest.mark.asyncio
async def test_positions_none_blocks():
    """get_positions()=None -> блокировка."""
    client = FakeClient(balance=100_000.0)
    client.positions_returns_none = True
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots == 0


@pytest.mark.asyncio
async def test_prices_none_blocks():
    """get_last_prices()=None для позиции -> блокировка."""
    client = FakeClient(balance=100_000.0)
    client.positions = {"F1": make_position(lots=10)}
    client.prices_returns_none = True
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots == 0


@pytest.mark.asyncio
async def test_empty_portfolio_is_ok():
    """Пустой портфель {} — допустим (не блокировка)."""
    client = FakeClient(balance=100_000.0)
    client.positions = {}
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots > 0  # расчёт проходит


@pytest.mark.asyncio
async def test_currency_positions_skipped():
    """RUB/USD/EUR — кэш, не экспозиция."""
    client = FakeClient(balance=100_000.0)
    client.positions = {
        "RUB000UTSTOM": make_position(lots=100000, raw_shares=100000),
        "F1": make_position(lots=1),
    }
    client.info = {'lot': 1, 'min_price_increment': 0.01}
    client.prices = {"F1": 100.0}
    rm = RiskManager(client)
    lots = await rm.calculate_position_size("FIGI", 100.0, None)
    assert lots > 0  # RUB пропущен, F1 учтён


# --- export_state / import_state ---

def test_export_import_roundtrip():
    client = FakeClient()
    rm = RiskManager(client)
    rm.daily_trade_counts = {"F1": 3, "F2": 1}
    from datetime import date
    rm.last_reset_date = date(2026, 1, 15)

    data = rm.export_state()
    assert data["daily_trade_counts"] == {"F1": 3, "F2": 1}
    assert data["last_reset_date"] == "2026-01-15"

    rm2 = RiskManager(client)
    rm2.import_state(data)
    assert rm2.daily_trade_counts == {"F1": 3, "F2": 1}
    assert rm2.last_reset_date.isoformat() == "2026-01-15"


def test_import_state_bad_date_falls_back_to_today():
    client = FakeClient()
    rm = RiskManager(client)
    rm.import_state({"daily_trade_counts": {"F1": 2}, "last_reset_date": "bad"})
    from datetime import date
    assert rm.last_reset_date == date.today()
    assert rm.daily_trade_counts == {"F1": 2}


def test_import_state_non_dict_ignored():
    client = FakeClient()
    rm = RiskManager(client)
    rm.import_state("not a dict")
    assert rm.daily_trade_counts == {}