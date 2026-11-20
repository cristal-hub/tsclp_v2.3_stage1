# tests/test_tinkoff_client.py
# Tsclp_v2.2_volatile — тесты TinkoffClient (mock, без реального API).
# Не отправляют заявок. Не требуют токенов.

import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from tinkoff_client import TinkoffClient, PositionData, OrderResult


class FakeQuotation:
    def __init__(self, units, nano=0):
        self.units = units
        self.nano = nano


class FakePosition:
    def __init__(self, figi, quantity_units, quantity_nano=0):
        self.figi = figi
        self.quantity = FakeQuotation(quantity_units, quantity_nano)


class FakePortfolio:
    def __init__(self, positions):
        self.positions = positions
        self.total_amount_portfolio = FakeQuotation(100000, 0)


class FakeLastPrice:
    def __init__(self, figi, price_units, price_nano=0):
        self.figi = figi
        self.price = FakeQuotation(price_units, price_nano)


class FakeLastPricesResponse:
    def __init__(self, last_prices):
        self.last_prices = last_prices


def make_client():
    c = TinkoffClient("fake_token", use_sandbox=True)
    c.client = MagicMock()
    c.account_id = "acc-123"
    c.instrument_info_cache = {"F1": {"lot": 1, "ticker": "T1", "name": "N1", "min_price_increment": 0.01}}
    return c


# --- get_positions ---

@pytest.mark.asyncio
async def test_get_positions_long():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([FakePosition("F1", 10)])
    )
    c.get_instrument_info = AsyncMock(return_value={"lot": 1})
    result = await c.get_positions()
    assert result is not None
    assert "F1" in result
    assert result["F1"].lots == 10
    assert result["F1"].direction == "LONG"
    assert result["F1"].raw_shares == Decimal(10)


@pytest.mark.asyncio
async def test_get_positions_short():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([FakePosition("F1", -5)])
    )
    c.get_instrument_info = AsyncMock(return_value={"lot": 1})
    result = await c.get_positions()
    assert result is not None
    assert result["F1"].lots == -5
    assert result["F1"].direction == "SHORT"


@pytest.mark.asyncio
async def test_get_positions_zero_excluded():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([FakePosition("F1", 0)])
    )
    c.get_instrument_info = AsyncMock(return_value={"lot": 1})
    result = await c.get_positions()
    assert result == {}


@pytest.mark.asyncio
async def test_get_positions_lot_size_gt_1():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([FakePosition("F1", 100)])  # 100 штук
    )
    c.get_instrument_info = AsyncMock(return_value={"lot": 10})  # лот 10
    result = await c.get_positions()
    assert result is not None
    assert result["F1"].lots == 10  # 100 / 10
    assert result["F1"].direction == "LONG"
    assert result["F1"].raw_shares == Decimal(100)


@pytest.mark.asyncio
async def test_get_positions_unresolved_remainder():
    """Обновлён (пояснение в шапке скрипта/диффа): по согласованному
    fail-closed поведению частичный/недостоверный снимок возвращается
    как None, а не частичный dict."""
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([FakePosition("F1", 15)])  # 15 штук
    )
    c.get_instrument_info = AsyncMock(return_value={"lot": 10})  # лот 10
    result = await c.get_positions()
    assert result is None  # снимок недостоверный: None, а не частичный dict
    assert c.uncertain_state is True


@pytest.mark.asyncio
async def test_get_positions_api_error_returns_none():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(side_effect=RuntimeError("api down"))
    result = await c.get_positions()
    assert result is None
    assert c.uncertain_state is True


@pytest.mark.asyncio
async def test_get_positions_empty_portfolio():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(return_value=FakePortfolio([]))
    result = await c.get_positions()
    assert result == {}
    assert c.uncertain_state is False


# --- get_last_prices ---

@pytest.mark.asyncio
async def test_get_last_prices_ok():
    c = make_client()
    c.client.market_data.get_last_prices = AsyncMock(
        return_value=FakeLastPricesResponse([FakeLastPrice("F1", 100, 500000000)])
    )
    result = await c.get_last_prices(["F1"])
    assert result == {"F1": 100.5}


@pytest.mark.asyncio
async def test_get_last_prices_error_returns_none():
    c = make_client()
    c.client.market_data.get_last_prices = AsyncMock(side_effect=RuntimeError("api down"))
    result = await c.get_last_prices(["F1"])
    assert result is None


@pytest.mark.asyncio
async def test_get_last_prices_empty_returns_empty_dict():
    c = make_client()
    c.client.market_data.get_last_prices = AsyncMock(
        return_value=FakeLastPricesResponse([])
    )
    result = await c.get_last_prices(["F1"])
    assert result == {}


# --- get_account_balance ---

@pytest.mark.asyncio
async def test_get_account_balance_ok():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([])
    )
    result = await c.get_account_balance()
    assert result == 100000.0


@pytest.mark.asyncio
async def test_get_account_balance_error_returns_none():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(side_effect=RuntimeError("api down"))
    result = await c.get_account_balance()
    assert result is None


# --- post_market_order ---

@pytest.mark.asyncio
async def test_post_market_order_success():
    c = make_client()
    fake_response = MagicMock()
    fake_response.order_id = "srv-order-1"
    c.client.sandbox.post_sandbox_order = AsyncMock(return_value=fake_response)
    result = await c.post_market_order("F1", 10, direction=1)
    assert isinstance(result, OrderResult)
    assert result.order_id  # UUID сгенерирован
    assert result.state is fake_response
    assert result.error is None


@pytest.mark.asyncio
async def test_post_market_order_error_preserves_order_id():
    c = make_client()
    c.client.sandbox.post_sandbox_order = AsyncMock(side_effect=RuntimeError("network"))
    result = await c.post_market_order("F1", 10, direction=1)
    assert isinstance(result, OrderResult)
    assert result.order_id  # UUID сохранён
    assert result.state is None
    assert isinstance(result.error, RuntimeError)
    assert c.uncertain_state is True


# --- cancel_order ---

@pytest.mark.asyncio
async def test_cancel_order_success():
    c = make_client()
    fake_resp = MagicMock()
    c.client.sandbox.cancel_sandbox_order = AsyncMock(return_value=fake_resp)
    result = await c.cancel_order("ord-1")
    assert result is fake_resp


@pytest.mark.asyncio
async def test_cancel_order_error_returns_none():
    c = make_client()
    c.client.sandbox.cancel_sandbox_order = AsyncMock(side_effect=RuntimeError("api down"))
    result = await c.cancel_order("ord-1")
    assert result is None


@pytest.mark.asyncio
async def test_cancel_then_check_status():
    """После отмены вызывающий код должен запросить статус."""
    c = make_client()
    c.client.sandbox.cancel_sandbox_order = AsyncMock(return_value=MagicMock())
    c.client.sandbox.get_sandbox_order_state = AsyncMock(return_value=MagicMock())
    await c.cancel_order("ord-1")
    state = await c.get_sandbox_order_state("ord-1")
    assert state is not None

# Дополнение к tests/test_tinkoff_client.py

@pytest.mark.asyncio
async def test_get_sandbox_max_lots_ok():
    """Возвращает sell_max_lots и buy_max_lots (лоты)."""
    c = make_client()
    fake_resp = MagicMock()
    fake_resp.sell_limits.sell_max_lots = 100
    fake_resp.buy_limits.buy_max_lots = 50
    c.client.sandbox.get_sandbox_max_lots = AsyncMock(return_value=fake_resp)
    result = await c.get_sandbox_max_lots("F1")
    assert result == {"sell_max_lots": 100, "buy_max_lots": 50}


@pytest.mark.asyncio
async def test_get_sandbox_max_lots_sell_none():
    """sell_max_lots=None -> None в dict (не подставляется)."""
    c = make_client()
    fake_resp = MagicMock()
    fake_resp.sell_limits.sell_max_lots = None
    fake_resp.buy_limits.buy_max_lots = 50
    c.client.sandbox.get_sandbox_max_lots = AsyncMock(return_value=fake_resp)
    result = await c.get_sandbox_max_lots("F1")
    assert result == {"sell_max_lots": None, "buy_max_lots": 50}


@pytest.mark.asyncio
async def test_get_sandbox_max_lots_both_none_returns_none():
    """Оба лимита None -> None (неполный ответ)."""
    c = make_client()
    fake_resp = MagicMock()
    fake_resp.sell_limits.sell_max_lots = None
    fake_resp.buy_limits.buy_max_lots = None
    c.client.sandbox.get_sandbox_max_lots = AsyncMock(return_value=fake_resp)
    result = await c.get_sandbox_max_lots("F1")
    assert result is None


@pytest.mark.asyncio
async def test_get_sandbox_max_lots_error_returns_none():
    """Ошибка API -> None."""
    c = make_client()
    c.client.sandbox.get_sandbox_max_lots = AsyncMock(side_effect=RuntimeError("api down"))
    result = await c.get_sandbox_max_lots("F1")
    assert result is None


@pytest.mark.asyncio
async def test_get_sandbox_max_lots_with_price():
    """price передаётся в запрос (Quotation)."""
    c = make_client()
    fake_resp = MagicMock()
    fake_resp.sell_limits.sell_max_lots = 10
    fake_resp.buy_limits.buy_max_lots = 10
    mock = AsyncMock(return_value=fake_resp)
    c.client.sandbox.get_sandbox_max_lots = mock
    await c.get_sandbox_max_lots("F1", price=100.5)
    args, kwargs = mock.call_args
    # Обновлён: SDK принимает get_sandbox_max_lots(*, request: GetMaxLotsRequest);
    # цена внутри request.price (проверено на реальном SDK).
    assert "request" in kwargs
    req = kwargs["request"]
    assert req.instrument_id == "F1"
    assert req.price.units == 100
    assert req.price.nano == 500000000