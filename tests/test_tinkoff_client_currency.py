# tests/test_tinkoff_client_currency.py
# Tsclp_v2.2_volatile — тесты get_positions: валютные позиции (этап 1.2).

import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from tinkoff_client import TinkoffClient, PositionData


def make_client():
    c = TinkoffClient("fake", use_sandbox=True)
    c.client = MagicMock()
    c.account_id = "acc"
    return c


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


# 1. RUB 97761.59211000 — без UNCERTAIN
@pytest.mark.asyncio
async def test_currency_rub_no_uncertain():
    c = make_client()
    # 97761.59211 -> units=97761, nano=592110000
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([
            FakePosition("RUB000UTSTOM", 97761, 592110000),
        ])
    )
    c.get_instrument_info = AsyncMock(return_value={
        "lot": 1, "instrument_type": "currency", "ticker": "RUB", "name": "RUB",
    })
    result = await c.get_positions()
    assert result == {}  # валютная не включается
    assert c.uncertain_state is False  # без ложного UNCERTAIN


# 2. Валютная позиция с дробным количеством — без UNCERTAIN
@pytest.mark.asyncio
async def test_currency_fractional_no_uncertain():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([
            FakePosition("USD000UTSTOM", 100, 500000000),  # 100.5
        ])
    )
    c.get_instrument_info = AsyncMock(return_value={
        "lot": 1, "instrument_type": "currency", "ticker": "USD", "name": "USD",
    })
    result = await c.get_positions()
    assert result == {}
    assert c.uncertain_state is False


# 3. Акция с количеством, не кратным lot_size — UNCERTAIN
@pytest.mark.asyncio
async def test_share_remainder_uncertain():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([FakePosition("SBER", 15)])  # 15 штук
    )
    c.get_instrument_info = AsyncMock(return_value={
        "lot": 10, "instrument_type": "share", "ticker": "SBER", "name": "SBER",
    })
    result = await c.get_positions()
    # Обновлён: fail-closed — недостоверный снимок возвращается как None
    assert result is None
    assert c.uncertain_state is True


# 4. Акция с корректным количеством — штатная обработка
@pytest.mark.asyncio
async def test_share_correct_quantity():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([FakePosition("SBER", 100)])  # 100 штук
    )
    c.get_instrument_info = AsyncMock(return_value={
        "lot": 10, "instrument_type": "share", "ticker": "SBER", "name": "SBER",
    })
    result = await c.get_positions()
    assert "SBER" in result
    assert result["SBER"].lots == 10
    assert result["SBER"].direction == "LONG"
    assert c.uncertain_state is False


# 5. Ошибка получения данных — fail-closed
@pytest.mark.asyncio
async def test_api_error_fail_closed():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(side_effect=RuntimeError("api down"))
    result = await c.get_positions()
    assert result is None
    assert c.uncertain_state is True


# 6. Корректная валютная + некорректная акция — ошибка акции сохраняется
@pytest.mark.asyncio
async def test_currency_ok_share_error_preserved():
    c = make_client()
    c.client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=FakePortfolio([
            FakePosition("RUB000UTSTOM", 97761, 592110000),  # валюта — OK
            FakePosition("SBER", 15),  # акция — остаток
        ])
    )

    async def fake_info(figi):
        if figi == "RUB000UTSTOM":
            return {"lot": 1, "instrument_type": "currency", "ticker": "RUB", "name": "RUB"}
        return {"lot": 10, "instrument_type": "share", "ticker": "SBER", "name": "SBER"}

    c.get_instrument_info = AsyncMock(side_effect=fake_info)
    result = await c.get_positions()
    # Обновлён: fail-closed — ошибка акции делает весь снимок недостоверным
    assert result is None  # частичный dict не выдаётся наружу
    assert c.uncertain_state is True  # ошибка акции сохранена