# tests/test_diag_max_lots.py
# Tsclp_v2.2_volatile — тесты diag_max_lots.py (mock).

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import diag_max_lots as dml


# Скрипт не имеет выделенных функций для тестирования (main — монолит).
# Тесты проверяют, что main не падает при разных mock-сценариях.

@pytest.mark.asyncio
async def test_main_positions_none(monkeypatch):
    """get_positions()=None -> rc=1, без вызовов GetSandboxMaxLots."""
    fake_client = MagicMock()
    fake_client.get_positions = AsyncMock(return_value=None)

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(dml, "TinkoffClient", lambda *a, **kw: FakeCtx())
    monkeypatch.setattr(dml.config, "validate", lambda: None)
    monkeypatch.setattr(dml.config, "USE_SANDBOX", True)

    rc = await dml.main()
    assert rc == 1
    fake_client.get_sandbox_max_lots.assert_not_called()


@pytest.mark.asyncio
async def test_main_empty_portfolio(monkeypatch):
    """Пустой портфель -> rc=0, без вызовов GetSandboxMaxLots."""
    fake_client = MagicMock()
    fake_client.get_positions = AsyncMock(return_value={})

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(dml, "TinkoffClient", lambda *a, **kw: FakeCtx())
    monkeypatch.setattr(dml.config, "validate", lambda: None)
    monkeypatch.setattr(dml.config, "USE_SANDBOX", True)

    rc = await dml.main()
    assert rc == 0
    fake_client.get_sandbox_max_lots.assert_not_called()


@pytest.mark.asyncio
async def test_main_max_lots_none(monkeypatch):
    """GetSandboxMaxLots=None -> не падает."""
    from tinkoff_client import PositionData
    from decimal import Decimal

    fake_client = MagicMock()
    fake_client.get_positions = AsyncMock(return_value={
        "F1": PositionData(lots=10, direction="LONG", raw_shares=Decimal(10)),
    })
    fake_client.get_instrument_info = AsyncMock(return_value={"lot": 1, "ticker": "T"})
    fake_client.get_sandbox_max_lots = AsyncMock(return_value=None)

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(dml, "TinkoffClient", lambda *a, **kw: FakeCtx())
    monkeypatch.setattr(dml.config, "validate", lambda: None)
    monkeypatch.setattr(dml.config, "USE_SANDBOX", True)

    rc = await dml.main()
    assert rc == 0