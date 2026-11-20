# tests/test_diagnose_sandbox.py
# Tsclp_v2.2_volatile — тесты diagnose_sandbox.py (mock).

import argparse
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import diagnose_sandbox as ds


# --- _parse_tickers ---

def test_parse_tickers_ok():
    assert ds._parse_tickers("SBER") == ["SBER"]


def test_parse_tickers_multiple():
    assert ds._parse_tickers("SBER,GAZP,LKOH") == ["SBER", "GAZP", "LKOH"]


def test_parse_tickers_with_spaces():
    assert ds._parse_tickers("SBER, GAZP") == ["SBER", "GAZP"]


def test_parse_tickers_empty_string():
    with pytest.raises(argparse.ArgumentTypeError):
        ds._parse_tickers("")


def test_parse_tickers_empty_element():
    with pytest.raises(argparse.ArgumentTypeError):
        ds._parse_tickers("SBER,,GAZP")


def test_parse_tickers_space_inside():
    with pytest.raises(argparse.ArgumentTypeError):
        ds._parse_tickers("SBE R")


# --- main: без --yes ---

@pytest.mark.asyncio
async def test_main_without_yes(monkeypatch):
    monkeypatch.setattr("sys.argv", ["diagnose_sandbox.py"])
    rc = await ds.main()
    assert rc == 0


# --- main: пустой список тикеров ---

@pytest.mark.asyncio
async def test_main_empty_tickers(monkeypatch):
    monkeypatch.setattr("sys.argv", ["diagnose_sandbox.py", "--yes"])
    monkeypatch.setattr(ds.config, "validate", lambda: None)

    fake_client = MagicMock()
    fake_client.users.get_accounts = AsyncMock(
        return_value=MagicMock(accounts=[MagicMock(id="acc")])
    )
    fake_client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=MagicMock(positions=[])
    )
    fake_client.sandbox.get_sandbox_stop_orders = AsyncMock(
        return_value=MagicMock(stop_orders=[])
    )

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(ds, "AsyncSandboxClient", lambda *a, **kw: FakeCtx())
    monkeypatch.setattr(ds.config, "TINKOFF_ACCOUNT_ID", "acc")

    rc = await ds.main()
    assert rc == 0


# --- main: неизвестный тикер -> неполная проверка (rc=2) ---

@pytest.mark.asyncio
async def test_main_unknown_ticker_returns_2(monkeypatch):
    monkeypatch.setattr("sys.argv", ["diagnose_sandbox.py", "--yes", "--tickers", "BAD"])
    monkeypatch.setattr(ds.config, "validate", lambda: None)
    monkeypatch.setattr(ds.config, "TINKOFF_ACCOUNT_ID", "acc")

    fake_client = MagicMock()
    fake_client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=MagicMock(positions=[])
    )
    fake_client.sandbox.get_sandbox_stop_orders = AsyncMock(
        return_value=MagicMock(stop_orders=[])
    )
    fake_client.instruments.get_instrument_by = AsyncMock(side_effect=RuntimeError("not found"))

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(ds, "AsyncSandboxClient", lambda *a, **kw: FakeCtx())

    rc = await ds.main()
    assert rc == 2


# --- main: найденный тикер -> rc=0 ---

@pytest.mark.asyncio
async def test_main_found_ticker_returns_0(monkeypatch):
    monkeypatch.setattr("sys.argv", ["diagnose_sandbox.py", "--yes", "--tickers", "SBER"])
    monkeypatch.setattr(ds.config, "validate", lambda: None)
    monkeypatch.setattr(ds.config, "TINKOFF_ACCOUNT_ID", "acc")

    fake_client = MagicMock()
    fake_client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=MagicMock(positions=[])
    )
    fake_client.sandbox.get_sandbox_stop_orders = AsyncMock(
        return_value=MagicMock(stop_orders=[])
    )
    fake_client.instruments.get_instrument_by = AsyncMock(
        return_value=MagicMock(instrument=MagicMock(figi="FIGI1"))
    )

    async def empty_candles(*args, **kwargs):
        return
        yield  # noqa

    fake_client.get_all_candles = empty_candles

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(ds, "AsyncSandboxClient", lambda *a, **kw: FakeCtx())

    rc = await ds.main()
    assert rc == 0


# --- main: пустой список стоп-заявок не падает ---

@pytest.mark.asyncio
async def test_main_empty_stops_ok(monkeypatch):
    monkeypatch.setattr("sys.argv", ["diagnose_sandbox.py", "--yes"])
    monkeypatch.setattr(ds.config, "validate", lambda: None)
    monkeypatch.setattr(ds.config, "TINKOFF_ACCOUNT_ID", "acc")

    fake_client = MagicMock()
    fake_client.sandbox.get_sandbox_portfolio = AsyncMock(
        return_value=MagicMock(positions=[])
    )
    fake_client.sandbox.get_sandbox_stop_orders = AsyncMock(
        return_value=MagicMock(stop_orders=[])
    )

    class FakeCtx:
        async def __aenter__(self): return fake_client
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(ds, "AsyncSandboxClient", lambda *a, **kw: FakeCtx())

    rc = await ds.main()
    assert rc == 0