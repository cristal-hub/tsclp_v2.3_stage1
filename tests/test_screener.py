# tests/test_screener.py
# Tsclp_v2.2_volatile — тесты скринера.
# Обновлено: разделены «ошибка сканирования» и «валидный результат < 10».
# Старый test_screener_error_keeps_last_success удалён (разделён).
# Исправлен test_screener_filters_low_turnover (volume=100_000 для turnover 10 млн < 50 млн).

import pytest
from datetime import datetime

import screener as screener_module
from screener import Screener
import config


class FakeQuotation:
    def __init__(self, value):
        self.units = int(value)
        self.nano = int((value % 1) * 1e9)


class FakeCandle:
    def __init__(self, high, low, close, volume):
        self.high = FakeQuotation(high)
        self.low = FakeQuotation(low)
        self.close = FakeQuotation(close)
        self.volume = volume
        self.time = datetime.now()


class FakeShare:
    def __init__(self, figi, ticker, name, real_exchange=1, currency="rub",
                 buy_available_flag=True, sell_available_flag=True):
        self.figi = figi
        self.ticker = ticker
        self.name = name
        self.real_exchange = real_exchange
        self.currency = currency
        self.buy_available_flag = buy_available_flag
        self.sell_available_flag = sell_available_flag


class FakeSharesResponse:
    def __init__(self, shares):
        self.instruments = shares


class FakeInstrumentsApi:
    def __init__(self, shares):
        self._shares = shares
        self.calls = 0

    async def shares(self, instrument_status=None):
        self.calls += 1
        return FakeSharesResponse(self._shares)


class FakeAsyncClient:
    instances = []

    def __init__(self, *args, **kwargs):
        self._shares = []
        self._candles_by_figi = {}
        self._raise_on = set()
        self.instruments = FakeInstrumentsApi(self._shares)
        self.candle_calls = 0
        FakeAsyncClient.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    async def get_all_candles(self, figi, from_, interval):
        self.candle_calls += 1
        if figi in self._raise_on:
            raise RuntimeError(f"API error for {figi}")
        for c in self._candles_by_figi.get(figi, []):
            yield c


def _install_fake_client(monkeypatch, shares, candles_by_figi, raise_on=None):
    FakeAsyncClient.instances = []

    def _factory(*args, **kwargs):
        inst = FakeAsyncClient()
        inst._shares = shares
        inst.instruments = FakeInstrumentsApi(shares)
        inst._candles_by_figi = candles_by_figi
        inst._raise_on = raise_on or set()
        return inst

    monkeypatch.setattr(screener_module, "AsyncClient", _factory)
    monkeypatch.setattr(config, "API_CALL_DELAY_SECONDS", 0)
    monkeypatch.setattr(config, "API_RETRY_DELAY_SECONDS", 0)


# --- Успешные сценарии ---

@pytest.mark.asyncio
async def test_screener_returns_top_n(monkeypatch):
    shares = [
        FakeShare("F1", "AAA", "A"),
        FakeShare("F2", "BBB", "B"),
        FakeShare("F3", "CCC", "C"),
    ]
    candles = {
        "F1": [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)],
        "F2": [FakeCandle(105, 95, 100, 1_000_000) for _ in range(30)],
        "F3": [FakeCandle(101, 99, 100, 1_000_000) for _ in range(30)],
    }
    _install_fake_client(monkeypatch, shares, candles)
    screener = Screener()
    result = await screener.select_top_volatile()
    assert result is not None
    assert len(result) == 3
    assert result[0]['ticker'] == "AAA"


@pytest.mark.asyncio
async def test_screener_valid_result_less_than_10(monkeypatch):
    """Полное сканирование, критериям соответствует < 10 — валидно."""
    shares = [
        FakeShare("F1", "AAA", "A"),
        FakeShare("F2", "BBB", "B"),
        FakeShare("F3", "CCC", "C"),
    ]
    candles = {f: [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)]
               for f in ("F1", "F2", "F3")}
    _install_fake_client(monkeypatch, shares, candles)
    screener = Screener()
    result = await screener.select_top_volatile()
    assert result is not None
    assert len(result) == 3  # валидно, не 10
    assert screener.last_errors_count == 0


@pytest.mark.asyncio
async def test_screener_filters_low_turnover(monkeypatch):
    """
    F1 не прошёл по turnover (avg_turnover < MIN_DAILY_TURNOVER_RUB) -> None.
    volume=100_000, close=100 -> avg_turnover = 10_000_000 < 50_000_000.
    """
    monkeypatch.setattr(config, "MIN_DAILY_TURNOVER_RUB", 50_000_000)
    shares = [FakeShare("F1", "AAA", "A")]
    candles = {"F1": [FakeCandle(110, 90, 100, 100_000) for _ in range(30)]}
    _install_fake_client(monkeypatch, shares, candles)
    screener = Screener()
    result = await screener.select_top_volatile()
    assert result is None
    assert screener.last_errors_count == 0  # не ошибка, а отклонение по критерию


@pytest.mark.asyncio
async def test_screener_returns_none_on_empty(monkeypatch):
    _install_fake_client(monkeypatch, [], {})
    screener = Screener()
    result = await screener.select_top_volatile()
    assert result is None


@pytest.mark.asyncio
async def test_screener_cache(monkeypatch):
    shares = [FakeShare("F1", "AAA", "A")]
    candles = {"F1": [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)]}
    _install_fake_client(monkeypatch, shares, candles)
    screener = Screener()
    r1 = await screener.select_top_volatile()
    calls_after_first = FakeAsyncClient.instances[0].candle_calls
    r2 = await screener.select_top_volatile()
    assert len(FakeAsyncClient.instances) == 1
    assert FakeAsyncClient.instances[0].candle_calls == calls_after_first
    assert r1 == r2


# --- Ошибки ---

@pytest.mark.asyncio
async def test_screener_partial_scan_returns_none(monkeypatch):
    """
    Ошибка по одному инструменту (errors_count > 0) -> None,
    даже если по остальным удалось набрать результат.
    """
    shares = [FakeShare("F1", "AAA", "A"), FakeShare("F2", "BBB", "B")]
    candles = {
        "F1": [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)],
        "F2": [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)],
    }
    _install_fake_client(monkeypatch, shares, candles, raise_on={"F2"})
    screener = Screener()
    result = await screener.select_top_volatile()
    assert result is None  # неполное сканирование
    assert screener.last_errors_count == 1
    assert screener.last_processed_count == 2


@pytest.mark.asyncio
async def test_screener_full_error_returns_none(monkeypatch):
    """Все инструменты с ошибкой -> None."""
    shares = [FakeShare("F1", "AAA", "A")]
    _install_fake_client(monkeypatch, shares, {}, raise_on={"F1"})
    screener = Screener()
    result = await screener.select_top_volatile()
    assert result is None
    assert screener.last_errors_count == 1


@pytest.mark.asyncio
async def test_screener_error_does_not_return_stale_cache(monkeypatch):
    """
    После успешного сканирования (кэш) — ошибка -> None, НЕ старый кэш.
    """
    shares = [FakeShare("F1", "AAA", "A")]
    candles = {"F1": [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)]}
    _install_fake_client(monkeypatch, shares, candles)
    screener = Screener()
    first = await screener.select_top_volatile()
    assert first is not None

    def _broken_factory(*args, **kwargs):
        raise RuntimeError("API down")

    monkeypatch.setattr(screener_module, "AsyncClient", _broken_factory)
    second = await screener.select_top_volatile(force=True)
    assert second is None  # НЕ старый кэш
    assert screener.cached_instruments == first  # кэш сохранён для диагностики


@pytest.mark.asyncio
async def test_screener_partial_scan_does_not_return_partial_result(monkeypatch):
    """Неполное сканирование не возвращает частичный список как TOP-10."""
    shares = [FakeShare("F1", "AAA", "A"), FakeShare("F2", "BBB", "B")]
    candles = {
        "F1": [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)],
        "F2": [FakeCandle(110, 90, 100, 1_000_000) for _ in range(30)],
    }
    _install_fake_client(monkeypatch, shares, candles, raise_on={"F2"})
    screener = Screener()
    result = await screener.select_top_volatile()
    assert result is None