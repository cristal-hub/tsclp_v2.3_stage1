
import pandas as pd
import pytest

import config
import strategy
from strategy import StrategyManager


def make_df(closes, volumes=None):
    n = len(closes)
    if volumes is None:
        volumes = [1000] * n

    return pd.DataFrame({
        'open': closes,
        'high': [c * 1.001 for c in closes],
        'low': [c * 0.999 for c in closes],
        'close': closes,
        'volume': volumes,
    }, index=pd.date_range('2026-01-01', periods=n, freq='5min'))


def make_adx_df(candles_df, value):
    """Создаёт контролируемые значения ADX для изолированного теста."""
    return pd.DataFrame(
        {f"ADX_{config.ADX_PERIOD}": [value] * len(candles_df)},
        index=candles_df.index,
    )


def install_fake_adx(monkeypatch, state):
    """Подменяет расчёт ADX детерминированным значением из state."""
    def fake_adx(high, low, close, length):
        return pd.DataFrame(
            {f"ADX_{length}": [state["value"]] * len(close)},
            index=close.index,
        )

    monkeypatch.setattr(strategy.ta, "adx", fake_adx)


def shift_df(df, minutes):
    result = df.copy()
    result.index = result.index + pd.Timedelta(minutes=minutes)
    return result


# ---------------------------------------------------------
# СТАРЫЕ ТЕСТЫ
# ---------------------------------------------------------

def test_insufficient_data_returns_hold():
    sm = StrategyManager()
    n = max(1, config.MIN_CANDLES_FOR_SIGNAL - 1)
    df = make_df([100.0] * n)

    signal, reason = sm.get_signal(df, 100.0)

    assert signal == "HOLD"
    assert reason


def test_ma100_missing_blocks_signal():
    sm = StrategyManager()
    closes = [100.0 + i * 0.1 for i in range(60)]
    df = make_df(closes)

    signal, reason = sm.get_signal(df, closes[-1])

    assert signal == "HOLD"


def test_volume_confirmation_rejects_low_volume(monkeypatch):
    monkeypatch.setattr(config, "VOLUME_SPIKE_FACTOR", 1.5)

    sm = StrategyManager()
    closes = [100.0] * 59 + [100.5]
    volumes = [1000] * 59 + [100]
    df = make_df(closes, volumes)

    ok, why = sm._volume_confirmation(df)

    assert ok is False
    assert why is not None


def test_volume_confirmation_accepts_spike(monkeypatch):
    monkeypatch.setattr(config, "VOLUME_SPIKE_FACTOR", 1.5)

    sm = StrategyManager()
    closes = [100.0] * 60
    volumes = [1000] * 59 + [5000]
    df = make_df(closes, volumes)

    ok, why = sm._volume_confirmation(df)

    assert ok is True
    assert why is None


def test_adx_hysteresis_does_not_flip_immediately():
    sm = StrategyManager()
    sm._last_regime = "TREND"
    sm._regime_streak = 0

    closes = [100.0 + i * 0.5 for i in range(150)]
    df = make_df(closes)

    regime1 = sm.get_market_regime(df)
    regime2 = sm.get_market_regime(df)

    assert regime1 in {"TREND", "RANGE", "UNKNOWN"}
    assert regime2 in {"TREND", "RANGE", "UNKNOWN"}


def test_prepare_dataframe_drops_incomplete_when_asked():
    sm = StrategyManager()
    df = make_df([100.0] * 10)
    df['is_complete'] = [True] * 9 + [False]

    prepared = sm._prepare_dataframe(df, drop_incomplete=True)

    assert len(prepared) == 9


def test_prepare_dataframe_keeps_incomplete_by_default():
    sm = StrategyManager()
    df = make_df([100.0] * 10)
    df['is_complete'] = [True] * 9 + [False]

    prepared = sm._prepare_dataframe(df, drop_incomplete=False)

    assert len(prepared) == 10


# ---------------------------------------------------------
# НОВЫЕ ТЕСТЫ: ADX HYSTERESIS
# ---------------------------------------------------------

def test_adx_same_candle_does_not_increase_streak(monkeypatch):
    """
    Повторный вызов на той же свече не должен засчитывать
    второе подтверждение смены режима.
    """
    state = {"value": 20.0}
    install_fake_adx(monkeypatch, state)

    sm = StrategyManager()
    sm._last_regime = "TREND"

    df = make_df([100.0] * 30)

    first = sm.get_market_regime(df)
    streak_after_first = sm._regime_streak

    second = sm.get_market_regime(df)
    streak_after_second = sm._regime_streak

    assert first == "TREND"
    assert second == "TREND"
    assert streak_after_first == 1
    assert streak_after_second == 1


def test_adx_flips_after_two_new_candles(monkeypatch):
    """
    При CONSERVATIVE смена режима происходит после двух
    последовательных новых свечей с подтверждением.
    """
    monkeypatch.setattr(config, "ADX_HYSTERESIS_CANDLES", 2)

    state = {"value": 20.0}
    install_fake_adx(monkeypatch, state)

    sm = StrategyManager()
    sm._last_regime = "TREND"

    df1 = make_df([100.0] * 30)
    regime1 = sm.get_market_regime(df1)

    # Повтор той же свечи не должен переключать режим.
    regime_repeat = sm.get_market_regime(df1)

    # Появилась следующая новая свеча.
    df2 = shift_df(df1, 5)
    regime2 = sm.get_market_regime(df2)

    assert regime1 == "TREND"
    assert regime_repeat == "TREND"
    assert regime2 == "RANGE"


def test_adx_candidate_reversal_resets_streak(monkeypatch):
    """
    Если кандидат на смену режима отменяется,
    незавершённая серия подтверждений сбрасывается.
    """
    monkeypatch.setattr(config, "ADX_HYSTERESIS_CANDLES", 2)

    state = {"value": 20.0}
    install_fake_adx(monkeypatch, state)

    sm = StrategyManager()
    sm._last_regime = "TREND"

    df1 = make_df([100.0] * 30)
    sm.get_market_regime(df1)

    assert sm._regime_streak == 1

    # Следующая свеча снова подтверждает текущий TREND.
    state["value"] = 30.0
    df2 = shift_df(df1, 5)
    regime2 = sm.get_market_regime(df2)

    assert regime2 == "TREND"
    assert sm._regime_streak == 0

    # Новый кандидат RANGE начинает подтверждения заново.
    state["value"] = 20.0
    df3 = shift_df(df2, 5)
    regime3 = sm.get_market_regime(df3)

    assert regime3 == "TREND"
    assert sm._regime_streak == 1


# ---------------------------------------------------------
# НОВЫЕ ТЕСТЫ: ОБРАБОТКА is_complete
# ---------------------------------------------------------

@pytest.mark.parametrize("unknown_value", [None, float("nan")])
def test_prepare_dataframe_keeps_unknown_completion_status(unknown_value):
    """
    None и NaN означают неизвестный статус свечи.
    Такие значения не должны приводить к удалению свечи.
    """
    sm = StrategyManager()

    df = make_df([100.0] * 10)
    df["is_complete"] = [True] * 9 + [unknown_value]

    prepared = sm._prepare_dataframe(df, drop_incomplete=True)

    assert len(prepared) == 10


def test_prepare_dataframe_drops_explicit_false():
    """
    Явное False означает незакрытую свечу.
    При drop_incomplete=True она должна удаляться.
    """
    sm = StrategyManager()

    df = make_df([100.0] * 10)
    df["is_complete"] = [True] * 9 + [False]

    prepared = sm._prepare_dataframe(df, drop_incomplete=True)

    assert len(prepared) == 9
