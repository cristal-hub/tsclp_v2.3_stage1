# config.py
# Tsclp_v2.1_volatile — конфигурация.
# STRATEGY LOGIC: UNCHANGED. RESEARCH ONLY.
# ИЗМЕНЕНО: PROFILE = "CONSERVATIVE" (после стабилизации механики).
# v2.3.0-stage1: секция "Ступень 1" (безубыток/трейлинг — ВЫКЛЮЧЕНЫ по
# умолчанию) и BUILD_VERSION. Стратегия входа и риск-модель НЕ менялись.

import os
from dotenv import load_dotenv

load_dotenv()

# --- API и Telegram ---
TINKOFF_API_TOKEN = os.getenv("TINKOFF_API_TOKEN")
TINKOFF_READONLY_TOKEN = os.getenv("TINKOFF_READONLY_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TINKOFF_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID")
TELEGRAM_ALLOWED_USER_ID = os.getenv("TELEGRAM_ALLOWED_USER_ID")

# --- Режим ---
USE_SANDBOX = True
ENABLE_TELEGRAM = True

# --- Скрининг ---
TOP_VOLATILE_INSTRUMENTS_COUNT = 10
VOLATILITY_PERIOD_DAYS = 30
VOLATILITY_REFRESH_HOURS = 24
MIN_DAILY_TURNOVER_RUB = 10_000_000

# --- Риск (НЕ меняется) ---
RISK_PER_TRADE = 0.01
MIN_STOP_LOSS_PERCENT = 0.005
STOP_LOSS_PERCENT = 0.012
TAKE_PROFIT_RATIO = 2.0
ATR_STOP_MULTIPLIER = 1.5
ATR_PERIOD_INTRADAY = 20
ATR_PERIOD = 14
MAX_ATR_STOP_PERCENT = 0.03
MAX_POSITION_SHARE_PER_INSTRUMENT = 0.2
MAX_TOTAL_EXPOSURE = 0.6
MAX_TRADES_PER_INSTRUMENT_PER_DAY = 5

# --- Ступень 1 (v2.3.0-stage1): безубыток/трейлинг и версия сборки ---
# ВНИМАНИЕ: BE_TRAIL_ENABLED=False — механика ВЫКЛЮЧЕНА. Включать явным True
# только после того, как пакет В (В1-В4) проверен в поле и журнал стабилен.
# При True каждый цикл запрашивает свечи по каждой OPEN-позиции (экстремумы
# и ATR) — доп. нагрузка ~1 запрос/позиция/цикл.
BUILD_VERSION = "v2.3.0-stage1"
BE_TRAIL_ENABLED = False
BE_TRIGGER_R = 1.0          # r_peak по ДОСТИГНУТОМУ максимуму (mfe_price) для перевода в безубыток
TRAIL_ATR_PERIOD = 14       # период ATR для трейлинга (5-минутные свечи)
TRAIL_ATR_MULTIPLIER = 3.0  # дистанция трейлинга: SL = mfe - 3*ATR (BUY; для SELL зеркально)

# --- Семантика quantity из get_positions() ---
POSITION_QUANTITY_IS_LOTS = True

# --- Комиссия и проскальзывание ---
COMMISSION_MODE = "TRADER"
COMMISSION_PER_SIDE = 0.003 if COMMISSION_MODE == "INVESTOR" else 0.0005
SLIPPAGE_PERCENT = 0.0002

# --- Стратегия (НЕ меняется) ---
MA_FAST_PERIOD = 9
MA_SLOW_PERIOD = 21
TREND_FILTER_MA_PERIOD = 100
BB_PERIOD = 20
BB_STD_DEV = 2
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
MIN_CANDLES_FOR_SIGNAL = 60
ADX_PERIOD = 14

# --- Профиль фильтров ---
PROFILE = "CONSERVATIVE"

if PROFILE == "FORCE_TRADE":
    ADX_TREND_THRESHOLD = 5
    ADX_HYSTERESIS_CANDLES = 1
    VOLUME_SPIKE_FACTOR = 0.0
    MA_FAST_PERIOD = 2
    MA_SLOW_PERIOD = 3
    TREND_FILTER_MA_PERIOD = 5
    RSI_OVERSOLD = 50
    RSI_OVERBOUGHT = 50
    MIN_CANDLES_FOR_SIGNAL = 10
elif PROFILE == "SANDBOX_TEST":
    ADX_TREND_THRESHOLD = 15
    ADX_HYSTERESIS_CANDLES = 1
    VOLUME_SPIKE_FACTOR = 0.0
    MA_FAST_PERIOD = 5
    MA_SLOW_PERIOD = 13
    TREND_FILTER_MA_PERIOD = 50
    RSI_OVERSOLD = 40
    RSI_OVERBOUGHT = 60
    MIN_CANDLES_FOR_SIGNAL = 60
elif PROFILE == "TECH_CHECK":
    ADX_TREND_THRESHOLD = 20
    ADX_HYSTERESIS_CANDLES = 1
    VOLUME_SPIKE_FACTOR = 1.0
    MA_FAST_PERIOD = 9
    MA_SLOW_PERIOD = 21
    TREND_FILTER_MA_PERIOD = 100
    RSI_OVERSOLD = 30
    RSI_OVERBOUGHT = 70
    MIN_CANDLES_FOR_SIGNAL = 60
elif PROFILE == "CONSERVATIVE":
    ADX_TREND_THRESHOLD = 25
    ADX_HYSTERESIS_CANDLES = 2
    VOLUME_SPIKE_FACTOR = 1.5
    MA_FAST_PERIOD = 9
    MA_SLOW_PERIOD = 21
    TREND_FILTER_MA_PERIOD = 100
    RSI_OVERSOLD = 30
    RSI_OVERBOUGHT = 70
    MIN_CANDLES_FOR_SIGNAL = 60
else:
    raise RuntimeError(f"Неизвестный PROFILE: {PROFILE}")

VOLUME_CONFIRMATION_WINDOW = 20

# --- API ---
API_RETRY_COUNT = 3
API_RETRY_DELAY_SECONDS = 5
API_REQUEST_TIMEOUT_SECONDS = 10
API_CALL_DELAY_SECONDS = 0.03

# --- Тайминги ---
TRADING_CYCLE_SECONDS = 30
CANDLES_LOOKBACK_DAYS = 2

# --- Отчётность ---
STARTING_BALANCE = os.getenv("STARTING_BALANCE")


def validate():
    missing = []
    if not TINKOFF_API_TOKEN:
        missing.append("TINKOFF_API_TOKEN")
    if not TINKOFF_READONLY_TOKEN:
        missing.append("TINKOFF_READONLY_TOKEN")
    if ENABLE_TELEGRAM:
        if not TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID")
        if not TELEGRAM_ALLOWED_USER_ID:
            missing.append("TELEGRAM_ALLOWED_USER_ID")
    if missing:
        raise RuntimeError("Не заданы: " + ", ".join(missing))
    if not USE_SANDBOX:
        raise RuntimeError("USE_SANDBOX=False запрещён. Только песочница.")