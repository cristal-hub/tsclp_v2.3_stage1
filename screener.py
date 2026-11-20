# screener.py
# Tsclp_v2.2_volatile — скрининг TOP-10 волатильных акций MOEX.
#
# ИЗМЕНЕНИЯ (этап 5):
#   - Разделены «ошибка сканирования» и «валидный результат < 10».
#     * errors_count > 0 -> сканирование неполное -> select_top_volatile
#       возвращает None (новые входы блокируются в main.py).
#     * errors_count == 0 и len(result) < TOP_VOLATILE_INSTRUMENTS_COUNT ->
#       валидный результат (не блокировать).
#   - Старый кэш НЕ возвращается как актуальный TOP-10 при ошибке.
#     cached_instruments / last_success_at сохраняются ТОЛЬКО для диагностики.
#   - processed_count — фактически обработанные инструменты (не только
#     прошедшие фильтры). Ошибка получения данных учитывается отдельно
#     от нормального отклонения по критериям.
#   - _do_scan() возвращает (result, errors_count, processed_count).
#
# НЕ ИЗМЕНЕНО: формула volatility = atr / last_close * 100,
#   критерии MOEX/RUB/buy/sell, MIN_DAILY_TURNOVER_RUB, ATR_PERIOD,
#   VOLATILITY_PERIOD_DAYS, REAL_EXCHANGE_MOEX = 1, кэш 24 ч,
#   AsyncClient(config.TINKOFF_READONLY_TOKEN).

import asyncio
import logging
from datetime import timedelta
from typing import Optional, Tuple

import pandas as pd
import pandas_ta_classic as ta
from t_tech.invest import AsyncClient, CandleInterval, InstrumentStatus
from t_tech.invest.utils import now

import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REAL_EXCHANGE_MOEX = 1


class Screener:
    def __init__(self):
        # Кэш используется только для диагностики и для валидного кэша
        # (когда errors_count == 0 в момент сканирования).
        self.cached_instruments = None
        self.cached_at = None
        self.last_success_at = None
        self.last_errors_count: int = 0
        self.last_processed_count: int = 0

    def _cache_valid(self) -> bool:
        """Кэш валиден, только если он был получен при полном сканировании."""
        if not self.cached_instruments or not self.cached_at:
            return False
        age_hours = (now() - self.cached_at).total_seconds() / 3600
        return age_hours < config.VOLATILITY_REFRESH_HOURS

    async def select_top_volatile(self, force: bool = False) -> Optional[list]:
        """
        Возвращает список инструментов TOP-N (может быть < N, если
        критериям соответствует меньше — это валидно).
        None — при ошибке сканирования (errors_count > 0) или
        отсутствии результата. main.py блокирует новые входы при None.
        """
        if not force and self._cache_valid():
            logging.info(f"[screener] Кэш ({len(self.cached_instruments)} инстр.)")
            return self.cached_instruments

        last_error = None
        for attempt in range(1, 3):
            try:
                result, errors_count, processed_count = await self._do_scan()
                self.last_errors_count = errors_count
                self.last_processed_count = processed_count

                if errors_count > 0:
                    # Неполное сканирование — не возвращаем как актуальный TOP-10.
                    logging.error(
                        f"[screener] Попытка {attempt}: неполное сканирование "
                        f"(errors={errors_count}, processed={processed_count}). "
                        f"Результат не используется для новых входов."
                    )
                    last_error = f"errors_count={errors_count}"
                    if attempt < 2:
                        await asyncio.sleep(config.API_RETRY_DELAY_SECONDS)
                    continue

                if result is None:
                    logging.warning(f"[screener] Попытка {attempt}: пусто (без ошибок).")
                    last_error = "empty"
                    if attempt < 2:
                        await asyncio.sleep(config.API_RETRY_DELAY_SECONDS)
                    continue

                # Полное сканирование с валидным результатом (может быть < N).
                self.cached_instruments = result
                self.cached_at = now()
                self.last_success_at = now()
                logging.info(
                    f"[screener] Скрининг: {len(result)} инструментов "
                    f"(processed={processed_count}, errors=0)."
                )
                return result
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                logging.error(f"[screener] Попытка {attempt} ошибка: {type(e).__name__}: {e}")
                if attempt < 2:
                    await asyncio.sleep(config.API_RETRY_DELAY_SECONDS)

        # Все попытки неудачны. НЕ возвращаем старый кэш как актуальный.
        if self.cached_instruments:
            logging.error(
                f"[screener] Скрининг не удался ({last_error}). "
                f"Последний успешный список ({len(self.cached_instruments)} инстр.) "
                f"сохранён для диагностики, НЕ используется для новых входов."
            )
        else:
            logging.error(f"[screener] Скрининг не удался ({last_error}). Кэша нет.")
        return None

    async def _do_scan(self) -> Tuple[Optional[list], int, int]:
        """
        Возвращает (result, errors_count, processed_count).
        errors_count — количество инструментов, по которым произошла ошибка
        получения данных (исключение). Нормальное отклонение по критериям
        (мало свечей, atr <= 0, low turnover) — НЕ ошибка, а processed_count.
        processed_count — фактически обработанные инструменты.
        """
        errors_count = 0
        processed_count = 0

        async with AsyncClient(config.TINKOFF_READONLY_TOKEN) as client:
            instruments = await client.instruments.shares(
                instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL
            )
            shares = [
                s for s in instruments.instruments
                if getattr(s, 'real_exchange', 0) == REAL_EXCHANGE_MOEX
                and (s.currency or "").lower() == "rub"
                and s.buy_available_flag and s.sell_available_flag
            ]
            logging.info(f"[screener] Акций MOEX: {len(shares)}")

            if not shares:
                return None, 0, 0

            volatility_data = []
            for share in shares:
                processed_count += 1
                try:
                    await asyncio.sleep(config.API_CALL_DELAY_SECONDS)
                    candles = []
                    async for c in client.get_all_candles(
                        figi=share.figi,
                        from_=now() - timedelta(days=config.VOLATILITY_PERIOD_DAYS),
                        interval=CandleInterval.CANDLE_INTERVAL_DAY,
                    ):
                        candles.append(c)

                    # Нормальное отклонение (не ошибка)
                    if len(candles) < config.VOLATILITY_PERIOD_DAYS - 1:
                        continue

                    df = pd.DataFrame([{
                        'high': float(c.high.units + c.high.nano / 1e9),
                        'low': float(c.low.units + c.low.nano / 1e9),
                        'close': float(c.close.units + c.close.nano / 1e9),
                        'volume': int(c.volume),
                    } for c in candles])

                    atr_series = ta.atr(
                        high=df['high'], low=df['low'], close=df['close'],
                        length=config.ATR_PERIOD,
                    )
                    if atr_series is None or atr_series.dropna().empty:
                        continue
                    atr = float(atr_series.dropna().iloc[-1])
                    last_close = float(df['close'].iloc[-1])
                    if last_close <= 0 or atr <= 0:
                        continue
                    avg_turnover = float(df['volume'].tail(20).mean()) * float(df['close'].tail(20).mean())
                    if avg_turnover < config.MIN_DAILY_TURNOVER_RUB:
                        continue
                    volatility_data.append({
                        'figi': share.figi, 'ticker': share.ticker, 'name': share.name,
                        'volatility': (atr / last_close) * 100,
                        'avg_turnover_rub': avg_turnover,
                    })
                except Exception as e:
                    # Ошибка получения данных — учитывается отдельно
                    errors_count += 1
                    logging.warning(f"[screener] {share.ticker}: {type(e).__name__}: {e}")
                    continue

            if not volatility_data:
                return None, errors_count, processed_count

            volatility_data.sort(key=lambda x: x['volatility'], reverse=True)
            result = volatility_data[:config.TOP_VOLATILE_INSTRUMENTS_COUNT]
            return result, errors_count, processed_count