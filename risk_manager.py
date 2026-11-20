# risk_manager.py
# Tsclp_v2.2_volatile — управление рисками.
#
# ИЗМЕНЕНИЯ (этап 3):
#   - current_exposure: единицы строго |lots| * lot_size * price.
#     Использует PositionData.lots (лоты) из get_positions().
#   - Неизвестная стоимость позиции (get_instrument_info=None,
#     get_last_prices=None, p<=0) -> БЛОКИРОВКА (return 0), не пропуск.
#   - get_account_balance()=None -> БЛОКИРОВКА (return 0), не 0.0.
#   - get_positions()=None -> БЛОКИРОВКА (return 0).
#   - Пустой портфель {} — допустим (API подтвердил отсутствие позиций).
#   - POSITION_QUANTITY_IS_LOTS удалён (get_positions() уже возвращает лоты).
#   - export_state() / import_state(data) — для persistence
#     (daily_trade_counts, last_reset_date).
#
# ПАРАМЕТРЫ РИСКА НЕ ИЗМЕНЕНЫ:
#   RISK_PER_TRADE, STOP_LOSS_PERCENT, MIN_STOP_LOSS_PERCENT,
#   TAKE_PROFIT_RATIO, ATR_STOP_MULTIPLIER, MAX_ATR_STOP_PERCENT,
#   MAX_POSITION_SHARE_PER_INSTRUMENT, MAX_TOTAL_EXPOSURE,
#   MAX_TRADES_PER_INSTRUMENT_PER_DAY, COMMISSION_PER_SIDE, SLIPPAGE_PERCENT.
#
# СТРАТЕГИЯ НЕ ИЗМЕНЕНА: calculate_sl_tp, _calculate_volatility_risk —
#   без изменений. record_trade, reset_daily_counts — без изменений.

import logging
from typing import Optional, Tuple
from datetime import date

import pandas as pd
import pandas_ta_classic as ta

import config


class RiskManager:
    def __init__(self, tinkoff_client):
        self.tinkoff_client = tinkoff_client
        self.daily_trade_counts = {}
        self.last_reset_date = date.today()

    def reset_daily_counts(self):
        self.daily_trade_counts = {}
        self.last_reset_date = date.today()

    def _maybe_reset_by_date(self):
        if date.today() != self.last_reset_date:
            self.reset_daily_counts()

    def export_state(self) -> dict:
        """Сериализация для persistence (JSON-совместимый dict)."""
        return {
            "daily_trade_counts": dict(self.daily_trade_counts),
            "last_reset_date": self.last_reset_date.isoformat(),
        }

    def import_state(self, data: dict) -> None:
        """
        Восстановление из persistence.
        Некорректные данные пропускаются с логом (не падаем).
        """
        if not isinstance(data, dict):
            logging.error("[risk_manager] import_state: data не dict")
            return
        counts = data.get("daily_trade_counts", {})
        if isinstance(counts, dict):
            self.daily_trade_counts = {
                str(k): int(v) for k, v in counts.items()
                if isinstance(v, (int, float)) and int(v) >= 0
            }
        else:
            logging.error("[risk_manager] import_state: daily_trade_counts не dict")
            self.daily_trade_counts = {}

        lrd = data.get("last_reset_date")
        if isinstance(lrd, str):
            try:
                self.last_reset_date = date.fromisoformat(lrd)
            except ValueError:
                logging.error(f"[risk_manager] import_state: некорректная дата {lrd!r}")
                self.last_reset_date = date.today()
        else:
            self.last_reset_date = date.today()

        # Если дата в прошлом — сброс по дате произойдёт при следующем вызове
        # _maybe_reset_by_date (что корректно).

    async def calculate_position_size(self, figi: str, last_price: float,
                                      candles_df: Optional[pd.DataFrame] = None) -> int:
        self._maybe_reset_by_date()

        if self.daily_trade_counts.get(figi, 0) >= config.MAX_TRADES_PER_INSTRUMENT_PER_DAY:
            return 0

        try:
            # 1. Баланс: None -> блокировка (неизвестно)
            account_balance = await self.tinkoff_client.get_account_balance()
            if account_balance is None or account_balance <= 0:
                logging.warning(
                    f"[risk_manager] {figi}: баланс неизвестен/<= 0 "
                    f"({account_balance}) — блокировка"
                )
                return 0

            risk_amount = account_balance * config.RISK_PER_TRADE
            if risk_amount <= 0:
                return 0

            # 2. Инструмент: None -> блокировка
            instrument_info = await self.tinkoff_client.get_instrument_info(figi)
            if not instrument_info:
                logging.warning(f"[risk_manager] {figi}: instrument_info=None — блокировка")
                return 0

            lot_size = instrument_info['lot']
            if not lot_size or lot_size <= 0:
                logging.warning(f"[risk_manager] {figi}: lot_size={lot_size} — блокировка")
                return 0

            lot_cost = last_price * lot_size
            if lot_cost <= 0:
                return 0

            # 3. Расчёт по риску (без изменений в стратегии)
            base_risk = last_price * max(config.STOP_LOSS_PERCENT, config.MIN_STOP_LOSS_PERCENT)
            friction = last_price * (config.COMMISSION_PER_SIDE * 2 + config.SLIPPAGE_PERCENT)
            per_share_risk = base_risk + friction

            atr_risk = self._calculate_volatility_risk(candles_df)
            if atr_risk is not None:
                atr_cap = last_price * config.MAX_ATR_STOP_PERCENT
                atr_risk = min(atr_risk, atr_cap)
                per_share_risk = max(per_share_risk, atr_risk + friction)

            if per_share_risk <= 0:
                return 0

            position_size_by_risk = int(risk_amount / (per_share_risk * lot_size))
            if position_size_by_risk <= 0:
                return 0

            # 4. Расчёт по капиталу (без изменений)
            max_position_value = account_balance * config.MAX_POSITION_SHARE_PER_INSTRUMENT
            position_size_by_capital = int(max_position_value / lot_cost)
            if position_size_by_capital <= 0:
                return 0

            # 5. Экспозиция: строгие единицы |lots| * lot_size * price
            #    None от get_positions() -> блокировка; {} -> допустимо.
            positions = await self.tinkoff_client.get_positions()
            if positions is None:
                logging.warning(
                    f"[risk_manager] {figi}: get_positions()=None (ошибка API) — блокировка"
                )
                return 0

            current_exposure = 0.0
            for f, pos_data in positions.items():
                # Валютные позиции — кэш, не экспозиция
                if f.startswith(("RUB", "USD", "EUR")):
                    continue

                # PositionData: lots (со знаком), direction, raw_shares
                lots = abs(getattr(pos_data, "lots", 0))

                info = await self.tinkoff_client.get_instrument_info(f)
                if not info:
                    logging.warning(
                        f"[risk_manager] {figi}: экспозиция {f} неизвестна "
                        f"(instrument_info=None) — блокировка"
                    )
                    return 0

                lot_size_f = info.get('lot')
                if not lot_size_f or lot_size_f <= 0:
                    logging.warning(
                        f"[risk_manager] {figi}: экспозиция {f} неизвестна "
                        f"(lot_size={lot_size_f}) — блокировка"
                    )
                    return 0

                prices = await self.tinkoff_client.get_last_prices([f])
                if prices is None:
                    logging.warning(
                        f"[risk_manager] {figi}: экспозиция {f} неизвестна "
                        f"(get_last_prices=None) — блокировка"
                    )
                    return 0
                p = prices.get(f)
                if p is None or p <= 0:
                    logging.warning(
                        f"[risk_manager] {figi}: экспозиция {f} неизвестна "
                        f"(price={p}) — блокировка"
                    )
                    return 0

                current_exposure += lots * lot_size_f * p

            max_total = account_balance * config.MAX_TOTAL_EXPOSURE
            if current_exposure + lot_cost * position_size_by_capital > max_total:
                allowed_value = max(0.0, max_total - current_exposure)
                position_size_by_capital = int(allowed_value / lot_cost)

            position_size = min(position_size_by_risk, position_size_by_capital)
            return position_size if position_size > 0 else 0
        except Exception as e:
            logging.error(f"Ошибка расчёта позиции {figi}: {type(e).__name__}: {e}")
            return 0

    def _calculate_volatility_risk(self, candles_df: Optional[pd.DataFrame]) -> Optional[float]:
        """Без изменений (стратегия)."""
        if candles_df is None or candles_df.empty:
            return None
        if not {'high', 'low', 'close'}.issubset(candles_df.columns):
            return None
        period = config.ATR_PERIOD_INTRADAY
        if len(candles_df) < period:
            return None
        try:
            atr_series = ta.atr(
                high=candles_df['high'], low=candles_df['low'],
                close=candles_df['close'], length=period,
            )
            if atr_series is None or atr_series.empty:
                return None
            atr_val = atr_series.iloc[-1]
            if pd.isna(atr_val) or atr_val <= 0:
                return None
            return float(atr_val) * config.ATR_STOP_MULTIPLIER
        except Exception as e:
            logging.error(f"ATR risk error: {type(e).__name__}: {e}")
            return None

    def calculate_sl_tp(self, entry_price: float, direction: str,
                        candles_df: Optional[pd.DataFrame] = None) -> Tuple[Optional[float], Optional[float]]:
        """Без изменений (стратегия)."""
        if entry_price <= 0:
            return None, None
        base_stop_distance = entry_price * max(config.STOP_LOSS_PERCENT, config.MIN_STOP_LOSS_PERCENT)
        atr_risk = self._calculate_volatility_risk(candles_df)
        if atr_risk is not None:
            atr_cap = entry_price * config.MAX_ATR_STOP_PERCENT
            atr_risk = min(atr_risk, atr_cap)
            base_stop_distance = max(base_stop_distance, atr_risk)
        take_profit_distance = base_stop_distance * config.TAKE_PROFIT_RATIO
        if direction == "BUY":
            sl = entry_price - base_stop_distance
            tp = entry_price + take_profit_distance
            if sl >= entry_price:
                return None, None
        elif direction == "SELL":
            sl = entry_price + base_stop_distance
            tp = entry_price - take_profit_distance
            if sl <= entry_price:
                return None, None
        else:
            return None, None
        if sl <= 0 or tp <= 0:
            return None, None
        return sl, tp

    def record_trade(self, figi: str):
        """Без изменений."""
        self.daily_trade_counts[figi] = self.daily_trade_counts.get(figi, 0) + 1