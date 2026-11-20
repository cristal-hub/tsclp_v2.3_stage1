# tinkoff_client.py
# Tsclp_v2.2_volatile — клиент T-Invest API (ТОЛЬКО ПЕСОЧНИЦА).
#
# ИЗМЕНЕНИЯ (этап 1):
#   - get_positions() возвращает dict[figi, PositionData] (лоты со знаком,
#     direction, raw_shares). Проверка делимости на lot_size; при остатке —
#     uncertain_state=True, позиция не возвращается (fail-closed).
#   - get_last_prices() → None при ошибке API, {} при успешном пустом ответе.
#   - get_account_balance() → None при ошибке (не 0.0).
#   - post_market_order() → OrderResult(order_id, state, error).
#     order_id сохраняется даже при потере ответа.
#   - cancel_order() — новый метод (отмена заявки).
#   - get_positions_debug() — без изменений (диагностика).
#   - get_instrument_info() — без изменений.
#
# ИЗМЕНЕНИЯ (этап 1.1):
#   - get_sandbox_max_lots(figi, price=None) — новый метод.
#     Возвращает {"sell_max_lots": int|None, "buy_max_lots": int|None}
#     или None при ошибке/неполном ответе. Единицы: лоты.
#     НЕ подставляет значения по умолчанию.
#
# ИЗМЕНЕНИЯ (этап 1.2):
#   - get_positions(): валютные позиции (instrument_type == "currency"
#     или префикс FIGI RUB/USD/EUR) НЕ включаются в PositionData
#     и НЕ проверяются на делимость. Ложный uncertain_state из-за
#     корректной валютной позиции исключён.
#   - Для торговых инструментов — прежняя строгая проверка делимости.
#   - get_instrument_info(): добавлено поле 'instrument_type' в кэш.
#
# НАПРАВЛЕНИЕ ПОЗИЦИИ:
#   OrderState.direction НЕ используется для позиции.
#   direction вычисляется ТОЛЬКО из знака PortfolioPosition.quantity.
#   Отрицательное quantity = SHORT (предположение Sandbox, проверяется mock-тестом).
#   Если знак не подтверждён (quantity == 0) — FLAT.
#   Если lot_size неизвестен или <= 0 — UNKNOWN, uncertain_state=True.
#
# ЦЕНА ИСПОЛНЕНИЯ: _extract_price — в execution.py (этап 2), не здесь.
#
# ИЗМЕНЕНИЯ (этап 5, партия P0 — устранение потери учёта заявок):
#   - post_market_order(..., order_id=None): order_id может быть передан
#     вызывающим кодом (сгенерирован ДО записи INTENT в OrderLedger).
#     None — прежнее поведение (генерация внутри клиента).
#   - get_positions(): fail-closed при неполном снимке. Любая ошибка обработки
#     строки портфеля (некорректное quantity, неизвестный lot_size, некратность,
#     ошибка API по инструменту) делает СНИМОК НЕДОСТОВЕРНЫМ: возвращается None,
#     а не частичный dict. Частичный ответ больше не может быть принят
#     за полный портфель (дефект D5). Валютные позиции — пропуск без
#     нарушения достоверности (как раньше).
#   - get_positions()/get_sandbox_order_state(): таймаут API_REQUEST_TIMEOUT_SECONDS
#     через asyncio.wait_for (дефект D8: зависание await-вызова без таймаута).
#   - get_sandbox_orders() — новый метод: список АКТИВНЫХ заявок брокера
#     (для четырёхсторонней сверки при рестарте). None при ошибке.

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional

from t_tech.invest import (
    CandleInterval,
    InstrumentIdType,
    OrderDirection,
    OrderType,
)
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import now, quotation_to_decimal, decimal_to_quotation

# GetMaxLotsRequest: путь импорта различается между сборками SDK.
#   SDK 1.51.0 (боевая сборка владельца): t_tech.invest.grpc.GetMaxLotsRequest.
#   SDK 0.3.3 (публичная сборка PyPI): класс есть, но экспортируется
#   из t_tech.invest (t_tech.invest.grpc.__init__ его не реэкспортирует).
#   Оба пути ведут к одному классу protobuf; первичный путь — 1.51.0.
try:
    from t_tech.invest.grpc import GetMaxLotsRequest
except ImportError:
    from t_tech.invest import GetMaxLotsRequest

import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class SandboxRequiredError(RuntimeError):
    pass


@dataclass
class PositionData:
    """Позиция в лотах со знаком + направление + исходное количество бумаг."""
    lots: int                  # лоты со знаком (отрицательное = SHORT)
    direction: str             # "LONG" | "SHORT" | "FLAT" | "UNKNOWN"
    raw_shares: Decimal        # исходное количество бумаг из SDK (штуки)


@dataclass
class OrderResult:
    """Результат отправки заявки. order_id сохраняется даже при ошибке."""
    order_id: str
    state: Optional[object]    # OrderState | None
    error: Optional[Exception]


# Префиксы FIGI валютных позиций (fallback, если instrument_type не получен)
CURRENCY_FIGI_PREFIXES = ("RUB", "USD", "EUR")

# Значение instrument_type для валюты (подтверждено документацией T-Банка:
# INSTRUMENT_TYPE_CURRENCY = "currency" в Python SDK)
CURRENCY_INSTRUMENT_TYPE = "currency"


class TinkoffClient:
    def __init__(self, token: str, use_sandbox: bool = True):
        if not use_sandbox:
            raise SandboxRequiredError("Tsclp_v2.2_volatile работает ТОЛЬКО в песочнице.")
        self.token = token
        self.use_sandbox = use_sandbox
        self._sandbox_client = None
        self.client = None
        self.account_id: Optional[str] = None
        self.instrument_info_cache = {}
        self.uncertain_state = False

    async def __aenter__(self):
        last_exc = None
        for attempt in range(config.API_RETRY_COUNT):
            try:
                self._sandbox_client = AsyncSandboxClient(self.token)
                self.client = await self._sandbox_client.__aenter__()
                logging.info("Подключение к T-Invest SANDBOX API успешно.")
                await self._ensure_account()
                return self
            except Exception as e:
                last_exc = e
                logging.error(f"Ошибка подключения (попытка {attempt + 1}): {type(e).__name__}: {e}")
                if attempt < config.API_RETRY_COUNT - 1:
                    await asyncio.sleep(config.API_RETRY_DELAY_SECONDS)
        raise RuntimeError(f"Не удалось подключиться: {type(last_exc).__name__}: {last_exc}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._sandbox_client:
            try:
                await self._sandbox_client.__aexit__(exc_type, exc_val, exc_tb)
            except Exception as e:
                logging.warning(f"Ошибка закрытия: {type(e).__name__}: {e}")

    async def _ensure_account(self):
        if config.TINKOFF_ACCOUNT_ID:
            self.account_id = config.TINKOFF_ACCOUNT_ID
            return
        accounts = await self.client.users.get_accounts()
        if accounts.accounts:
            self.account_id = accounts.accounts[0].id
        else:
            resp = await self.client.sandbox.open_sandbox_account()
            self.account_id = resp.account_id

    async def get_all_tradable_shares(self):
        try:
            instruments = await self.client.instruments.shares()
            return [
                s for s in instruments.instruments
                if (s.exchange or "").upper() == "MOEX"
                and (s.currency or "").lower() == "rub"
                and s.buy_available_flag and s.sell_available_flag
            ]
        except Exception as e:
            logging.error(f"Ошибка списка акций: {type(e).__name__}: {e}")
            return []

    async def get_historical_candles(self, figi, days, interval=CandleInterval.CANDLE_INTERVAL_5_MIN):
        try:
            await asyncio.sleep(config.API_CALL_DELAY_SECONDS)
            candles = []
            async for candle in self.client.get_all_candles(
                figi=figi, from_=now() - timedelta(days=days), interval=interval,
            ):
                candles.append(candle)
            return candles
        except Exception as e:
            logging.error(f"Ошибка свечей {figi}: {type(e).__name__}: {e}")
            return []

    async def get_last_prices(self, figi_list):
        """
        Возвращает dict[figi, float] (цена за штуку) при успехе.
        None — при ошибке API (состояние НЕИЗВЕСТНО).
        {} — при успешном ответе без цен (пустой список).
        """
        try:
            await asyncio.sleep(config.API_CALL_DELAY_SECONDS)
            response = await self.client.market_data.get_last_prices(figi=figi_list)
            return {
                p.figi: float(quotation_to_decimal(p.price))
                for p in response.last_prices if p.price is not None
            }
        except Exception as e:
            logging.error(f"Ошибка цен (состояние неизвестно): {type(e).__name__}: {e}")
            return None

    async def get_instrument_info(self, figi):
        if figi in self.instrument_info_cache:
            return self.instrument_info_cache[figi]
        try:
            response = await self.client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id=figi,
            )
            instrument = response.instrument
            inc = quotation_to_decimal(instrument.min_price_increment)
            # instrument_type: enum InstrumentType. Приводим к нижнему регистру строки
            # для сравнения. В Python SDK значение: "currency", "share", "bond" и т.д.
            raw_type = getattr(instrument, "instrument_type", None)
            if raw_type is None:
                instrument_type = None
            else:
                # Enum -> str (например, "currency", "share")
                try:
                    instrument_type = str(raw_type).lower()
                except Exception:
                    instrument_type = None
            info = {
                'lot': int(instrument.lot or 1),
                'min_price_increment': float(inc),
                'ticker': instrument.ticker,
                'name': instrument.name,
                'instrument_type': instrument_type,
            }
            self.instrument_info_cache[figi] = info
            return info
        except Exception as e:
            logging.error(f"Ошибка info {figi}: {type(e).__name__}: {e}")
            return None

    async def get_account_balance(self) -> Optional[float]:
        """
        Возвращает баланс (float) при успехе.
        None — при ошибке API (состояние НЕИЗВЕСТНО).
        """
        try:
            portfolio = await self.client.sandbox.get_sandbox_portfolio(account_id=self.account_id)
            return float(quotation_to_decimal(portfolio.total_amount_portfolio))
        except Exception as e:
            logging.error(f"Ошибка баланса (состояние неизвестно): {type(e).__name__}: {e}")
            return None

    async def get_positions(self) -> Optional[Dict[str, PositionData]]:
        """
        Возвращает dict[figi, PositionData] (лоты со знаком) при успехе.
        None — при ошибке API ИЛИ при неполном/недостоверном снимке (состояние НЕИЗВЕСТНО).
        {} — при успешном ответе с пустым портфелем.

        FAIL-CLOSED (этап 5): частичный снимок недопустим. Если хотя бы одна
        строка портфеля не может быть достоверно обработана — возвращается None.
        Вызывающий код (reconcile/риск/входы) обязан трактовать None как
        «портфель неизвестен» и не менять локальный учёт.

        Валютные позиции (instrument_type == "currency" или префикс FIGI
        RUB/USD/EUR) НЕ включаются в результат и НЕ проверяются на делимость.
        Это исключает ложный uncertain_state=True из-за корректной
        валютной позиции с дробным количеством (например, RUB 97761.59211).

        Для торговых инструментов — строгая проверка делимости:
          - shares = quotation_to_decimal(pos.quantity)
          - lot_size = get_instrument_info(figi).lot
          - если lot_size <= 0 или None -> uncertain_state=True, позиция НЕ возвращается
          - если shares % lot_size != 0 -> uncertain_state=True, позиция НЕ возвращается
          - lots = int(shares // lot_size) (со знаком)
          - direction: lots > 0 -> LONG, lots < 0 -> SHORT, lots == 0 -> FLAT
        """
        try:
            portfolio = await asyncio.wait_for(
                self.client.sandbox.get_sandbox_portfolio(account_id=self.account_id),
                timeout=config.API_REQUEST_TIMEOUT_SECONDS,
            )
            result: Dict[str, PositionData] = {}
            snapshot_valid = True  # этап 5: снимок достоверен, пока не доказано обратное
            for pos in portfolio.positions:
                figi = pos.figi
                try:
                    shares = quotation_to_decimal(pos.quantity)
                except (InvalidOperation, TypeError):
                    logging.error(f"[positions] {figi}: некорректное quantity — снимок недостоверный")
                    self.uncertain_state = True
                    snapshot_valid = False
                    continue

                if shares == 0:
                    continue  # FLAT — не включаем

                # --- Определение валютной позиции ---
                info = await self.get_instrument_info(figi)
                instrument_type = info.get('instrument_type') if info else None
                is_currency_by_type = (
                    instrument_type is not None
                    and CURRENCY_INSTRUMENT_TYPE in instrument_type
                )
                is_currency_by_prefix = figi.startswith(CURRENCY_FIGI_PREFIXES)

                if is_currency_by_type or is_currency_by_prefix:
                    # Валютная позиция — кэш, не торговый инструмент.
                    # Не проверяем делимость, не включаем в PositionData.
                    logging.debug(
                        f"[positions] {figi}: валюта (type={instrument_type!r}, "
                        f"prefix={is_currency_by_prefix}) — пропуск"
                    )
                    continue

                # --- Торговый инструмент: строгая проверка ---
                if not info or not info.get('lot'):
                    logging.error(f"[positions] {figi}: lot_size неизвестен — снимок недостоверный")
                    self.uncertain_state = True
                    snapshot_valid = False
                    continue

                lot_size = int(info['lot'])
                if lot_size <= 0:
                    logging.error(f"[positions] {figi}: lot_size={lot_size} <= 0 — снимок недостоверный")
                    self.uncertain_state = True
                    snapshot_valid = False
                    continue

                try:
                    shares_dec = Decimal(shares)
                except (InvalidOperation, TypeError):
                    logging.error(f"[positions] {figi}: некорректное Decimal — снимок недостоверный")
                    self.uncertain_state = True
                    snapshot_valid = False
                    continue

                lot_size_dec = Decimal(lot_size)
                if shares_dec % lot_size_dec != 0:
                    logging.error(
                        f"[positions] {figi}: shares={shares_dec} не кратно "
                        f"lot_size={lot_size_dec} — снимок недостоверный, остаток не отбрасывается"
                    )
                    self.uncertain_state = True
                    snapshot_valid = False
                    continue

                lots = int(shares_dec // lot_size_dec)  # со знаком
                if lots > 0:
                    direction = "LONG"
                elif lots < 0:
                    direction = "SHORT"
                else:
                    direction = "FLAT"

                result[figi] = PositionData(
                    lots=lots,
                    direction=direction,
                    raw_shares=shares_dec,
                )

            if not snapshot_valid:
                # Этап 5: частичный снимок НЕ выдаётся наружу.
                logging.error(
                    "[positions] Снимок портфеля неполный — возвращаю None "
                    "(сверка/учёт по частичному снимку запрещены)"
                )
                return None

            return result
        except Exception as e:
            logging.error(f"Ошибка позиций (состояние неизвестно): {type(e).__name__}: {e}")
            self.uncertain_state = True
            return None

    async def get_sandbox_orders(self) -> Optional[list]:
        """
        Список АКТИВНЫХ заявок брокера (для стартовой четырёхсторонней сверки).

        Возвращает список объектов OrderState при успехе,
        None — при ошибке API (состояние неизвестно).

        Используется ТОЛЬКО для восстановления учёта после перезапуска:
        заявка, известная брокеру, но отсутствующая в OrderLedger, переводится
        в UNCERTAIN с блокировкой BUY по её FIGI до разрешения.
        """
        try:
            response = await asyncio.wait_for(
                self.client.sandbox.get_sandbox_orders(account_id=self.account_id),
                timeout=config.API_REQUEST_TIMEOUT_SECONDS,
            )
            return list(getattr(response, "orders", []) or [])
        except Exception as e:
            logging.error(f"Ошибка списка заявок (состояние неизвестно): {type(e).__name__}: {e}")
            return None

    async def get_sandbox_max_lots(self, figi: str, price: Optional[float] = None) -> Optional[dict]:
        """
        GetSandboxMaxLots: доступное количество лотов для продажи/покупки.

        Возвращает dict:
            {"sell_max_lots": int | None, "buy_max_lots": int | None}
        или None при ошибке/неполном ответе.

        Единицы: лоты (подтверждено документацией T-Банка).
        НЕ подставляет значения по умолчанию. Если поле отсутствует/None
        или некорректно — соответствующий ключ = None.

        Для SHORT семантика buy_max_lots НЕ подтверждена — вызывающий код
        не должен использовать buy_max_lots для закрытия шорта без
        отдельного подтверждения.
        """
        try:
            request_kwargs = {
                "account_id": self.account_id,
                "instrument_id": figi,
            }
            if price is not None:
                try:
                    request_kwargs["price"] = decimal_to_quotation(Decimal(str(price)))
                except (InvalidOperation, TypeError, ValueError):
                    logging.warning(
                        f"[max_lots] {figi}: некорректная price={price!r} — "
                        f"запрос без price"
                    )

            request = GetMaxLotsRequest(**request_kwargs)
            response = await self.client.sandbox.get_sandbox_max_lots(request=request)

            sell_max = None
            buy_max = None

            sell_limits = getattr(response, "sell_limits", None)
            if sell_limits is not None:
                raw = getattr(sell_limits, "sell_max_lots", None)
                if raw is not None:
                    try:
                        sell_max = int(raw)
                    except (TypeError, ValueError):
                        logging.error(f"[max_lots] {figi}: sell_max_lots={raw!r} — None")
                        sell_max = None

            buy_limits = getattr(response, "buy_limits", None)
            if buy_limits is not None:
                raw = getattr(buy_limits, "buy_max_lots", None)
                if raw is not None:
                    try:
                        buy_max = int(raw)
                    except (TypeError, ValueError):
                        logging.error(f"[max_lots] {figi}: buy_max_lots={raw!r} — None")
                        buy_max = None

            if sell_max is None and buy_max is None:
                logging.error(f"[max_lots] {figi}: оба лимита None — неполный ответ")
                return None

            return {"sell_max_lots": sell_max, "buy_max_lots": buy_max}
        except Exception as e:
            logging.error(f"[max_lots] {figi}: {type(e).__name__}: {e}")
            return None

    async def get_positions_debug(self):
        """ДИАГНОСТИКА — не используется в торговой логике. Без изменений."""
        try:
            portfolio = await self.client.sandbox.get_sandbox_portfolio(account_id=self.account_id)
            result = []
            for pos in portfolio.positions:
                info = await self.get_instrument_info(pos.figi) if pos.figi else None
                q_raw = getattr(pos, "quantity", None)
                ql_raw = getattr(pos, "quantity_lots", None)
                try:
                    q_dec = float(quotation_to_decimal(q_raw)) if q_raw is not None else None
                except Exception:
                    q_dec = None
                try:
                    ql_dec = float(quotation_to_decimal(ql_raw)) if ql_raw is not None else None
                except Exception:
                    ql_dec = None
                result.append({
                    "figi": pos.figi,
                    "ticker": getattr(pos, "ticker", None),
                    "instrument_type": getattr(pos, "instrument_type", None),
                    "quantity_raw": str(q_raw),
                    "quantity_lots_raw": str(ql_raw),
                    "quantity_dec": q_dec,
                    "quantity_lots_dec": ql_dec,
                    "lot_size": info["lot"] if info else None,
                })
            return result
        except Exception as e:
            logging.error(f"Ошибка диагностики позиций: {type(e).__name__}: {e}")
            return []

    async def post_market_order(self, figi, quantity, direction, order_id: Optional[str] = None) -> OrderResult:
        """
        Отправляет рыночную заявку.
        Возвращает OrderResult(order_id, state, error).
        order_id сохраняется ДАЖЕ при исключении (потеря ответа).
        Вызывающий код НЕ должен повторно отправлять заявку при state=None.

        Этап 5: order_id может быть передан вызывающим кодом — он генерируется
        ДО записи INTENT в OrderLedger, поэтому намерение и фактическая заявка
        гарантированно имеют один и тот же идентификатор. При order_id=None
        идентификатор генерируется здесь (прежнее поведение, совместимость).
        """
        if not order_id:
            order_id = str(uuid.uuid4())
        order_id = str(order_id)
        try:
            response = await self.client.sandbox.post_sandbox_order(
                account_id=self.account_id, figi=figi, quantity=quantity,
                direction=direction, order_type=OrderType.ORDER_TYPE_MARKET,
                order_id=order_id,
            )
            return OrderResult(order_id=order_id, state=response, error=None)
        except Exception as e:
            logging.error(f"Ошибка заявки {figi} (order_id={order_id}): {type(e).__name__}: {e}")
            self.uncertain_state = True
            return OrderResult(order_id=order_id, state=None, error=e)

    async def get_sandbox_order_state(self, order_id: str):
        """
        Статус заявки по order_id (источник истины — брокер).

        Возвращает OrderState при успехе, None при ошибке/недоступности.
        ВАЖНО (согласовано с владельцем): None — это «статус неизвестен».
        Ответы «заявка не найдена» и сетевые ошибки НЕ являются доказательством
        того, что заявка не отправлена: вызывающий код обязан трактовать None
        как UNCERTAIN и разрешать отказ только по терминальному статусу
        с executed_lots == 0, полученному от брокера.
        Таймаут — API_REQUEST_TIMEOUT_SECONDS (защита от зависания цикла).
        """
        try:
            return await asyncio.wait_for(
                self.client.sandbox.get_sandbox_order_state(
                    account_id=self.account_id, order_id=order_id,
                ),
                timeout=config.API_REQUEST_TIMEOUT_SECONDS,
            )
        except Exception as e:
            logging.error(f"Ошибка статуса заявки {order_id}: {type(e).__name__}: {e}")
            return None

    async def cancel_order(self, order_id: str):
        """
        Отменяет заявку.
        Возвращает CancelOrderResponse или None.
        ВАЖНО: успешный ответ НЕ означает окончательную отмену.
        Вызывающий код должен запросить get_sandbox_order_state(order_id)
        и проверить финальный статус, lots_executed, lots_left.
        """
        try:
            return await self.client.sandbox.cancel_sandbox_order(
                account_id=self.account_id, order_id=order_id,
            )
        except Exception as e:
            logging.error(f"Ошибка отмены заявки {order_id}: {type(e).__name__}: {e}")
            return None

    def is_uncertain(self) -> bool:
        return self.uncertain_state