# execution.py
# Tsclp_v2.2_volatile — мониторинг позиций.
# Полная замена: совместимость с API Samsung + per-order учёт SELL.
#
# ИЗМЕНЕНИЯ (исправление потери первичной регистрации BUY — guard
# register_executed_buy):
#   - ExecutionMonitor хранит компактный реестр closed_entry_order_ids —
#     entry_order_id закрытых BUY-позиций. Обоснование: monitor.positions
#     держит только одну запись на FIGI; register()/register_recovery()
#     перезаписывают CLOSED-запись новой позицией по тому же FIGI, и после
#     перезапуска невозможно было отличить «старую закрытую сделку» от
#     «новой незарегистрированной позиции». Без этого признака ослабление
#     guard первичной регистрации (ledger FILL больше не считается
#     доказательством применения лотов) открывало бы «воскрешение»
#     закрытой сделки по позднему residual-отчёту. Реестр:
#     - пополняется при перезаписи CLOSED-записи (register/register_recovery);
#     - пополняется при экспорте state (все текущие CLOSED-записи);
#     - сериализуется в state.json (export_state/import_state),
#       обратная совместимость: ключ отсутствует в старом state.json —
#       восстанавливается пустой список;
#     - НЕ ограничен по размеру (дополнительный этап): прежний FIFO-лимит
#       200 записей вытеснял признаки СТАРЫХ закрытых сделок — после 200
#       закрытий поздний residual-отчёт вытесненной заявки больше не
#       блокировался guard'ом register_executed_buy («воскрешение»
#       закрытой позиции). Каждая запись — только короткий order_id,
#       размер архива пренебрежим: безопасность важнее экономии байт.
#   Торговая логика (SL/TP, закрытия, промоция) не менялась.
#
# ИЗМЕНЕНИЯ (этап BUY REGISTRATION FIX 3 — защита на уровне execution.py):
#   apply_late_entry_lots() и record_recovery_entry_lots() отклоняют
#   исполнение с ПУСТЫМ/ОТСУТСТВУЮЩИМ order_id (None, "", только пробелы):
#   немедленный return False БЕЗ каких-либо изменений позиции (quantity,
#   entry_price, entry_applied_lots, entry_order_id не трогаются).
#   Обоснование: отсутствие достоверного идентификатора — НЕОПРЕДЕЛЁННОСТЬ,
#   а не разрешение применить исполнение. Раньше проверка принадлежности
#   (`if order_id and pos.entry_order_id and order_id != pos.entry_order_id`)
#   при пустом order_id пропускалась, и дельта применялась к существующей
#   позиции без какого-либо подтверждения, что исполнение относится именно
#   к этой позиции. Теперь:
#   - пустой/missing order_id — отклонение (лог + return False);
#   - чужой (несовпадающий с entry_order_id) order_id — отклонение
#     (как и прежде);
#   - собственный корректный order_id — штатное применение дельты
#     (как и прежде).
#   Основная защита — в вызывающем коде main.py (register_executed_buy:
#   guard BUY_EXEC_NO_ORDER_ID, uncertain_state, диагностика); эти проверки
#   — защита в глубину на уровне самого модуля: даже при обходе вызывающего
#   кода метод не применит лоты без идентификатора. Совместимость: все
#   текущие вызовы (main.py: register_executed_buy OPEN/RECOVERY-ветки,
#   promote_recovery_positions) передают непустой order_id, проверенный
#   вызывающим кодом; поведение остальных методов не менялось.
#
# ИЗМЕНЕНИЯ (v2.3.0-stage1 — телеметрия MAE/MFE и безубыток/трейлинг, С1):
#   TrackedPosition дополнен полями (в КОНЕЦ dataclass — порядок прежних
#   полей не менялся; обратная совместимость со старым state.json через
#   from_dict/setdefault):
#   - initial_sl_price / initial_tp_price — SL/TP на момент входа (плановый
#     риск R); инициализация ровно в двух точках: register() и
#     promote_recovery();
#   - mae_price / mfe_price — худшая/лучшая цена с момента входа: ТОЛЬКО
#     по high/low 5-минутных свечей (update_extremes), НЕ по точечным
#     ценам опроса — поллинг 30 с пропускает внутрибарные экстремумы;
#   - breakeven_moved — флаг перевода в безубыток.
#   Механика ВЫКЛЮЧЕНА по умолчанию (config.BE_TRAIL_ENABLED=False):
#   _update_sl() — триггер безубытка по r_peak от ДОСТИГНУТОГО максимума
#   (mfe_price, не текущей цены): SL -> entry ± буфер (2*комиссия +
#   проскальзывание); затем трейлинг SL = mfe ∓ TRAIL_ATR_MULTIPLIER*ATR,
#   только в сторону ужесточения и не хуже уровня безубытка. check_all()
#   принимает extremes/atrs (необязательные — старый вызов с одной ценой
#   работает как прежде) и классифицирует причину SL-выхода: SL / BE / TRAIL.
import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, List, Optional

import config


class PositionState(str, Enum):
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    UNCERTAIN = "UNCERTAIN"
    RECOVERY = "RECOVERY"


ACTIVE_STATES = (
    PositionState.OPEN, PositionState.CLOSING,
    PositionState.UNCERTAIN, PositionState.RECOVERY,
)


@dataclass
class TrackedPosition:
    figi: str
    ticker: str
    direction: str
    entry_price: float
    quantity: int
    sl_price: float
    tp_price: float
    lot_size: int = 1
    state: PositionState = PositionState.OPEN
    close_reason: Optional[str] = None
    close_order_id: Optional[str] = None
    close_requested_at: Optional[str] = None
    close_price_before_order: float = 0.0
    executed_lots: int = 0
    execution_price: Optional[float] = None
    trade_id: str = ""
    signal_reason: str = ""
    signal_time: str = ""
    price_before_order: float = 0.0
    order_send_time: str = ""
    partial: bool = False
    logged: bool = False
    actual_commission: Optional[float] = None
    entry_actual_commission: Optional[float] = None
    entry_order_id: Optional[str] = None
    entry_requested_lots: int = 0
    entry_applied_lots: int = 0
    entry_snapshot_synced: bool = False
    close_requested_lots: int = 0
    close_applied_lots: int = 0
    recovery_source: Optional[str] = None
    # Новый журнал закрытий. Ключ — точный broker order_id.
    close_orders: Dict[str, dict] = field(default_factory=dict)
    # История применений сохраняется в state.json; записи не перезаписываются.
    close_applications: List[dict] = field(default_factory=list)
    # --- v2.3.0-stage1: телеметрия и безубыток/трейлинг (С1) ---
    # Поля добавлены В КОНЕЦ dataclass: порядок прежних полей не изменился.
    # Для старых state.json значения восстанавливаются в from_dict().
    # SL/TP на момент входа (плановый риск R = entry - initial_sl для BUY).
    initial_sl_price: float = 0.0
    initial_tp_price: float = 0.0
    # Экстремумы с момента входа: худшая цена (BUY: min low; SELL: max high)
    # и лучшая цена (BUY: max high; SELL: min low). 0.0 = «не инициализирован».
    mae_price: float = 0.0
    mfe_price: float = 0.0
    # Флаг перевода в безубыток (триггер — r_peak по достигнутому mfe).
    breakeven_moved: bool = False

    def to_dict(self) -> dict:
        d = asdict(self)
        d["state"] = self.state.value
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "TrackedPosition":
        d = dict(data)
        if isinstance(d.get("state"), str):
            d["state"] = PositionState(d["state"])
        allowed = {f.name for f in cls.__dataclass_fields__.values()}
        d = {k: v for k, v in d.items() if k in allowed}
        # Старые state.json не содержат истории SELL. Не приписываем старый
        # singleton cursor новой заявке: legacy-запись создаётся только для
        # close_order_id, который реально сохранён в state.
        d.setdefault("close_orders", {})
        d.setdefault("close_applications", [])
        # v2.3.0-stage1: старые state.json не содержат телеметрии С1.
        # initial_* восстанавливаются из текущих sl/tp (лучшее доступное
        # приближение для уже открытых позиций), экстремумы стартуют с нуля
        # (начнут накапливаться по свечам), breakeven_moved — False.
        d.setdefault("initial_sl_price", d.get("sl_price") or 0.0)
        d.setdefault("initial_tp_price", d.get("tp_price") or 0.0)
        d.setdefault("mae_price", 0.0)
        d.setdefault("mfe_price", 0.0)
        d.setdefault("breakeven_moved", False)
        return cls(**d)


class ExecutionMonitor:
    # Компактный реестр entry_order_id закрытых BUY-позиций (см. шапку);
    # без лимита размера — обрезка теряла признаки старых закрытых сделок.

    def __init__(self, client):
        self.client = client
        self.positions: Dict[str, TrackedPosition] = {}
        self.monitoring_error = False
        self.closed_entry_order_ids: List[str] = []

    def _archive_closed_entry_order(self, pos: TrackedPosition) -> None:
        """Сохраняет признак закрытой сделки (entry_order_id) перед
        перезаписью CLOSED-записи новой позицией по тому же FIGI."""
        if pos is None or pos.state != PositionState.CLOSED or not pos.entry_order_id:
            return
        if pos.entry_order_id not in self.closed_entry_order_ids:
            self.closed_entry_order_ids.append(pos.entry_order_id)
            # Дополнительный этап: без обрезки архива — вытесненный признак
            # закрытой сделки терял защиту guard'а от «воскрешения».

    def register(self, **kwargs) -> TrackedPosition:
        figi = kwargs.get("figi")
        if figi and self.has_active_position(figi):
            existing = self.positions[figi]
            logging.error(
                f"[execution] register: active position {figi} exists "
                f"({existing.state.value}); overwrite denied"
            )
            return existing
        self._archive_closed_entry_order(self.positions.get(figi))
        pos = TrackedPosition(**kwargs)
        # v2.3.0-stage1 (С1): точка инициализации №1 — SL/TP фиксируются на
        # момент входа (плановый риск для триггера безубытка).
        if pos.initial_sl_price <= 0 and pos.sl_price > 0:
            pos.initial_sl_price = pos.sl_price
        if pos.initial_tp_price <= 0 and pos.tp_price > 0:
            pos.initial_tp_price = pos.tp_price
        if not self._valid_position(pos):
            raise ValueError("Некорректные параметры позиции")
        self.positions[pos.figi] = pos
        logging.info(
            f"[execution] {pos.state.value} {pos.ticker} qty={pos.quantity} "
            f"entry={pos.entry_price} sl={pos.sl_price} tp={pos.tp_price}"
        )
        return pos

    @staticmethod
    def _valid_position(pos):
        return (
            bool(pos.figi) and pos.quantity > 0 and pos.lot_size > 0
            and pos.direction in ("BUY", "SELL")
            and (pos.state == PositionState.RECOVERY or
                 (pos.entry_price > 0 and pos.sl_price > 0 and pos.tp_price > 0))
        )

    def register_recovery(
        self, figi, ticker, direction, quantity, lot_size, trade_id="",
        order_id=None, signal_reason="", signal_time="", price_before_order=0.0,
        order_send_time="", requested_lots=0, partial=False,
        entry_commission=None, execution_price=None, recovery_source="adopted",
    ):
        if self.has_active_position(figi):
            return self.positions[figi]
        if direction not in ("BUY", "SELL") or int(quantity) <= 0 or int(lot_size) <= 0:
            raise ValueError("Некорректная RECOVERY-позиция")
        self._archive_closed_entry_order(self.positions.get(figi))
        pos = TrackedPosition(
            figi=figi, ticker=ticker, direction=direction, entry_price=0.0,
            quantity=int(quantity), sl_price=0.0, tp_price=0.0,
            lot_size=int(lot_size), state=PositionState.RECOVERY,
            executed_lots=int(quantity), trade_id=trade_id,
            signal_reason=signal_reason, signal_time=signal_time,
            price_before_order=price_before_order, order_send_time=order_send_time,
            partial=partial, entry_requested_lots=int(requested_lots or quantity),
            entry_applied_lots=int(quantity), entry_order_id=order_id,
            entry_actual_commission=entry_commission,
            execution_price=execution_price, recovery_source=recovery_source,
        )
        self.positions[figi] = pos
        logging.warning(f"[execution] RECOVERY {ticker} qty={quantity} source={recovery_source}")
        return pos

    def promote_recovery(self, pos, entry_price, sl_price, tp_price, execution_price=None):
        if pos.state != PositionState.RECOVERY:
            return False
        try:
            ep, sl, tp = float(entry_price), float(sl_price), float(tp_price)
        except (TypeError, ValueError):
            return False
        if min(ep, sl, tp) <= 0:
            return False
        pos.entry_price, pos.sl_price, pos.tp_price = ep, sl, tp
        pos.execution_price = float(execution_price) if execution_price and execution_price > 0 else ep
        pos.recovery_source = None
        # v2.3.0-stage1 (С1): точка инициализации №2 — SL/TP фиксируются на
        # момент промоции (цена входа восстановлена), экстремумы стартуют
        # от цены входа (до промоции они не отслеживались — позиция была
        # RECOVERY), флаг безубытка сброшен.
        pos.initial_sl_price = sl
        pos.initial_tp_price = tp
        pos.mae_price = ep
        pos.mfe_price = ep
        pos.breakeven_moved = False
        pos.state = PositionState.OPEN
        return True

    def sync_recovery_quantity(self, pos, broker_lots):
        if pos.state != PositionState.RECOVERY:
            return False
        try:
            lots = int(broker_lots)
        except (TypeError, ValueError):
            return False
        if lots == 0 or (lots > 0) != (pos.direction == "BUY"):
            return False
        absolute = abs(lots)
        if absolute == pos.quantity:
            pos.entry_snapshot_synced = True
            return False
        old = pos.quantity
        pos.quantity = absolute
        pos.entry_snapshot_synced = True
        if absolute > int(pos.entry_applied_lots or 0):
            pos.entry_applied_lots = absolute
            pos.executed_lots = absolute
        logging.info(f"[execution] RECOVERY {pos.ticker}: broker qty {old} -> {absolute}")
        return True

    def record_recovery_entry_lots(self, pos, executed_lots, order_id=None, already_seen_lots=None):
        if pos.state != PositionState.RECOVERY or pos.direction != "BUY":
            return False
        # Этап BUY REGISTRATION FIX 3 (защита в глубину): исполнение БЕЗ
        # достоверного order_id отклоняется целиком — отсутствие
        # идентификатора это НЕОПРЕДЕЛЁННОСТЬ, а НЕ разрешение применить
        # лоты. Позиция не изменяется (quantity/entry_price/
        # entry_applied_lots не трогаются); разрешение неопределённости —
        # вызывающим кодом (uncertain_state + диагностика) и сверкой.
        if not str(order_id or "").strip():
            logging.error(
                f"[execution] record_recovery_entry_lots: отклонено — "
                f"исполнение без достоверного order_id "
                f"(figi={pos.figi}, ticker={pos.ticker}, "
                f"entry_order_id={pos.entry_order_id or '—'}, "
                f"executed_lots={executed_lots}) — лоты НЕ применены"
            )
            return False
        if order_id and pos.entry_order_id and order_id != pos.entry_order_id:
            return False
        try:
            le = int(executed_lots)
            seen = int(already_seen_lots) if already_seen_lots is not None else 0
        except (TypeError, ValueError):
            return False
        if le < 0:
            return False
        applied = int(pos.entry_applied_lots or 0)
        baseline = max(applied, seen)
        if le < baseline:
            return False
        if le == baseline:
            if le == applied:
                return False
            pos.entry_applied_lots = pos.executed_lots = le
            return True
        if pos.entry_snapshot_synced:
            pos.entry_applied_lots = pos.executed_lots = le
            return True
        delta = le - baseline
        pos.quantity += delta
        pos.entry_applied_lots = pos.executed_lots = le
        return True

    def apply_late_entry_lots(self, pos, executed_lots, order_id=None):
        if pos.state != PositionState.OPEN or pos.direction != "BUY":
            return False
        # Этап BUY REGISTRATION FIX 3 (защита в глубину): исполнение БЕЗ
        # достоверного order_id отклоняется целиком — отсутствие
        # идентификатора это НЕОПРЕДЕЛЁННОСТЬ, а НЕ разрешение применить
        # лоты к уже открытой позиции. Позиция не изменяется (quantity/
        # entry_price/entry_applied_lots не трогаются); разрешение
        # неопределённости — вызывающим кодом (uncertain_state +
        # диагностика) и сверкой.
        if not str(order_id or "").strip():
            logging.error(
                f"[execution] apply_late_entry_lots: отклонено — "
                f"исполнение без достоверного order_id "
                f"(figi={pos.figi}, ticker={pos.ticker}, "
                f"entry_order_id={pos.entry_order_id or '—'}, "
                f"executed_lots={executed_lots}) — лоты НЕ применены"
            )
            return False
        if order_id and pos.entry_order_id and order_id != pos.entry_order_id:
            return False
        try:
            le = int(executed_lots)
        except (TypeError, ValueError):
            return False
        applied = int(pos.entry_applied_lots or 0)
        if le <= applied:
            return False
        pos.quantity += le - applied
        pos.entry_applied_lots = pos.executed_lots = le
        return True

    def add_close_order(self, pos, order_id, requested_lots, requested_at=None):
        """Регистрирует отдельную SELL-заявку до/после отправки; повторный ID идемпотентен."""
        if not order_id or int(requested_lots) <= 0:
            raise ValueError("Нужны order_id и положительное requested_lots")
        oid = str(order_id)
        existing = pos.close_orders.get(oid)
        if existing:
            if int(existing["requested_lots"]) != int(requested_lots):
                pos.state = PositionState.UNCERTAIN
                raise ValueError("Один order_id получил разные requested_lots")
            return existing
        rec = {
            "order_id": oid, "trade_id": pos.trade_id, "figi": pos.figi,
            "side": "SELL" if pos.direction == "BUY" else "BUY",
            "requested_lots": int(requested_lots),
            "confirmed_executed_lots": 0, "applied_lots": 0,
            "status": "INTENT", "requested_at": requested_at,
            "avg_exec_price": None, "commission": None,
        }
        pos.close_orders[oid] = rec
        return rec

    def apply_close_execution(
        self, pos, order_id, cumulative_executed_lots, status,
        execution_price=None, commission=None, requested_lots=None,
    ):
        """Применяет только новый cumulative delta именно этой SELL-заявки."""
        oid = str(order_id)
        rec = pos.close_orders.get(oid)
        if rec is None:
            if requested_lots is None:
                pos.state = PositionState.UNCERTAIN
                return False
            rec = self.add_close_order(pos, oid, requested_lots)
        try:
            cumulative = int(cumulative_executed_lots)
        except (TypeError, ValueError):
            pos.state = PositionState.UNCERTAIN
            return False
        requested = int(rec["requested_lots"])
        previous = int(rec["confirmed_executed_lots"])
        applied = int(rec["applied_lots"])
        if cumulative < previous or cumulative < applied or cumulative > requested:
            pos.state = PositionState.UNCERTAIN
            logging.error(
                f"[execution] {pos.ticker}: contradictory SELL cumulative "
                f"{cumulative}; previous={previous}, applied={applied}, requested={requested}"
            )
            return False
        delta = cumulative - applied
        if delta > pos.quantity:
            pos.state = PositionState.UNCERTAIN
            logging.error(f"[execution] {pos.ticker}: SELL delta {delta} exceeds remaining {pos.quantity}")
            return False
        # Записываем подтверждение и применение как отдельные поля одной записи.
        rec["confirmed_executed_lots"] = cumulative
        rec["status"] = str(status)
        if execution_price is not None and execution_price > 0:
            rec["avg_exec_price"] = float(execution_price)
        if commission is not None:
            rec["commission"] = float(commission)
        if delta:
            from_applied = applied
            pos.quantity -= delta
            rec["applied_lots"] = cumulative
            pos.close_applications.append({
                "order_id": oid, "from_applied_lots": from_applied,
                "to_applied_lots": cumulative, "delta_lots": delta,
            })
        else:
            rec["applied_lots"] = applied
        # Совместимые legacy-поля отражают только текущий/последний ордер.
        if pos.close_order_id == oid:
            pos.close_requested_lots = requested
            pos.close_applied_lots = int(rec["applied_lots"])
        pos.executed_lots = cumulative
        pos.execution_price = rec["avg_exec_price"]
        pos.actual_commission = rec["commission"]
        terminal = str(status).upper() in {"FILL", "CANCELLED", "REJECTED", "EXPIRED", "1", "2", "3"}
        full = cumulative == requested
        if pos.quantity == 0:
            pos.state = PositionState.CLOSED
        elif terminal:
            # Частичный терминальный SELL закончен; остаток позиции остаётся OPEN.
            pos.state = PositionState.OPEN
            if pos.close_order_id == oid:
                pos.close_order_id = None
                pos.close_requested_lots = 0
                pos.close_applied_lots = 0
        elif str(status).upper() in {"PARTIALLYFILL", "5"}:
            pos.state = PositionState.UNCERTAIN
        else:
            pos.state = PositionState.CLOSING
        return True

    def get_position(self, figi):
        return self.positions.get(figi)

    def has_active_position(self, figi):
        p = self.positions.get(figi)
        return p is not None and p.state in ACTIVE_STATES

    # --- v2.3.0-stage1 (С1): безубыток/трейлинг (ВЫКЛЮЧЕНО по умолчанию) ---

    @staticmethod
    def _be_buffer(entry_price: float) -> float:
        """Буфер безубытка: компенсация двойной комиссии + проскальзывания."""
        return entry_price * (2 * config.COMMISSION_PER_SIDE + config.SLIPPAGE_PERCENT)

    @staticmethod
    def _risk_per_share(pos) -> float:
        """Плановый риск на акцию: entry - initial_sl (BUY); initial_sl - entry (SELL)."""
        if pos.direction == "BUY":
            return pos.entry_price - pos.initial_sl_price
        return pos.initial_sl_price - pos.entry_price

    def update_extremes(self, pos, high, low) -> bool:
        """Обновляет MAE/MFE по high/low 5-минутной свечи (НЕ по точечным
        ценам опроса: поллинг 30 с пропускает внутрибарные экстремумы).
        BUY:  mfe = max(high), mae = min(low). SELL: зеркально.
        Только для OPEN-позиций; значение 0.0 трактуется как «не инициализирован»."""
        if pos.state != PositionState.OPEN:
            return False
        try:
            high = float(high) if high is not None else None
            low = float(low) if low is not None else None
        except (TypeError, ValueError):
            return False
        changed = False
        if pos.direction == "BUY":
            if high and high > 0 and (pos.mfe_price <= 0 or high > pos.mfe_price):
                pos.mfe_price = high
                changed = True
            if low and low > 0 and (pos.mae_price <= 0 or low < pos.mae_price):
                pos.mae_price = low
                changed = True
        else:
            if low and low > 0 and (pos.mfe_price <= 0 or low < pos.mfe_price):
                pos.mfe_price = low
                changed = True
            if high and high > 0 and (pos.mae_price <= 0 or high > pos.mae_price):
                pos.mae_price = high
                changed = True
        return changed

    def _update_sl(self, pos, atr) -> None:
        """Триггер безубытка и трейлинг (С1).

        Триггер — по ДОСТИГНУТОМУ максимуму (r_peak от mfe_price, не от
        текущей цены): при r_peak >= BE_TRIGGER_R SL переносится на
        entry ± буфер (2*комиссия + проскальзывание). После перевода —
        трейлинг: SL = mfe ∓ TRAIL_ATR_MULTIPLIER*ATR, только в сторону
        ужесточения и не хуже уровня безубытка. SL никогда не ослабляется.
        Работает только при config.BE_TRAIL_ENABLED=True (по умолчанию
        выключено — поведение системы идентично прежнему)."""
        if not getattr(config, "BE_TRAIL_ENABLED", False):
            return
        if pos.state != PositionState.OPEN:
            return
        risk = self._risk_per_share(pos)
        if risk <= 0:
            return
        buffer = self._be_buffer(pos.entry_price)
        if pos.direction == "BUY":
            # --- триггер безубытка ---
            if not pos.breakeven_moved and pos.mfe_price > 0:
                r_peak = (pos.mfe_price - pos.entry_price) / risk
                if r_peak >= config.BE_TRIGGER_R:
                    new_sl = pos.entry_price + buffer
                    if new_sl > pos.sl_price:
                        logging.info(
                            f"[execution] {pos.ticker}: BE trigger r_peak={r_peak:.2f} "
                            f"(mfe={pos.mfe_price:.4f}) — SL {pos.sl_price:.4f} -> {new_sl:.4f}"
                        )
                        pos.sl_price = new_sl
                    pos.breakeven_moved = True
            # --- трейлинг от достигнутого максимума ---
            if pos.breakeven_moved and atr and atr > 0 and pos.mfe_price > 0:
                candidate = pos.mfe_price - config.TRAIL_ATR_MULTIPLIER * float(atr)
                if candidate > pos.sl_price and candidate > pos.entry_price + buffer:
                    logging.info(
                        f"[execution] {pos.ticker}: TRAIL — SL {pos.sl_price:.4f} -> {candidate:.4f} "
                        f"(mfe={pos.mfe_price:.4f}, atr={float(atr):.4f})"
                    )
                    pos.sl_price = candidate
        else:
            # --- зеркало для SHORT (в текущей стратегии не используются,
            # но учёт поддерживает адопцию SHORT-позиций) ---
            if not pos.breakeven_moved and pos.mfe_price > 0:
                r_peak = (pos.entry_price - pos.mfe_price) / risk
                if r_peak >= config.BE_TRIGGER_R:
                    new_sl = pos.entry_price - buffer
                    if new_sl < pos.sl_price:
                        pos.sl_price = new_sl
                    pos.breakeven_moved = True
            if pos.breakeven_moved and atr and atr > 0 and pos.mfe_price > 0:
                candidate = pos.mfe_price + config.TRAIL_ATR_MULTIPLIER * float(atr)
                if candidate < pos.sl_price and candidate < pos.entry_price - buffer:
                    pos.sl_price = candidate

    def _classify_sl_exit(self, pos) -> str:
        """Причина SL-выхода для журнала: SL (штатный стоп) / BE (безубыток)
        / TRAIL (трейлинг выше уровня безубытка)."""
        if not pos.breakeven_moved:
            return "SL"
        buffer = self._be_buffer(pos.entry_price)
        if pos.direction == "BUY":
            return "TRAIL" if pos.sl_price > pos.entry_price + buffer else "BE"
        return "TRAIL" if pos.sl_price < pos.entry_price - buffer else "BE"

    async def check_all(self, prices: Dict[str, float], extremes=None, atrs=None) -> List[TrackedPosition]:
        """Проверка SL/TP по всем OPEN-позициям.

        v2.3.0-stage1: extremes — {figi: (high, low)} последней 5-мин свечи,
        atrs — {figi: ATR}. Порядок обработки позиции: экстремумы ->
        триггер BE/трейлинг (только при BE_TRAIL_ENABLED=True) -> проверка
        SL/TP. При срабатывании стопа причина классифицируется (SL/BE/TRAIL).
        Вызов с одним аргументом (только цены) работает как прежде."""
        to_close = []
        self.monitoring_error = False
        if prices is None or not prices:
            self.monitoring_error = True
            return to_close
        for pos in list(self.positions.values()):
            if pos.state != PositionState.OPEN:
                continue
            price = prices.get(pos.figi)
            if price is None or price <= 0:
                self.monitoring_error = True
                continue
            # С1: свечные экстремумы -> триггер BE/трейлинг -> SL/TP
            if extremes:
                candle = extremes.get(pos.figi)
                if candle:
                    self.update_extremes(pos, candle[0], candle[1])
            if getattr(config, "BE_TRAIL_ENABLED", False):
                self._update_sl(pos, (atrs or {}).get(pos.figi))
            if pos.direction == "BUY":
                hit_tp = price >= pos.tp_price
                hit_sl = price <= pos.sl_price
            else:
                hit_tp = price <= pos.tp_price
                hit_sl = price >= pos.sl_price
            if hit_tp:
                reason = "TP"
            elif hit_sl:
                reason = self._classify_sl_exit(pos)
            else:
                reason = None
            if reason:
                pos.state = PositionState.CLOSING
                pos.close_reason = reason
                pos.close_price_before_order = float(price)
                to_close.append(pos)
        return to_close

    async def resolve_closing(self, pos):
        """Проверяет активный close_order_id. История предыдущих SELL сохраняется."""
        oid = pos.close_order_id
        if not oid:
            pos.state = PositionState.UNCERTAIN
            return
        try:
            state = await self.client.get_sandbox_order_state(oid)
            if state is None:
                pos.state = PositionState.UNCERTAIN
                return
            raw = getattr(state, "execution_report_status", None)
            try:
                status_int = int(raw) if raw is not None else -1
            except (TypeError, ValueError):
                status_int = -1
            status = {1: "FILL", 2: "REJECTED", 3: "CANCELLED", 4: "NEW", 5: "PARTIALLYFILL"}.get(status_int, str(raw))
            lots = int(getattr(state, "lots_executed", 0) or 0)
            requested = int(pos.close_requested_lots or pos.quantity)
            price = self._extract_price(state, lots, pos.lot_size)
            commission = self._extract_commission(state)
            if oid not in pos.close_orders:
                # Миграция legacy-позиции: старый cursor нельзя безопасно
                # переносить на неизвестную новую заявку. Он применим только
                # к order_id, записанному в state.json.
                self.add_close_order(pos, oid, requested, pos.close_requested_at)
                legacy_applied = int(pos.close_applied_lots or 0)
                if legacy_applied:
                    # В старом формате cursor мог относиться к предыдущей SELL:
                    # доказать его принадлежность текущему order_id невозможно.
                    # Не переносим его и не списываем повторно — fail closed.
                    pos.state = PositionState.UNCERTAIN
                    logging.error(
                        f"[execution] {pos.ticker}: legacy close_applied_lots="
                        f"{legacy_applied} без per-order истории; требуется reconcile"
                    )
                    return
            self.apply_close_execution(pos, oid, lots, status, price, commission, requested)
            if status_int == 5 and pos.state != PositionState.CLOSED:
                pos.state = PositionState.UNCERTAIN
            elif status_int in (2, 3) and lots == 0 and pos.state != PositionState.CLOSED:
                pos.state = PositionState.OPEN
                pos.close_order_id = None
                pos.close_requested_lots = 0
                pos.close_applied_lots = 0
            elif status_int == 1 and lots < requested and pos.state != PositionState.CLOSED:
                pos.state = PositionState.UNCERTAIN
        except Exception as exc:
            logging.error(f"[execution] resolve_closing {pos.ticker}: {type(exc).__name__}: {exc}")
            pos.state = PositionState.UNCERTAIN

    def resolve_uncertain(self, pos, broker_positions):
        broker = broker_positions.get(pos.figi)
        if broker is None:
            pos.quantity = 0
            pos.state = PositionState.CLOSED
            return
        if getattr(broker, "direction", "UNKNOWN") == "UNKNOWN":
            pos.state = PositionState.UNCERTAIN
            return
        expected = "LONG" if pos.direction == "BUY" else "SHORT"
        if broker.direction != expected or abs(int(broker.lots)) != pos.quantity:
            pos.state = PositionState.UNCERTAIN
            return
        pos.quantity = abs(int(broker.lots))
        pos.state = PositionState.OPEN

    @staticmethod
    def _extract_price(state, lots_executed, lot_size):
        try:
            ep = getattr(state, "executed_order_price", None)
            if ep is None or lots_executed <= 0 or lot_size <= 0:
                return None
            total = float(ep.units) + float(ep.nano) / 1e9
            return total / (lots_executed * lot_size)
        except Exception:
            return None

    @staticmethod
    def _extract_commission(state):
        try:
            ec = getattr(state, "executed_commission", None)
            if ec is None:
                return None
            value = abs(float(ec.units) + float(ec.nano) / 1e9)
            return value if value > 0 else None
        except Exception:
            return None

    def active_count(self):
        return sum(p.state in ACTIVE_STATES for p in self.positions.values())

    def export_state(self):
        # Компактный реестр закрытых входных заявок: архив перезаписанных
        # записей + все текущие CLOSED-записи (дедуп; БЕЗ лимита размера —
        # обрезка теряла признаки старых закрытых сделок, см. шапку).
        closed_ids = list(self.closed_entry_order_ids)
        for pos in self.positions.values():
            if pos.state == PositionState.CLOSED and pos.entry_order_id:
                if pos.entry_order_id not in closed_ids:
                    closed_ids.append(pos.entry_order_id)
        return {
            "positions": {figi: pos.to_dict() for figi, pos in self.positions.items()},
            "monitoring_error": self.monitoring_error,
            "closed_entry_order_ids": closed_ids,
        }

    def import_state(self, data):
        if not isinstance(data, dict):
            logging.error("[execution] import_state: data is not dict")
            return
        # Обратная совместимость: старый state.json без ключа -> пустой список.
        raw_closed = data.get("closed_entry_order_ids", [])
        if isinstance(raw_closed, list):
            # Дополнительный этап: полный импорт без обрезки — все признаки
            # закрытых сделок должны переживать перезапуск.
            self.closed_entry_order_ids = [str(x) for x in raw_closed if x]
        else:
            self.closed_entry_order_ids = []
        restored = {}
        positions = data.get("positions", {})
        if not isinstance(positions, dict):
            logging.error("[execution] import_state: positions is not dict")
            return
        for figi, raw in positions.items():
            try:
                pos = TrackedPosition.from_dict(raw)
                if pos.figi != figi:
                    raise ValueError("FIGI key mismatch")
                # Validate persisted close journal; malformed journal fails closed.
                if not isinstance(pos.close_orders, dict) or not isinstance(pos.close_applications, list):
                    raise ValueError("invalid close journal")
                restored[figi] = pos
            except Exception as exc:
                logging.error(f"[execution] import_state {figi}: {type(exc).__name__}: {exc}")
        self.positions = restored
        self.monitoring_error = bool(data.get("monitoring_error", False))
