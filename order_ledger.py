# order_ledger.py
# Durable append-only order journal with per-order execution application history.
# Backward-compatible with the public API used by the current main.py.

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

STATUS_INTENT = "INTENT"
STATUS_SENT = "SENT"
STATUS_NEW = "NEW"
STATUS_PARTIALLYFILL = "PARTIALLYFILL"
STATUS_FILL = "FILL"
STATUS_CANCELLED = "CANCELLED"
STATUS_REJECTED = "REJECTED"
STATUS_EXPIRED = "EXPIRED"
STATUS_UNCERTAIN = "UNCERTAIN"

TERMINAL_STATUSES = {STATUS_FILL, STATUS_CANCELLED, STATUS_REJECTED, STATUS_EXPIRED}
NON_TERMINAL_STATUSES = {STATUS_INTENT, STATUS_SENT, STATUS_NEW, STATUS_PARTIALLYFILL, STATUS_UNCERTAIN}
DEAD_WITHOUT_EXECUTION = {STATUS_REJECTED, STATUS_CANCELLED, STATUS_EXPIRED}

RECORD_FIELDS = [
    "timestamp_utc", "status", "order_id", "trade_id", "figi", "ticker",
    "side", "requested_lots", "executed_lots", "lots_left",
    "avg_exec_price", "reason", "source", "error", "created_at_utc", "updated_at_utc",
]
IDENTITY_FIELDS = ("order_id", "trade_id", "figi", "side", "requested_lots")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _positive_int(value, field: str, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field}: bool is not an integer quantity")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(f"{field}: invalid integer {value!r}")
    if result < (0 if allow_zero else 1) or str(value).strip() not in (str(result), f"+{result}"):
        raise ValueError(f"{field}: invalid quantity {value!r}")
    return result


class OrderLedger:
    """Append-only JSONL journal. Status snapshots and application events share one file."""

    def __init__(self, path: str):
        self.path = path
        self._latest: Dict[str, dict] = {}
        self._reliable = True
        self._unreliable_reason = ""
        self._corrupt_tail_truncated = False  # compatibility with prior implementation
        self._applications: Dict[str, dict] = {}  # order_id -> application cursor/history
        self._prepared: Dict[str, dict] = {}      # apply_id -> immutable prepared record
        self._committed = set()

    def is_reliable(self) -> bool:
        return self._reliable

    @property
    def unreliable_reason(self) -> str:
        return self._unreliable_reason

    def _mark_unreliable(self, reason: str) -> None:
        if self._reliable:
            self._reliable = False
            self._unreliable_reason = reason
            logging.error("[ledger] РЕЕСТР НЕДОСТОВЕРЕН: %s. Новые BUY должны быть заблокированы.", reason)

    @staticmethod
    def _normalize(rec: dict) -> dict:
        out = {k: rec.get(k) for k in RECORD_FIELDS}
        out["order_id"] = str(rec.get("order_id"))
        out["status"] = str(rec.get("status"))
        ts = rec.get("timestamp_utc")
        out["created_at_utc"] = rec.get("created_at_utc") or ts
        out["updated_at_utc"] = rec.get("updated_at_utc") or ts
        return out

    def load(self) -> None:
        self._latest, self._applications, self._prepared, self._committed = {}, {}, {}, set()
        self._reliable, self._unreliable_reason = True, ""
        self._corrupt_tail_truncated = False
        if not os.path.isfile(self.path):
            return
        try:
            with open(self.path, "rb") as f:
                raw = f.read()
        except Exception as exc:
            self._mark_unreliable(f"ошибка чтения {self.path}: {type(exc).__name__}: {exc}")
            return
        lines = raw.splitlines(keepends=True)
        for index, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                rec = json.loads(line.decode("utf-8").strip())
                if not isinstance(rec, dict):
                    raise ValueError("record is not an object")
                kind = rec.get("record_type", "ORDER_STATUS")
                if kind == "APPLY_PREPARED":
                    self._replay_prepared(rec)
                elif kind == "APPLY_COMMITTED":
                    self._replay_committed(rec)
                elif kind == "ORDER_STATUS":
                    if "status" not in rec or "order_id" not in rec:
                        raise ValueError("missing status/order_id")
                    oid = str(rec["order_id"])
                    norm = self._normalize(rec)
                    prev = self._latest.get(oid)
                    if prev and prev.get("created_at_utc"):
                        norm["created_at_utc"] = prev["created_at_utc"]
                    self._latest[oid] = norm
                else:
                    raise ValueError(f"unknown record_type={kind!r}")
            except Exception as exc:
                where = "хвост" if index == len(lines) - 1 else "середина"
                self._corrupt_tail_truncated = where == "хвост"
                self._mark_unreliable(f"повреждённая строка ({where}): {type(exc).__name__}: {exc}")
        # A prepared operation without commit is expected after a crash and is recoverable.

    def _append_raw(self, record: dict) -> bool:
        try:
            parent = os.path.dirname(os.path.abspath(self.path))
            os.makedirs(parent, exist_ok=True)
            payload = (json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            with open(self.path, "ab") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            return True
        except Exception as exc:
            logging.error("[ledger] НЕ ЗАПИСАНО: %s: %s", type(exc).__name__, exc)
            return False

    def append(self, status: str, order_id: Optional[str], **fields) -> bool:
        oid = str(order_id) if order_id is not None else None
        prev = self._latest.get(oid) if oid is not None else None
        now = utc_now_iso()
        record = {"record_type": "ORDER_STATUS", "timestamp_utc": now, "status": str(status), "order_id": oid}
        for key in ("trade_id", "figi", "ticker", "side", "requested_lots", "executed_lots", "lots_left", "avg_exec_price", "reason", "source", "error"):
            value = fields.get(key)
            if value is None and prev is not None and key in ("trade_id", "figi", "ticker", "side", "requested_lots"):
                value = prev.get(key)
            record[key] = value
        # Even legacy callers cannot silently move cumulative execution backwards
        # or beyond the immutable requested quantity.
        if prev is not None and fields.get("executed_lots") is not None and status != STATUS_UNCERTAIN:
            try:
                incoming = _positive_int(fields.get("executed_lots"), "executed_lots", allow_zero=True)
                previous = _positive_int(prev.get("executed_lots") or 0, "previous executed_lots", allow_zero=True)
                requested = _positive_int(record.get("requested_lots"), "requested_lots")
                if incoming < previous or incoming > requested:
                    record["status"] = STATUS_UNCERTAIN
                    record["executed_lots"] = previous
                    record["reason"] = (str(fields.get("reason") or "") +
                        f"; cumulative execution contradiction {previous}->{incoming}, requested={requested}").strip("; ")
            except ValueError as exc:
                record["status"] = STATUS_UNCERTAIN
                record["reason"] = (str(fields.get("reason") or "") + f"; invalid execution quantity: {exc}").strip("; ")
                record["executed_lots"] = prev.get("executed_lots") or 0
        record["created_at_utc"] = (prev or {}).get("created_at_utc") or now
        record["updated_at_utc"] = now
        if not self._append_raw(record):
            return False
        if oid is not None:
            self._latest[oid] = self._normalize(record)
        return True

    def record_intent(self, order_id: str, *, trade_id: str, figi: str, ticker: str,
                      side: str, requested_lots: int, reason: str = "") -> bool:
        try:
            _positive_int(requested_lots, "requested_lots")
            if not all((order_id, trade_id, figi, ticker)) or side not in ("BUY", "SELL"):
                raise ValueError("missing identity field or invalid side")
        except ValueError as exc:
            logging.error("[ledger] invalid INTENT: %s", exc)
            return False
        if str(order_id) in self._latest:
            self._mark_unreliable(f"повторный INTENT для order_id={order_id}")
            return False
        return self.append(STATUS_INTENT, order_id, trade_id=trade_id, figi=figi,
                           ticker=ticker, side=side, requested_lots=requested_lots,
                           reason=reason, source="pre_send")

    def record_sent(self, order_id: str, *, trade_id: str = None, figi: str = None,
                    ticker: str = None, side: str = None, requested_lots: int = None,
                    executed_lots: int = None, avg_exec_price: float = None,
                    status_hint: str = None, reason: str = "", source: str = "post_response",
                    error: str = None) -> bool:
        status = STATUS_SENT if status_hint is None else status_hint
        if status == STATUS_FILL:
            try:
                valid = executed_lots is not None and _positive_int(executed_lots, "executed_lots") > 0
            except ValueError:
                valid = False
            if not valid:
                return self.record_uncertain(order_id, trade_id=trade_id, figi=figi, ticker=ticker,
                    side=side, requested_lots=requested_lots, executed_lots=executed_lots,
                    reason="FILL без подтверждённого executed_lots > 0", source=source, error=error)
        return self.append(status, order_id, trade_id=trade_id, figi=figi, ticker=ticker,
                           side=side, requested_lots=requested_lots, executed_lots=executed_lots,
                           avg_exec_price=avg_exec_price, reason=reason, source=source, error=error)

    def record_status(self, order_id: str, status: str, *, trade_id: str = None,
                      figi: str = None, ticker: str = None, side: str = None,
                      requested_lots: int = None, executed_lots: int = None,
                      lots_left: int = None, avg_exec_price: float = None,
                      reason: str = "", source: str = "", error: str = None) -> bool:
        return self.append(status, order_id, trade_id=trade_id, figi=figi, ticker=ticker,
            side=side, requested_lots=requested_lots, executed_lots=executed_lots,
            lots_left=lots_left, avg_exec_price=avg_exec_price, reason=reason, source=source, error=error)

    def record_uncertain(self, order_id: str, *, trade_id: str = None, figi: str = None,
                         ticker: str = None, side: str = None, requested_lots: int = None,
                         executed_lots: int = None, avg_exec_price: float = None,
                         reason: str = "", source: str = "", error: str = None) -> bool:
        return self.append(STATUS_UNCERTAIN, order_id, trade_id=trade_id, figi=figi,
            ticker=ticker, side=side, requested_lots=requested_lots, executed_lots=executed_lots,
            avg_exec_price=avg_exec_price, reason=reason, source=source, error=error)

    # ---- New exact per-order cumulative execution / apply protocol ----
    def record_observation(self, order_id: str, *, executed_lots: int, status: str,
                           avg_exec_price: float = None, source: str = "broker") -> bool:
        """Record monotonic broker cumulative execution. Contradictions fail closed."""
        current = self._latest.get(str(order_id))
        if current is None:
            self._mark_unreliable(f"observation для неизвестного order_id={order_id}")
            return False
        try:
            observed = _positive_int(executed_lots, "executed_lots", allow_zero=True)
            requested = _positive_int(current.get("requested_lots"), "requested_lots")
            previous = _positive_int(current.get("executed_lots") or 0, "previous executed_lots", allow_zero=True)
            if observed < previous or observed > requested:
                raise ValueError(f"cumulative execution regression/overflow: {previous}->{observed}, requested={requested}")
        except ValueError as exc:
            self.record_uncertain(order_id, reason=str(exc), source=source,
                                  executed_lots=current.get("executed_lots"))
            return False
        return self.record_status(order_id, status, executed_lots=observed,
                                  avg_exec_price=avg_exec_price, source=source)

    @staticmethod
    def _make_apply_id(order_id: str, trade_id: str, figi: str, side: str,
                       from_lots: int, to_lots: int) -> str:
        raw = "|".join((str(order_id), str(trade_id), str(figi), str(side), str(from_lots), str(to_lots)))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def prepare_application(self, order_id: str, *, trade_id: str, figi: str, side: str,
                            from_applied_lots: int, to_applied_lots: int) -> Optional[str]:
        """Durably prepare one exact delta before the caller persists position state."""
        current = self._latest.get(str(order_id))
        try:
            if current is None:
                raise ValueError("unknown order_id")
            if (current.get("trade_id"), current.get("figi"), current.get("side")) != (trade_id, figi, side):
                raise ValueError("order/trade/FIGI/side identity mismatch")
            start = _positive_int(from_applied_lots, "from_applied_lots", allow_zero=True)
            end = _positive_int(to_applied_lots, "to_applied_lots", allow_zero=True)
            observed = _positive_int(current.get("executed_lots") or 0, "observed executed_lots", allow_zero=True)
            if end <= start or end > observed:
                raise ValueError(f"invalid application range {start}->{end}; observed={observed}")
            state = self._applications.get(str(order_id), {"applied_lots": 0})
            cursor = int(state.get("applied_lots", 0))
            pending_for_order = [x for x in self._prepared.values()
                                 if x.get("order_id") == str(order_id) and x["apply_id"] not in self._committed]
            if pending_for_order:
                pending = pending_for_order[-1]
                if pending["from_applied_lots"] == start and pending["to_applied_lots"] == end:
                    return pending["apply_id"]
                raise ValueError("another uncommitted application exists for this order")
            if start != cursor:
                raise ValueError(f"application cursor mismatch: journal={cursor}, requested_from={start}")
            apply_id = self._make_apply_id(str(order_id), trade_id, figi, side, start, end)
            rec = {"record_type": "APPLY_PREPARED", "timestamp_utc": utc_now_iso(),
                   "apply_id": apply_id, "order_id": str(order_id), "trade_id": trade_id,
                   "figi": figi, "side": side, "from_applied_lots": start, "to_applied_lots": end}
            if not self._append_raw(rec):
                return None
            self._prepared[apply_id] = rec
            return apply_id
        except (TypeError, ValueError) as exc:
            logging.error("[ledger] application prepare rejected for %s: %s", order_id, exc)
            return None

    def commit_application(self, apply_id: str) -> bool:
        """Commit only a previously prepared operation; caller must persist state first."""
        rec = self._prepared.get(str(apply_id))
        if rec is None:
            self._mark_unreliable(f"commit без APPLY_PREPARED: {apply_id}")
            return False
        if str(apply_id) in self._committed:
            return True
        oid = rec["order_id"]
        state = self._applications.get(oid, {"applied_lots": 0})
        cursor = int(state.get("applied_lots", 0))
        if cursor == rec["to_applied_lots"]:
            # State may already have been persisted before a crash; commit is safe.
            pass
        elif cursor != rec["from_applied_lots"]:
            self._mark_unreliable(f"application commit cursor conflict order_id={oid}: {cursor}")
            return False
        event = {"record_type": "APPLY_COMMITTED", "timestamp_utc": utc_now_iso(),
                 "apply_id": str(apply_id), "order_id": oid,
                 "from_applied_lots": rec["from_applied_lots"], "to_applied_lots": rec["to_applied_lots"]}
        if not self._append_raw(event):
            return False
        self._committed.add(str(apply_id))
        self._applications[oid] = {"applied_lots": rec["to_applied_lots"],
                                   "last_apply_id": str(apply_id)}
        return True

    def _replay_prepared(self, rec: dict) -> None:
        required = ("apply_id", "order_id", "trade_id", "figi", "side", "from_applied_lots", "to_applied_lots")
        if any(k not in rec for k in required):
            raise ValueError("incomplete APPLY_PREPARED")
        expected = self._make_apply_id(rec["order_id"], rec["trade_id"], rec["figi"], rec["side"],
                                       int(rec["from_applied_lots"]), int(rec["to_applied_lots"]))
        if expected != rec["apply_id"]:
            raise ValueError("apply_id hash mismatch")
        old = self._prepared.get(rec["apply_id"])
        if old is not None and old != rec:
            raise ValueError("conflicting duplicate APPLY_PREPARED")
        self._prepared[rec["apply_id"]] = rec

    def _replay_committed(self, rec: dict) -> None:
        aid = str(rec.get("apply_id"))
        prepared = self._prepared.get(aid)
        if prepared is None:
            raise ValueError("APPLY_COMMITTED without prepared record")
        if (str(rec.get("order_id")) != prepared["order_id"] or
                int(rec.get("from_applied_lots")) != int(prepared["from_applied_lots"]) or
                int(rec.get("to_applied_lots")) != int(prepared["to_applied_lots"])):
            raise ValueError("APPLY_COMMITTED does not match prepared record")
        oid = prepared["order_id"]
        cursor = int(self._applications.get(oid, {}).get("applied_lots", 0))
        if cursor not in (int(prepared["from_applied_lots"]), int(prepared["to_applied_lots"])):
            raise ValueError("non-contiguous committed application history")
        self._committed.add(aid)
        self._applications[oid] = {"applied_lots": int(prepared["to_applied_lots"]), "last_apply_id": aid}

    def application_state(self, order_id: str) -> dict:
        state = self._applications.get(str(order_id), {"applied_lots": 0, "last_apply_id": None})
        pending = [dict(x) for x in self._prepared.values()
                   if x["order_id"] == str(order_id) and x["apply_id"] not in self._committed]
        return {"applied_lots": int(state.get("applied_lots", 0)),
                "last_apply_id": state.get("last_apply_id"), "pending": pending}

    def pending_applications(self) -> List[dict]:
        return [dict(x) for aid, x in self._prepared.items() if aid not in self._committed]

    # ---- Existing query API ----
    def latest(self, order_id: str) -> Optional[dict]:
        rec = self._latest.get(str(order_id))
        return dict(rec) if rec is not None else None

    def has_order(self, order_id: str) -> bool:
        return str(order_id) in self._latest

    def latest_all(self) -> Dict[str, dict]:
        return {k: dict(v) for k, v in self._latest.items()}

    def _pending_residual_execution(self, rec: dict) -> bool:
        if rec.get("status") not in TERMINAL_STATUSES:
            return False
        try:
            executed = int(rec.get("executed_lots") or 0)
            requested = int(rec.get("requested_lots") or 0)
        except (TypeError, ValueError):
            return False
        if executed <= 0:
            return False
        if rec["status"] in DEAD_WITHOUT_EXECUTION:
            return True
        return requested > 0 and executed < requested

    def open_orders(self, side: Optional[str] = None) -> List[dict]:
        return [dict(rec) for rec in self._latest.values()
                if (rec["status"] in NON_TERMINAL_STATUSES or self._pending_residual_execution(rec))
                and (side is None or rec.get("side") == side)]

    def figi_has_open_order(self, figi: str, side: str = "BUY") -> bool:
        return any(r.get("figi") == figi and r.get("side") == side and r["status"] in NON_TERMINAL_STATUSES
                   for r in self._latest.values())

    def figi_blocks_buy(self, figi: str) -> bool:
        for rec in self._latest.values():
            if rec["status"] not in NON_TERMINAL_STATUSES or rec.get("figi") != figi:
                continue
            if rec.get("side") in ("BUY", None) or rec["status"] == STATUS_UNCERTAIN:
                return True
        return False

    def has_unknown_figi_open_orders(self) -> bool:
        return any(r["status"] in NON_TERMINAL_STATUSES and not r.get("figi") for r in self._latest.values())

    def figi_is_uncertain(self, figi: str, side: str = "BUY") -> bool:
        return any(r.get("figi") == figi and r.get("side") == side and r["status"] == STATUS_UNCERTAIN
                   for r in self._latest.values())

    def open_count(self, side: Optional[str] = None) -> int:
        return len(self.open_orders(side))

    def export_summary(self) -> List[dict]:
        return [dict(rec) for rec in self._latest.values()]
