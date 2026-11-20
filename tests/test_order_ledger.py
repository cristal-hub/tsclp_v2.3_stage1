import json
from pathlib import Path

from order_ledger import OrderLedger, STATUS_CANCELLED, STATUS_FILL, STATUS_INTENT, STATUS_UNCERTAIN


def make_intent(ledger, oid="o1", side="BUY", trade="t1", figi="F1", lots=5):
    assert ledger.record_intent(oid, trade_id=trade, figi=figi, ticker="TEST",
                                side=side, requested_lots=lots)


def test_legacy_api_and_restart(tmp_path):
    path = tmp_path / "orders.jsonl"
    ledger = OrderLedger(str(path))
    make_intent(ledger)
    assert ledger.record_status("o1", "NEW", executed_lots=0, source="test")
    restored = OrderLedger(str(path))
    restored.load()
    assert restored.is_reliable()
    assert restored.latest("o1")["status"] == "NEW"
    assert restored.latest("o1")["created_at_utc"]
    assert restored.open_count() == 1


def test_apply_prepare_commit_is_durable_and_idempotent(tmp_path):
    path = tmp_path / "orders.jsonl"
    ledger = OrderLedger(str(path))
    make_intent(ledger)
    assert ledger.record_observation("o1", executed_lots=3, status="PARTIALLYFILL")
    aid = ledger.prepare_application("o1", trade_id="t1", figi="F1", side="BUY",
                                     from_applied_lots=0, to_applied_lots=3)
    assert aid and len(aid) == 64
    assert ledger.prepare_application("o1", trade_id="t1", figi="F1", side="BUY",
                                      from_applied_lots=0, to_applied_lots=3) == aid
    assert ledger.commit_application(aid)
    assert ledger.commit_application(aid)

    restored = OrderLedger(str(path))
    restored.load()
    assert restored.is_reliable()
    assert restored.application_state("o1")["applied_lots"] == 3
    assert restored.application_state("o1")["pending"] == []


def test_prepared_without_commit_survives_restart(tmp_path):
    path = tmp_path / "orders.jsonl"
    ledger = OrderLedger(str(path))
    make_intent(ledger)
    assert ledger.record_observation("o1", executed_lots=2, status="PARTIALLYFILL")
    aid = ledger.prepare_application("o1", trade_id="t1", figi="F1", side="BUY",
                                     from_applied_lots=0, to_applied_lots=2)
    restored = OrderLedger(str(path))
    restored.load()
    assert restored.is_reliable()
    assert restored.application_state("o1")["applied_lots"] == 0
    assert restored.application_state("o1")["pending"][0]["apply_id"] == aid
    assert restored.commit_application(aid)
    assert restored.application_state("o1")["applied_lots"] == 2


def test_cumulative_execution_regression_fails_closed(tmp_path):
    ledger = OrderLedger(str(tmp_path / "orders.jsonl"))
    make_intent(ledger, lots=5)
    assert ledger.record_observation("o1", executed_lots=3, status="PARTIALLYFILL")
    assert not ledger.record_observation("o1", executed_lots=2, status="PARTIALLYFILL")
    assert ledger.latest("o1")["status"] == STATUS_UNCERTAIN
    assert ledger.latest("o1")["executed_lots"] == 3
    assert ledger.figi_blocks_buy("F1")


def test_overfill_rejected(tmp_path):
    ledger = OrderLedger(str(tmp_path / "orders.jsonl"))
    make_intent(ledger, lots=2)
    assert not ledger.record_observation("o1", executed_lots=3, status=STATUS_FILL)
    assert ledger.latest("o1")["status"] == STATUS_UNCERTAIN


def test_sequential_sell_orders_have_independent_cursors(tmp_path):
    ledger = OrderLedger(str(tmp_path / "orders.jsonl"))
    make_intent(ledger, oid="sell-1", side="SELL", trade="trade-A", lots=4)
    assert ledger.record_observation("sell-1", executed_lots=2, status="PARTIALLYFILL")
    a1 = ledger.prepare_application("sell-1", trade_id="trade-A", figi="F1", side="SELL",
                                    from_applied_lots=0, to_applied_lots=2)
    assert ledger.commit_application(a1)
    assert ledger.record_status("sell-1", STATUS_CANCELLED, executed_lots=2)

    make_intent(ledger, oid="sell-2", side="SELL", trade="trade-A", lots=2)
    assert ledger.record_observation("sell-2", executed_lots=2, status=STATUS_FILL)
    a2 = ledger.prepare_application("sell-2", trade_id="trade-A", figi="F1", side="SELL",
                                    from_applied_lots=0, to_applied_lots=2)
    assert a2 != a1
    assert ledger.commit_application(a2)
    assert ledger.application_state("sell-1")["applied_lots"] == 2
    assert ledger.application_state("sell-2")["applied_lots"] == 2


def test_identity_mismatch_cannot_prepare(tmp_path):
    ledger = OrderLedger(str(tmp_path / "orders.jsonl"))
    make_intent(ledger)
    assert ledger.record_observation("o1", executed_lots=1, status="PARTIALLYFILL")
    assert ledger.prepare_application("o1", trade_id="wrong", figi="F1", side="BUY",
                                      from_applied_lots=0, to_applied_lots=1) is None


def test_corrupt_line_marks_ledger_unreliable_without_rewriting(tmp_path):
    path = tmp_path / "orders.jsonl"
    ledger = OrderLedger(str(path))
    make_intent(ledger)
    original = path.read_bytes()
    with path.open("ab") as f:
        f.write(b'{broken json}\n')
    damaged = path.read_bytes()
    restored = OrderLedger(str(path))
    restored.load()
    assert not restored.is_reliable()
    assert path.read_bytes() == damaged
    assert path.read_bytes().startswith(original)


def test_terminal_status_with_partial_fill_remains_pollable(tmp_path):
    ledger = OrderLedger(str(tmp_path / "orders.jsonl"))
    make_intent(ledger, side="SELL", lots=5)
    assert ledger.record_status("o1", STATUS_CANCELLED, executed_lots=2)
    assert ledger.open_orders()[0]["order_id"] == "o1"


def test_legacy_record_status_cannot_regress_cumulative_execution(tmp_path):
    ledger = OrderLedger(str(tmp_path / "orders.jsonl"))
    make_intent(ledger, lots=5)
    assert ledger.record_status("o1", "PARTIALLYFILL", executed_lots=3)
    assert ledger.record_status("o1", "PARTIALLYFILL", executed_lots=2)
    assert ledger.latest("o1")["status"] == STATUS_UNCERTAIN
    assert ledger.latest("o1")["executed_lots"] == 3
