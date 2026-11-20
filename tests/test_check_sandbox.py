# tests/test_check_sandbox.py
# Tsclp_v2.2_volatile — тесты check_sandbox.py (mock).

import pytest
from unittest.mock import MagicMock

import check_sandbox as cs


def make_op(op_type, payment_units=0, payment_nano=0):
    op = MagicMock()
    op.operation_type = op_type
    if payment_units == 0 and payment_nano == 0:
        op.payment = None
    else:
        op.payment = MagicMock(units=payment_units, nano=payment_nano)
    return op


def test_is_trade_operation_buy():
    assert cs._is_trade_operation(make_op(15)) is True


def test_is_trade_operation_sell():
    assert cs._is_trade_operation(make_op(22)) is True


def test_is_trade_operation_broker_fee():
    assert cs._is_trade_operation(make_op(19)) is False


def test_is_trade_operation_other():
    assert cs._is_trade_operation(make_op(4)) is False


def test_is_broker_fee_true():
    assert cs._is_broker_fee(make_op(19)) is True


def test_is_broker_fee_false():
    assert cs._is_broker_fee(make_op(15)) is False


def test_payment_float_negative():
    op = make_op(15, payment_units=-100, payment_nano=-500_000_000)
    assert cs._payment_float(op) == pytest.approx(-100.5)


def test_payment_float_positive():
    op = make_op(22, payment_units=200, payment_nano=0)
    assert cs._payment_float(op) == pytest.approx(200.0)


def test_payment_float_none():
    op = make_op(15)
    assert cs._payment_float(op) == 0.0


def test_payment_float_broken():
    op = MagicMock()
    op.payment = MagicMock(units="bad", nano=None)
    # quotation_to_decimal может упасть — 0.0
    assert cs._payment_float(op) == 0.0