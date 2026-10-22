"""House (counterparty pool) health check: pure decision logic."""
from __future__ import annotations

from bot.doctor import CRIT, OK, WARN, evaluate_house


def _by(res):
    return {r["name"]: r for r in res}


def test_falling_pool_warns_never_crits():
    r = _by(evaluate_house([0, 0, 6.5e6, 5.9e6, 4.2e6, 2.9e6], 2.9e6, False, True))
    assert r["house:pool"]["sev"] == WARN
    assert r["house:pool"]["drop"] > 0.5
    assert r["house:trading-paused"]["sev"] == OK


def test_stable_pool_ok():
    r = _by(evaluate_house([5e6, 5.1e6, 4.9e6, 5.0e6], 4.95e6, False, True))
    assert r["house:pool"]["sev"] == OK


def test_small_dip_below_threshold_ok():
    r = _by(evaluate_house([5e6, 5e6, 5e6], 4e6, False, False))   # -20%
    assert r["house:pool"]["sev"] == OK


def test_paused_with_positions_is_critical_without_positions_warn():
    assert _by(evaluate_house([5e6] * 4, 5e6, True, True))["house:trading-paused"]["sev"] == CRIT
    assert _by(evaluate_house([5e6] * 4, 5e6, True, False))["house:trading-paused"]["sev"] == WARN


def test_little_history_is_not_a_false_alarm():
    r = _by(evaluate_house([0, 0, 0], 3e6, False, True))
    assert r["house:pool"]["sev"] == OK
    assert "not enough" in r["house:pool"]["msg"]
