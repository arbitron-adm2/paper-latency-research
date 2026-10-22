"""Threshold close (both legs, one intent) + economics guard."""
from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from bot.config import BotConfig
from bot.economics import Economics
from bot.farmer import BotHalt
from bot.live import OpenPosition, WalletSnapshot, scalp_trigger
from bot.logger import BotLogger
from bot.single import SingleFarmer
from bot.state import BotState, SlotInfo, SlotState

ADDR = "0xacc"
E18 = 10**18


def _hedge():
    lp = OpenPosition(1, 0, True, 829145, Decimal(100), 100, 821244, 1_000_000, None)     # entry 82914.5, bust 82124.4
    sp = OpenPosition(2, 0, False, 829145, Decimal(100), 100, 837038, 1_000_000, None)    # bust 83703.8
    return [lp, sp]


def _snap(positions=(), free=Decimal(40)):
    return WalletSnapshot(ADDR, free, free, Decimal(0), Decimal(0), Decimal(0), open_positions=list(positions))


@pytest.fixture
def f(tmp_path):
    cfg = BotConfig(wallet_a_path=tmp_path / "a", wallet_b_path=tmp_path / "b", dry_run=False, close_fraction=0.6)
    st = BotState(slot_a=SlotInfo(label="A", address=ADDR, side="LONG", state=SlotState.OPEN, opened_at_ms=1),
                  slot_b=SlotInfo(label="B", address=ADDR, side="SHORT", state=SlotState.OPEN, opened_at_ms=1))
    fa = SingleFarmer(cfg, create_autospec(BotLogger, instance=True), st)
    fa.client_a = fa.client_b = MagicMock()
    fa.monitor, fa.staker = MagicMock(), MagicMock()
    fa.staker.claim_and_stake_both.return_value = {"a_paper": Decimal(0), "total": Decimal(0)}
    fa.economics = None
    fa.state_path = tmp_path / "st.json"
    return fa


def _live(snap):
    return (patch("bot.single.get_snapshot", return_value=snap), patch("bot.live.get_snapshot", return_value=snap))


class _Ctx:
    def __init__(self, *ps):
        self.ps = ps

    def __enter__(self):
        [p.start() for p in self.ps]

    def __exit__(self, *a):
        [p.stop() for p in self.ps]


# ── pure trigger ──

def test_scalp_trigger_thresholds():
    kw = dict(entry=82914.5, bust_long=82124.4, bust_short=83703.8, fraction=0.6)
    assert scalp_trigger(price=83388.2, **kw) == "up"
    assert scalp_trigger(price=83388.0, **kw) is None
    assert scalp_trigger(price=82440.3, **kw) == "down"
    assert scalp_trigger(price=82440.6, **kw) is None
    assert scalp_trigger(price=90000.0, **{**kw, "fraction": 0}) is None      # 0 disables the feature


# ── farmer ──

def test_no_close_before_threshold(f):
    f._close_slot_positions = MagicMock(return_value=True)
    with _Ctx(*_live(_snap(_hedge())), patch("bot.single.get_mid", return_value=82950.0)):
        assert f.reconcile() == "hedged"
    f._close_slot_positions.assert_not_called()


def test_threshold_closes_both_legs_in_one_call_and_completes_the_cycle(f):
    f._close_slot_positions = MagicMock(return_value=True)
    with _Ctx(*_live(_snap(_hedge())), patch("bot.single.get_mid", return_value=83400.0)):
        assert f.reconcile() == "scalp_closed"
    f._close_slot_positions.assert_called_once_with("a")          # ONE intent for the account, both ids inside
    assert f.state.total_cycles == 1 and f.state.slot_a.state == SlotState.IDLE
    assert f._closing_since is None
    f.logger.bust_event.assert_called_once()
    f.staker.claim_and_stake_both.assert_called_once()            # staking is attempted right after every cycle


def test_queued_threshold_close_is_resumed_without_reevaluating_price(f):
    f._close_slot_positions = MagicMock(return_value=False)
    with _Ctx(*_live(_snap(_hedge())), patch("bot.single.get_mid", return_value=83400.0)):
        assert f.reconcile() == "closing"
    assert f._closing_since is not None and f.state.total_cycles == 0
    f._close_slot_positions = MagicMock(return_value=True)
    with _Ctx(*_live(_snap(_hedge())), patch("bot.single.get_mid", side_effect=AssertionError("must not poll price while closing"))):
        assert f.reconcile() == "scalp_closed"
    assert f.state.total_cycles == 1
    f.logger.bust_event.assert_called_once()                      # not announced twice


def test_threshold_close_stuck_for_10_minutes_halts(f):
    f._close_slot_positions = MagicMock(return_value=False)
    f._closing_since = time.time() - 700
    with _Ctx(*_live(_snap(_hedge())), patch("bot.single.get_mid", return_value=83400.0)), pytest.raises(BotHalt):
        f.reconcile()


def test_missing_price_never_closes(f):
    f._close_slot_positions = MagicMock(return_value=True)
    with _Ctx(*_live(_snap(_hedge())), patch("bot.single.get_mid", return_value=None)):
        assert f.reconcile() == "hedged"
    f._close_slot_positions.assert_not_called()


def test_fast_poll_only_while_a_hedge_is_open(f):
    assert f.poll_interval() == f.config.fast_poll_s
    f.state.slot_a.state = SlotState.IDLE
    assert f.poll_interval() == f.config.poll_interval_s


def test_economics_recorded_from_exchange_trade_records(f, tmp_path):
    f.economics = Economics(tmp_path / "e.json")
    f._cycle_pos, f._cycle_kind, f._cycle_notional = [1, 2], "scalp", 10000.0
    items = [
        {"positionId": "1", "adjustedPnlRaw": str(int(10.7047 * E18)), "userPaidFeeRaw": str(int(0.2141 * E18)), "paperMintedRaw": "0"},
        {"positionId": "2", "adjustedPnlRaw": str(int(-13.7491 * E18)), "userPaidFeeRaw": "0", "paperMintedRaw": str(int(547.46 * E18))},
    ]
    resp = MagicMock()
    resp.json.return_value = {"items": items}
    with patch("httpx.get", return_value=resp):
        f._record_economics()
    c = f.economics.cycles[-1]
    assert c["kind"] == "scalp" and c["net"] == pytest.approx(-3.2585, abs=1e-3) and c["paper"] == pytest.approx(547.46)
    assert f.economics.cost_per_paper() == pytest.approx(3.2585 / 547.46, rel=1e-3)     # ~ $5.95 per 1000 PAPER, measured live


# ── economics guard ──

def _econ(tmp_path, now=1_000_000.0):
    t = {"now": now}
    return Economics(tmp_path / "e.json", clock=lambda: t["now"]), t


def test_yield_from_samples_and_payback(tmp_path):
    e, t = _econ(tmp_path)
    e.samples = [[t["now"] - 7200, 1_000_000.0, 4.7e9], [t["now"], 1_000_000.0 + 258_000.0, 4.7e9]]     # +$258k in 2h
    y = e.yield_per_paper_day()
    assert y == pytest.approx(258_000 / (2 / 24) / 4.7e9, rel=1e-6)                                        # ~ $0.00066 / PAPER / day
    e.cycles = [{"ts": t["now"], "net": -3.26, "paper": 547.46, "kind": "scalp", "notional": 1e4}]
    assert e.payback_days() == pytest.approx((3.26 / 547.46) / y, rel=1e-6)                                # ~ 9 days at $5.95/1k


def test_not_enough_data_never_blocks(tmp_path):
    e, t = _econ(tmp_path)
    assert e.yield_per_paper_day() is None and e.decision(Decimal(234), 7, 0.3) == (True, "ok")
    e.samples = [[t["now"] - 600, 1.0, 4.7e9], [t["now"], 2.0, 4.7e9]]                                    # only 10 min: too short
    assert e.yield_per_paper_day() is None


def test_blocks_when_payback_too_long_and_when_budget_is_spent(tmp_path):
    e, t = _econ(tmp_path)
    e.samples = [[t["now"] - 7200, 0.0, 4.7e9], [t["now"], 258_000.0, 4.7e9]]
    e.cycles = [{"ts": t["now"] - 100, "net": -3.26, "paper": 547.46, "kind": "scalp", "notional": 1e4}]
    ok, why = e.decision(Decimal(234), max_payback_days=7, daily_budget_frac=0.3)
    assert not ok and "does not pay back" in why and "$5.95/1k" in why
    assert e.decision(Decimal(234), max_payback_days=20, daily_budget_frac=0.3)[0] is True
    e.cycles.append({"ts": t["now"] - 50, "net": -80.0, "paper": 3000.0, "kind": "liquidation", "notional": 1e4})
    ok, why = e.decision(Decimal(234), max_payback_days=99, daily_budget_frac=0.3)
    assert not ok and "24h spend" in why


def test_old_spend_ages_out_of_the_budget(tmp_path):
    e, t = _econ(tmp_path)
    e.cycles = [{"ts": t["now"] - 90_000, "net": -100.0, "paper": 100.0, "kind": "x", "notional": 0}]
    assert e.spent_24h() == 0


def test_economics_persist_round_trip(tmp_path):
    e, t = _econ(tmp_path)
    e.record_cycle(Decimal("-3.26"), Decimal("547.46"), "scalp", 1e4)
    e2 = Economics(tmp_path / "e.json", clock=lambda: t["now"])
    assert e2.cycles[0]["paper"] == pytest.approx(547.46)


def test_farmer_does_not_open_while_economics_blocks(f):
    f.economics = MagicMock()
    f.economics.decision.return_value = (False, "PAPER does not pay back")
    f.state.slot_a.state = SlotState.IDLE
    f._last_stake_check_s = time.time()
    f.open_hedge = MagicMock()
    f._queue_ok = MagicMock(return_value=True)
    with _Ctx(*_live(_snap(free=Decimal(234)))):
        f.run_once()
        f.run_once()
    f.open_hedge.assert_not_called()
    assert f._econ_block == "PAPER does not pay back"
    assert sum("not opening a new hedge" in str(c) for c in f.logger.warn.call_args_list) == 1   # logged once, not every tick
