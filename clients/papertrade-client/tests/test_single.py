"""SingleFarmer: both legs in ONE account (no cross-wallet rebalancing)."""
from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from bot.config import BotConfig
from bot.farmer import BotHalt
from bot.live import OpenPosition, PendingOp, WalletSnapshot
from bot.logger import BotLogger
from bot.single import SingleFarmer
from bot.state import BotState, SlotInfo, SlotState

ADDR = "0xacc"
ID_L, ID_S = "0x" + "11" * 32, "0x" + "22" * 32


def _pos(pid, long, intent=None, opened=1_000_000):
    return OpenPosition(pid, 0, long, 830305, Decimal(100), 100, 1, opened, intent)


def _snap(positions=(), ops=(), free=Decimal(200), paper=Decimal(0), staked=Decimal(0)):
    return WalletSnapshot(ADDR, free, free, Decimal(0), paper, staked,
                          open_positions=list(positions), pending_ops=list(ops))


def _op(action, iid, close_ids=()):
    return PendingOp(iid, action, int(time.time() * 1000), 0, close_ids=tuple(close_ids))


@pytest.fixture
def f(tmp_path):
    cfg = BotConfig(wallet_a_path=tmp_path / "a", wallet_b_path=tmp_path / "b", dry_run=False,
                    margin_usdc=100.0, leverage=250, naked_max_s=0, max_queue_wait_s=0)
    st = BotState(slot_a=SlotInfo(label="A", address=ADDR, side="LONG", state=SlotState.IDLE),
                  slot_b=SlotInfo(label="B", address=ADDR, side="SHORT", state=SlotState.IDLE))
    fa = SingleFarmer(cfg, create_autospec(BotLogger, instance=True), st)
    fa.client_a = MagicMock()
    fa.client_b = fa.client_a
    fa.client_a.write.open_position.side_effect = [{"intentId": ID_L}, {"intentId": ID_S}]
    fa.monitor = MagicMock()
    fa.staker = MagicMock()
    fa.staker.claim_and_stake_both.return_value = {"a_paper": Decimal(0), "total": Decimal(0)}
    fa.state_path = tmp_path / "st.json"
    return fa


def _live(*snaps):
    """get_snapshot patched everywhere it is imported; successive calls walk through `snaps`, last one sticks."""
    it = iter(snaps)
    last = {}

    def get(addr, **_k):
        try:
            last["v"] = next(it)
        except StopIteration:
            pass
        return last["v"]

    return patch("bot.single.get_snapshot", side_effect=get), patch("bot.live.get_snapshot", side_effect=get)


class _Both:
    def __init__(self, *snaps):
        self.ps = _live(*snaps)

    def __enter__(self):
        for p in self.ps:
            p.start()

    def __exit__(self, *a):
        for p in self.ps:
            p.stop()


# ── sizing ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("free,expected", [
    ("200", (Decimal(100), 100)),                      # the target setup: 2 x $100 @ 100x = $10k notional each
    ("1000", (Decimal(100), 100)),                     # capped by margin_usdc
    ("197.5", (Decimal("98.75"), 102)),                # shrinks, leverage grows to keep notional >= $10k
    ("20", (Decimal(10), 1000 // 1)),                  # exactly two minimum legs -> 1000x
    ("19.99", None),                                   # cannot fund two $10 legs
    ("0", None),
])
def test_sizing(f, free, expected):
    f.config.leverage = 1000
    got = f.sizing(Decimal(free))
    assert got == expected
    if got:
        m, lev = got
        assert m * lev >= 10_000 and 2 * m <= Decimal(free)


def test_sizing_respects_leverage_cap(f):
    f.config.leverage = 250
    assert f.sizing(Decimal(30)) is None               # would need 667x > cap


# ── opening ───────────────────────────────────────────────────────────

def test_open_hedge_sends_long_then_short_from_the_same_account(f):
    f.config.dry_run = False
    both = [_pos(1, True, ID_L), _pos(2, False, ID_S)]
    with _Both(_snap(free=Decimal(200)), _snap(both), _snap(both)), patch("time.sleep"):
        ids = f.open_hedge()
    assert ids == (ID_L, ID_S)
    calls = f.client_a.write.open_position.call_args_list
    assert [c.kwargs["is_long"] for c in calls] == [True, False]
    assert all(c.kwargs["margin_usdc"] == Decimal(100) and c.kwargs["leverage"] == 100 for c in calls)
    assert isinstance(calls[0].kwargs["margin_usdc"], Decimal)           # exact, never float
    assert f.state.slot_a.state == SlotState.OPEN
    assert f._consecutive_aborts == 0


def test_verify_finds_each_leg_by_intent_in_one_shared_snapshot(f):
    f._hedge_intents = {"a": ID_L, "b": ID_S}
    both = _snap([_pos(7, False, ID_S, opened=2_000_000), _pos(8, True, ID_L, opened=2_000_500)])
    with _Both(both), patch("time.sleep"):
        f._verify_hedge()
    f.logger.info.assert_called()
    assert f._consecutive_aborts == 0


def test_second_leg_submit_failure_cancels_first_and_does_not_halt(f):
    f.client_a.write.open_position.side_effect = [{"intentId": ID_L}, RuntimeError("HTTP 500")]
    f._flatten = MagicMock()
    with _Both(_snap(free=Decimal(200))), patch("time.sleep"):
        f.open_hedge()
    f.client_a.write.cancel_intent.assert_called_with(ID_L)               # queued leg 1 must not survive
    assert f._open_cooldown_until > time.time() + 200
    assert f._consecutive_aborts == 1


def test_three_aborts_in_a_row_halt_the_bot(f):
    f._flatten = MagicMock()
    f._consecutive_aborts = 2
    f.client_a.write.open_position.side_effect = [{"intentId": ID_L}, RuntimeError("HTTP 500")]
    with _Both(_snap(free=Decimal(200))), patch("time.sleep"), pytest.raises(BotHalt):
        f.open_hedge()


def test_not_enough_balance_opens_nothing(f):
    with _Both(_snap(free=Decimal("19"))):
        assert f.open_hedge() == ("", "")
    f.client_a.write.open_position.assert_not_called()


# ── reconcile ─────────────────────────────────────────────────────────

def test_reconcile_hedged_and_flat(f):
    f.config.close_fraction = 0          # threshold close has its own tests (test_scalp.py)
    with _Both(_snap([_pos(1, True), _pos(2, False)])):
        assert f.reconcile() == "hedged"
    assert f.state.slot_a.state == SlotState.OPEN and f.state.slot_b.state == SlotState.OPEN
    with _Both(_snap()):
        assert f.reconcile() == "flat"
    assert f.state.slot_a.state == SlotState.IDLE


def test_reconcile_lone_leg_with_liquidation_closes_survivor_and_completes_cycle(f):
    f.monitor.detect_bust.return_value = {"kind": "Liquidated"}
    f._close_slot_positions = MagicMock(return_value=True)
    with _Both(_snap([_pos(1, True)]), _snap(paper=Decimal(4300))):
        assert f.reconcile() == "bust_handled"
    f._close_slot_positions.assert_called_once_with("a")
    assert f.state.total_cycles == 1 and f.state.slot_a.state == SlotState.IDLE
    f.staker.claim_and_stake_both.assert_called_once()
    f.logger.bust_event.assert_called_once()
    assert f.logger.bust_event.call_args.kwargs["side"] == "short"        # survivor LONG => SHORT leg died


def test_queued_survivor_close_is_tracked_across_ticks_without_double_count(f):
    f.monitor.detect_bust.return_value = {"kind": "Liquidated"}
    f._close_slot_positions = MagicMock(return_value=False)               # close sits in the relayer queue
    with _Both(_snap([_pos(1, True)])):
        assert f.reconcile() == "closing"
        assert f.reconcile() == "closing"                                 # next tick: no second bust_event
    assert f.logger.bust_event.call_count == 1 and f.state.total_cycles == 0
    with _Both(_snap(paper=Decimal(4300))):                               # relayer executed the close meanwhile
        assert f.reconcile() == "bust_handled"
    assert f.state.total_cycles == 1 and f._closing_since is None
    with _Both(_snap()):
        assert f.reconcile() == "flat"
    assert f.state.total_cycles == 1                                      # not counted twice


def test_lone_leg_without_liquidation_gets_grace_then_flatten_and_halt(f):
    f.monitor.detect_bust.return_value = None
    f._flatten = MagicMock()
    with _Both(_snap([_pos(1, False)])):
        assert f.reconcile() == "orphan_pending"
        f._flatten.assert_not_called()
        f._orphan_since -= 100
        with pytest.raises(BotHalt):
            f.reconcile()
    f._flatten.assert_called_once_with("a")


def test_handle_bust_end_to_end_with_real_logger(tmp_path):
    """No logger mock: signature drift between farmer and BotLogger must fail here (it caused a live incident)."""
    cfg = BotConfig(wallet_a_path=tmp_path / "a", wallet_b_path=tmp_path / "b", dry_run=False)
    st = BotState(slot_a=SlotInfo(label="A", address=ADDR, side="LONG", state=SlotState.OPEN, opened_at_ms=1),
                  slot_b=SlotInfo(label="B", address=ADDR, side="SHORT", state=SlotState.OPEN, opened_at_ms=1))
    log = tmp_path / "bot.log"
    fa = SingleFarmer(cfg, BotLogger(log), st)
    fa.client_a = fa.client_b = MagicMock()
    fa.staker = MagicMock()
    fa.staker.claim_and_stake_both.return_value = {"a_paper": Decimal(4300), "total": Decimal(4300)}
    fa._close_slot_positions = MagicMock(return_value=True)
    fa.state_path = tmp_path / "st.json"
    with _Both(_snap(paper=Decimal(4300))):
        fa.handle_bust("a", loser_side="long")
    text = log.read_text()
    assert "bust_event" in text and "cycle_summary" in text
    assert st.total_cycles == 1 and st.slot_a.paper_staked_total == 4300


def test_flatten_all_closes_the_account_once(f):
    f._close_slot_positions = MagicMock(return_value=True)
    with _Both(_snap([_pos(1, True), _pos(2, False)])):
        out = f.flatten_all()
    assert out == {"a": [1, 2], "b": []}
    f._close_slot_positions.assert_called_once_with("a")


# ── run_once gating ───────────────────────────────────────────────────

def test_run_once_opens_when_flat_funded_and_queue_ok(f):
    f.open_hedge = MagicMock()
    f._queue_ok = MagicMock(return_value=True)
    f._last_stake_check_s = time.time()
    with _Both(_snap(free=Decimal(200))):
        f.run_once()
    f.open_hedge.assert_called_once()


@pytest.mark.parametrize("why", ["paused", "closing", "open_state", "unfunded", "queue"])
def test_run_once_does_not_open(f, why):
    f.open_hedge = MagicMock()
    f._queue_ok = MagicMock(return_value=why != "queue")
    f._last_stake_check_s = time.time()
    free = Decimal(5) if why == "unfunded" else Decimal(200)
    if why == "closing":
        f._closing_since = time.time()
    if why == "open_state":
        f.state.slot_a.state = SlotState.OPEN
    with _Both(_snap(free=free)):
        f.run_once(allow_open=(why != "paused"))
    f.open_hedge.assert_not_called()


# ── auto-deposit ──────────────────────────────────────────────────────

def test_auto_deposit_sends_once_and_respects_cooldown(f):
    f.config.auto_deposit = True
    f.wallet_a = MagicMock()
    with patch("bot.single.onchain.get_usdc_balance", return_value=Decimal(200)), \
         patch("bot.single.onchain.get_hype_balance", return_value=Decimal("0.01")), \
         patch("bot.single.onchain.deposit_usdc_to_papertrade", return_value="0xtx") as dep:
        f._maybe_deposit()
        f._last_dep_check = 0                      # even if the poll timer allows it...
        f._maybe_deposit()                         # ...the 180 s credit cooldown blocks a double deposit
    dep.assert_called_once()
    assert dep.call_args.kwargs["amount_usdc"] == Decimal(200)


@pytest.mark.parametrize("usdc,hype", [("9.99", "0.01"), ("200", "0.0001")])
def test_auto_deposit_skips_dust_and_low_gas(f, usdc, hype):
    f.wallet_a = MagicMock()
    with patch("bot.single.onchain.get_usdc_balance", return_value=Decimal(usdc)), \
         patch("bot.single.onchain.get_hype_balance", return_value=Decimal(hype)), \
         patch("bot.single.onchain.deposit_usdc_to_papertrade") as dep:
        f._maybe_deposit()
    dep.assert_not_called()


def test_auto_deposit_error_never_breaks_the_loop(f):
    f.config.auto_deposit = True
    f._last_stake_check_s = time.time()
    f.open_hedge = MagicMock()
    with patch("bot.single.onchain.get_usdc_balance", side_effect=RuntimeError("rpc down")), \
         _Both(_snap(free=Decimal(200))):
        f._queue_ok = MagicMock(return_value=True)
        f.run_once()                               # must not raise
    assert f._deposit_cooldown_until > time.time() + 200
