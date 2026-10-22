"""Relayer-queue behaviour: queued legs, aborts, idempotent closes, staking, gating."""
from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock, patch, create_autospec

import pytest

from bot.config import BotConfig
from bot.farmer import PaperFarmer
from bot.live import OpenPosition, PendingOp, WalletSnapshot, _parse
from bot.logger import BotLogger
from bot.staker import PaperStaker
from bot.state import BotState, SlotInfo, SlotState

IA, IB = "0x" + "a1" * 32, "0x" + "b2" * 32


def _snap(positions=(), ops=(), paper=Decimal(0)):
    return WalletSnapshot("x", Decimal(0), Decimal(0), Decimal(0), paper, Decimal(0),
                          open_positions=list(positions), pending_ops=list(ops))


def _pos(pid, intent, entry=830305, long=True):
    return OpenPosition(pid, 0, long, entry, Decimal(10), 1000, 1, 1_000_000 + pid, intent)


def _op(action, intent, close_ids=()):
    return PendingOp(intent, action, int(time.time() * 1000), 0, close_ids=tuple(close_ids))


@pytest.fixture
def farmer(tmp_path):
    cfg = BotConfig(wallet_a_path=tmp_path / "a", wallet_b_path=tmp_path / "b", dry_run=False,
                    naked_max_s=0, max_queue_wait_s=0, open_cooldown_s=300)
    state = BotState(slot_a=SlotInfo(label="A", address="0xa", side="LONG"),
                     slot_b=SlotInfo(label="B", address="0xb", side="SHORT"))
    f = PaperFarmer(cfg, create_autospec(BotLogger, instance=True), state)
    f.client_a, f.client_b = MagicMock(), MagicMock()
    f._hedge_intents = {"a": IA, "b": IB}
    return f


def _live(a, b):
    m = {"0xa": a, "0xb": b}
    return patch("bot.live.get_snapshot", side_effect=lambda addr, **k: m[addr])


def test_parse_pending_ops_from_wire():
    raw = {"address": "0xa", "balance": "0", "locked": "0", "funds": {}, "paper": {},
           "operations": [[IA, "openPosition", 0, True, False, "10000000000000000000", 1000, 1791668546000, "1791672146", None],
                          [IB, "closePositions", None, None, None, None, None, 1791668547000, "1791672147", ["72364"]]],
           "queueRevision": "7"}
    s = _parse(raw)
    assert [o.action for o in s.pending_ops] == ["openPosition", "closePositions"]
    assert s.pending_ops[0].margin == Decimal(10)
    assert s.pending_ops[1].close_ids == (72364,)
    assert s.pending("closePositions")[0].intent_id == IB and s.queue_revision == "7"


def test_hedge_confirmed_when_both_filled(farmer):
    with _live(_snap([_pos(1, IA)]), _snap([_pos(2, IB, long=False)])):
        farmer._verify_hedge()
    farmer.logger.info.assert_called()
    assert farmer._open_cooldown_until == 0.0


def test_one_leg_filled_other_queued_waits_within_naked_window(farmer):
    farmer.config.naked_max_s = 3600
    farmer.config.max_queue_wait_s = 3600
    seq = iter([(_snap([_pos(1, IA)]), _snap(ops=[_op("openPosition", IB)])),
                (_snap([_pos(1, IA)]), _snap([_pos(2, IB, long=False)]))])
    cur = {}

    def get(addr, **k):
        if addr == "0xa" and "pair" not in cur or addr == "0xa" and cur.get("served_b"):
            cur["pair"] = next(seq); cur["served_b"] = False
        if addr == "0xb":
            cur["served_b"] = True
        return cur["pair"][0 if addr == "0xa" else 1]

    farmer._abort_hedge = MagicMock()
    with patch("bot.live.get_snapshot", side_effect=get), patch("time.sleep"):
        farmer._verify_hedge()
    farmer._abort_hedge.assert_not_called()      # queued leg was tolerated, then filled


def test_queued_leg_beyond_naked_window_cancels_and_flattens(farmer):
    farmer._flatten = MagicMock()
    with _live(_snap([_pos(1, IA)]), _snap(ops=[_op("openPosition", IB)])), patch("time.sleep"):
        farmer._verify_hedge()
    farmer.client_a.write.cancel_intent.assert_called_with(IA)
    farmer.client_b.write.cancel_intent.assert_called_with(IB)
    assert farmer._flatten.call_count == 2
    assert farmer._open_cooldown_until > time.time() + 200      # cooled down, NOT halted


def test_rejected_leg_needs_two_polls_then_flattens(farmer):
    farmer.config.naked_max_s = 3600
    farmer.config.max_queue_wait_s = 3600
    farmer._abort_hedge = MagicMock()
    calls = {"n": 0}

    def get(addr, **k):
        calls["n"] += 1
        return _snap([_pos(1, IA)]) if addr == "0xa" else _snap()   # B: neither position nor pending

    with patch("bot.live.get_snapshot", side_effect=get), patch("time.sleep"):
        farmer._verify_hedge()
    assert calls["n"] == 4              # 2 polls x 2 wallets before concluding (index lag tolerance)
    farmer._abort_hedge.assert_called_once()


def test_close_is_idempotent_while_close_is_queued(farmer):
    farmer.config.max_queue_wait_s = 0
    queued = _snap([_pos(7, IB, long=False)], ops=[_op("closePositions", "0x" + "c3" * 32, close_ids=[7])])
    with patch("bot.live.get_snapshot", return_value=queued), patch("time.sleep"):
        assert farmer._close_slot_positions("b") is False       # still queued...
    farmer.client_b.write.close_positions.assert_not_called()   # ...and NOT re-sent


def test_close_sends_once_when_nothing_queued(farmer):
    farmer.config.max_queue_wait_s = 0
    with patch("bot.live.get_snapshot", return_value=_snap([_pos(7, IB, long=False)])), patch("time.sleep"):
        assert farmer._close_slot_positions("b") is False
    farmer.client_b.write.close_positions.assert_called_once_with([7], dry_run=False)


def test_handle_bust_with_queued_close_does_not_halt_or_double_count(farmer):
    farmer.monitor = MagicMock()
    farmer.staker = MagicMock()
    farmer.state.slot_a.state = farmer.state.slot_b.state = SlotState.OPEN
    farmer._close_slot_positions = MagicMock(return_value=False)
    farmer.handle_bust("a")                              # must not raise
    assert farmer.state.total_cycles == 0
    farmer.staker.claim_paper.assert_not_called()
    assert farmer._closing_since is not None
    farmer.logger.bust_event.assert_called_once()
    farmer.handle_bust("a")                              # next tick: same bust, still queued
    farmer.logger.bust_event.assert_called_once()        # logged only once


def test_staker_skips_when_stake_already_queued():
    a, b = MagicMock(), MagicMock()
    a.write._wallet.address, b.write._wallet.address = "0xa", "0xb"
    snaps = {"0xa": _snap(ops=[_op("paperStake", IA)], paper=Decimal(5000)), "0xb": _snap(paper=Decimal(2000))}
    st = PaperStaker(a, b, create_autospec(BotLogger, instance=True), dry_run=False)
    with patch("bot.live.get_snapshot", side_effect=lambda addr, **k: snaps[addr]):
        out = st.claim_and_stake_both()
    a.write.stake.assert_not_called()                    # A's stake is still queued: no duplicate
    b.write.stake.assert_called_once()
    assert out["a_paper"] == 0 and out["b_paper"] == Decimal(2000)


@pytest.mark.parametrize("pending_gas,block_gas,ok", [("3000000", "3000000", True), ("73235000", "3000000", False)])
def test_queue_gate_uses_drain_time_not_saturation(farmer, pending_gas, block_gas, ok):
    p = {"totalPendingGas": pending_gas, "blockGasLimit": block_gas, "saturationPct": 0, "queuedIntents": 228}
    with patch("bot.live.get_pressure", return_value=p):
        assert farmer._queue_ok() is ok


def test_queue_gate_fails_closed_without_stream_and_respects_cooldown(farmer):
    with patch("bot.live.get_pressure", return_value=None):
        assert farmer._queue_ok() is False
    farmer._open_cooldown_until = time.time() + 100
    with patch("bot.live.get_pressure", return_value={"totalPendingGas": "1", "blockGasLimit": "3000000"}):
        assert farmer._queue_ok() is False


# ── regression: the live incident (logger signature mismatch crashed handle_bust, winner stayed open) ──

def test_handle_bust_end_to_end_with_real_logger(tmp_path):
    """No mocks on the logger: any signature drift between farmer and BotLogger fails here."""
    log = tmp_path / "bot.log"
    cfg = BotConfig(wallet_a_path=tmp_path / "a", wallet_b_path=tmp_path / "b", dry_run=False)
    state = BotState(slot_a=SlotInfo(label="A", address="0xa", side="LONG", state=SlotState.OPEN, opened_at_ms=1),
                     slot_b=SlotInfo(label="B", address="0xb", side="SHORT", state=SlotState.OPEN, opened_at_ms=1))
    f = PaperFarmer(cfg, BotLogger(log), state)
    f.client_a, f.client_b = MagicMock(), MagicMock()
    f.staker = MagicMock()
    f.staker.claim_and_stake_both.return_value = {"a_paper": Decimal("415.7"), "b_paper": Decimal(0), "total": Decimal("415.7")}
    f._close_slot_positions = MagicMock(return_value=True)          # winner B closes
    f.state_path = tmp_path / "state.json"
    with patch("bot.live.get_snapshot", return_value=_snap(paper=Decimal("415.7"))):
        f.handle_bust("a")                                          # must not raise
    f._close_slot_positions.assert_called_once_with("b")            # the WINNER (B) is the one closed
    assert state.total_cycles == 1
    assert state.slot_a.state == SlotState.IDLE and state.slot_b.state == SlotState.IDLE
    text = log.read_text()
    assert "bust_event" in text and "cycle_summary" in text
    assert state.slot_a.paper_earned_total == pytest.approx(415.7)


# ── exchange rate limiting (Cloudflare 429 + Retry-After) ──

class _Resp:
    def __init__(self, status, headers=None, lines=()):
        self.status_code, self.headers, self._lines = status, headers or {}, list(lines)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_lines(self):
        return iter(self._lines)


def test_snapshot_honours_retry_after_and_caches(monkeypatch):
    import bot.live as L
    L._cache.clear()
    L._next_ok = 0.0
    good = _Resp(200, lines=["event: wallet", 'data: {"address": "0xrl", "balance": "0"}'])
    calls = iter([_Resp(429, {"retry-after": "7"}), good])
    sleeps = []
    monkeypatch.setattr(L.httpx, "stream", lambda *a, **k: next(calls))
    monkeypatch.setattr(L.time, "sleep", lambda s: sleeps.append(s))
    snap = L.get_snapshot("0xRL")
    assert snap.address == "0xrl"
    assert L._next_ok >= L.time.time() + 5            # the 429 pushed the whole process back ~7s
    n = L.get_snapshot("0xrl")                        # within TTL: served from cache, no new request
    assert n is snap


# ── PAPER stake minimum (live finding: HTTP 409 below_minimum_paper_stake, minimum 1000 PAPER) ──

def _staker(snap_paper, staked=Decimal(0), min_body=None):
    from papertrade.exceptions import HttpError
    a = MagicMock()
    a.write._wallet.address = "0xa"
    if min_body:
        a.write.stake.side_effect = HttpError(409, min_body)
    st = PaperStaker(a, None, create_autospec(BotLogger, instance=True), dry_run=False)
    snap = WalletSnapshot("0xa", Decimal(0), Decimal(0), Decimal(0), snap_paper, staked)
    return st, a, snap


def test_below_minimum_is_not_sent_and_not_an_error():
    st, a, snap = _staker(Decimal("415.687"))
    with patch("bot.live.get_snapshot", return_value=snap):
        out = st.claim_and_stake_both()
    a.write.stake.assert_not_called()                                    # no doomed 409 every poll
    assert out["total"] == 0
    st.logger.warn.assert_not_called()
    st.logger.info.assert_called()                                       # explained once, quietly


def test_at_minimum_stakes_exact_amount_and_wallet_b_may_be_absent():
    exact = Decimal("1234.567890123456789012")
    st, a, snap = _staker(exact)
    with patch("bot.live.get_snapshot", return_value=snap):
        out = st.claim_and_stake_both()
    a.write.stake.assert_called_once_with(exact, dry_run=False)          # Decimal, never float
    assert out["a_paper"] == exact


def test_server_changed_minimum_is_learned_from_409_body():
    st, a, snap = _staker(Decimal(1100), min_body={"code": "below_minimum_paper_stake",
                                                   "minimumPaperStakeRaw": str(5000 * 10**18)})
    with patch("bot.live.get_snapshot", return_value=snap):
        st.claim_and_stake_both()
    assert st.min_stake == Decimal(5000)
    st.logger.warn.assert_not_called()


def test_write_layer_stake_amount_is_decimal_exact():
    from decimal import Decimal as D
    raw = 415687866980974789720
    assert int(D(str(D(raw) / D(10**18))) * 10**18) == raw               # float would give ...788608
