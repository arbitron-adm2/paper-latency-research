"""PANIC withdrawal + RiskGuard. All exchange / chain I/O is faked."""
from __future__ import annotations

import json
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from bot import panic as P
from bot.live import OpenPosition, PendingOp, WalletSnapshot
from bot.risk import RiskGuard
from papertrade.exceptions import HttpError

BACKUP = "0x4444444444444444444444444444444444444444"
A = SimpleNamespace(address="0x1111111111111111111111111111111111111111")


def _pos(i):
    return OpenPosition(i, 0, True, 1, Decimal(100), 100, 1, 1, None)


def _snap(free=Decimal(0), positions=(), ops=(), locked=Decimal(0)):
    return WalletSnapshot(A.address, free + locked, free, locked, Decimal(0), Decimal(0),
                          open_positions=list(positions), pending_ops=list(ops))


def _op(action, iid="0x" + "ab" * 32, close_ids=()):
    return PendingOp(iid, action, 0, 0, close_ids=tuple(close_ids))


class World:
    """A tiny fake exchange+chain: withdrawals above `cap` are rejected, accepted ones land on EVM instantly."""

    def __init__(self, free, positions=(), cap=None):
        self.free, self.positions, self.cap = Decimal(free), list(positions), cap
        self.evm, self.sent, self.withdrawn, self.closed, self.cancelled = Decimal(0), [], [], [], []

    def snapshot(self, _addr, **_k):
        return _snap(self.free, self.positions, locked=Decimal(100) * len(self.positions))

    def client(self, _w):
        w = self

        class Wr:
            def close_positions(self, ids, dry_run=False):
                w.closed.append(list(ids))
                w.free += Decimal(100) * len(w.positions)
                w.positions = []

            def cancel_intent(self, iid, dry_run=False):
                w.cancelled.append(iid)

            def withdraw_to_evm(self, amt, dry_run=False):
                amt = Decimal(amt)
                if amt + 1 > w.free or (w.cap is not None and amt > w.cap):      # $1 fee must fit (measured live)
                    raise HttpError(409, {"code": "admission_rejected"})
                w.withdrawn.append(amt)
                w.free -= amt + 1
                w.evm += amt - Decimal("0.1")

        return SimpleNamespace(write=Wr())


def _panic(world, **kw):
    p = P.Panic({"A": A}, backup=BACKUP, log=lambda m: None, notify=None, sleep=lambda s: None,
                make_client=world.client, **kw)
    p.snapshot = world.snapshot
    p.usdc_balance = lambda _a: world.evm

    def transfer(_w, to, amt):
        assert to == BACKUP                                    # the ONLY destination
        world.sent.append(Decimal(amt))
        world.evm -= Decimal(amt)
        return "0xtx"

    p.transfer = transfer
    return p


@pytest.fixture(autouse=True)
def _flag_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "FLAG_FILE", tmp_path / "panic.flag")
    monkeypatch.setattr(P, "REPORT_FILE", tmp_path / "panic_report.json")
    monkeypatch.setattr(P, "set_flag", lambda reason, path=tmp_path / "panic.flag": path.write_text(json.dumps({"reason": reason})))


def test_candidates_descend_to_the_minimum():
    c = P.withdraw_candidates(Decimal("234.2"))
    assert c[0] == Decimal("234.200000") and c[-1] == Decimal(10) and c == sorted(c, reverse=True)
    assert P.withdraw_candidates(Decimal("10")) == [Decimal("10.000000")]
    assert P.withdraw_candidates(Decimal("9.99")) == []


def test_flatten_withdraw_sweep_happy_path(tmp_path):
    w = World(free="37.46", positions=[_pos(1), _pos(2)])
    rep = _panic(w).run("test")
    assert w.closed == [[1, 2]]                                # both legs, one intent
    assert w.withdrawn == [Decimal("236.460000")]              # free 237.46 - the $1 fee: everything in ONE payment
    assert sum(w.sent) == sum(w.withdrawn) - Decimal("0.1") * len(w.withdrawn)
    assert rep["ok"] and rep["wallets"]["A"]["open_positions"] == 0
    assert (tmp_path / "panic.flag").exists()                  # farmer is blocked afterwards


def test_rejected_amounts_fall_back_to_smaller_chunks_until_everything_is_out():
    w = World(free="100", cap=Decimal("40"))                   # exchange refuses anything above $40 at a time
    rep = _panic(w).run("test")
    assert all(x <= Decimal("40") for x in w.withdrawn) and len(w.withdrawn) >= 3
    assert w.free < Decimal(11)                                # only sub-minimum dust left (< $10 + $1 fee)
    assert rep["ok"] and sum(w.sent) > Decimal("85")


def test_balance_that_cannot_cover_minimum_plus_fee_is_reported_not_forced():
    w = World(free="10.5")                                     # 10.5 - 1.00 fee = 9.5 < the $10 minimum
    rep = _panic(w).run("test")
    assert w.withdrawn == [] and rep["wallets"]["A"]["left_on_exchange"] == "10.5"


def test_queued_open_is_cancelled_and_flat_confirmed_twice():
    w = World(free="50")
    calls = {"n": 0}
    base = w.snapshot

    def snap(addr, **k):
        calls["n"] += 1
        s = base(addr)
        if calls["n"] == 1:
            s.pending_ops.append(_op("openPosition", "0x" + "cd" * 32))
        return s

    p = _panic(w)
    p.snapshot = snap
    assert p.flatten("A", A) is True
    assert w.cancelled == ["0x" + "cd" * 32] and calls["n"] >= 3


def test_close_is_not_resent_while_queued():
    w = World(free="0", positions=[_pos(7)])
    n = {"i": 0}

    def snap(_a, **_k):
        n["i"] += 1
        if n["i"] <= 2:
            return _snap(positions=[_pos(7)], ops=[_op("closePositions", close_ids=[7])])
        w.positions = []
        return _snap()

    p = _panic(w)
    p.snapshot = snap
    assert p.flatten("A", A)
    assert w.closed == []                                      # the queued close was trusted, not duplicated


def test_dry_run_changes_nothing(tmp_path):
    w = World(free="237", positions=[_pos(1)])
    _panic(w, dry_run=True).run("test")
    assert w.closed == [] and w.withdrawn == [] and w.sent == [] and not (tmp_path / "panic.flag").exists()


def test_test_mode_moves_only_the_given_amount_without_flag_or_flatten(tmp_path):
    w = World(free="200", positions=[_pos(1), _pos(2)])
    _panic(w, test_amount=Decimal(10)).run("path test")
    assert w.withdrawn == [Decimal(10)] and w.closed == [] and sum(w.sent) == Decimal("9.9")
    assert not (tmp_path / "panic.flag").exists()


def test_refuses_backup_equal_to_own_wallet():
    with pytest.raises(ValueError):
        P.Panic({"A": A}, backup=A.address, log=lambda m: None, notify=None)


def test_backup_file_is_validated(tmp_path):
    f = tmp_path / "b"
    f.write_text("not-an-address\n")
    with pytest.raises(ValueError):
        P.load_backup(f)
    f.write_text(BACKUP.lower() + "\n")
    assert P.load_backup(f) == BACKUP                          # checksummed


def test_shipped_backup_address_is_the_one_the_user_gave():
    assert P.load_backup() == BACKUP


def test_flag_blocks_farmer_and_can_be_cleared(tmp_path):
    f = tmp_path / "x.flag"
    assert P.flag_active(f) is None
    P.set_flag("r", f)
    assert P.flag_active(f)["reason"] == "r" and P.clear_flag(f) and P.flag_active(f) is None


# ── RiskGuard ──

def test_riskguard_triggers_on_drawdown_and_explains_withdrawals(tmp_path):
    t = {"now": 1_000.0}
    g = RiskGuard(tmp_path / "r.json", max_drawdown=0.4, clock=lambda: t["now"])
    none = lambda ts: Decimal(0)                                # noqa: E731
    assert g.evaluate(Decimal(240), none)[0] is False
    t["now"] += 60
    assert g.evaluate(Decimal(200), none)[0] is False           # -17%: normal operating cost
    t["now"] += 60
    hit, why = g.evaluate(Decimal(140), none)                   # -42%
    assert hit and "42%" in why
    t["now"] += 60
    assert g.evaluate(Decimal(140), lambda ts: Decimal(100))[0] is False    # $100 was withdrawn on purpose


def test_riskguard_deposits_raise_the_peak_and_dust_is_ignored(tmp_path):
    t = {"now": 1.0}
    g = RiskGuard(tmp_path / "r.json", max_drawdown=0.4, clock=lambda: t["now"])
    for eq in (100, 500):
        t["now"] += 10
        assert g.evaluate(Decimal(eq), lambda ts: Decimal(0))[0] is False
    t["now"] += 10
    assert g.evaluate(Decimal(25), lambda ts: Decimal(0))[0] is False        # below watch threshold: nothing left to protect


def test_riskguard_persists_samples(tmp_path):
    g = RiskGuard(tmp_path / "r.json", clock=lambda: 5_000.0)
    g.evaluate(Decimal(200), lambda ts: Decimal(0))
    assert RiskGuard(tmp_path / "r.json", clock=lambda: 5_001.0).peak() == (5000.0, 200.0)


def test_full_balance_withdrawal_pays_the_fee_once():
    w = World(free="234.20")
    _panic(w).run("test")
    assert len(w.withdrawn) == 1 and w.free == 0 and sum(w.sent) == Decimal("233.20") - Decimal("0.1")
