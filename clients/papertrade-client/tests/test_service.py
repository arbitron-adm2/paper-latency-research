"""reconcile() safety logic + HTTP control API of the farmer service."""
from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from decimal import Decimal
from unittest.mock import MagicMock, patch, create_autospec

import pytest

from bot.config import BotConfig
from bot.farmer import BotHalt, PaperFarmer
from bot.live import OpenPosition, WalletSnapshot
from bot.logger import BotLogger
from bot.service import FarmerService
from bot.state import BotState, SlotInfo, SlotState


def _snap(positions):
    return WalletSnapshot("x", Decimal(0), Decimal(0), Decimal(0), Decimal(0), Decimal(0), open_positions=positions)


def _pos(opened=1_000_000, long=True):
    return OpenPosition(1, 0, long, 1, Decimal(10), 1000, 1, opened, None)


@pytest.fixture
def farmer(tmp_path):
    cfg = BotConfig(wallet_a_path=tmp_path / "a", wallet_b_path=tmp_path / "b", dry_run=False)
    state = BotState(
        slot_a=SlotInfo(label="A", address="0xa", side="LONG"),
        slot_b=SlotInfo(label="B", address="0xb", side="SHORT"),
    )
    f = PaperFarmer(cfg, create_autospec(BotLogger, instance=True), state)
    f.monitor = MagicMock()
    f.handle_bust = MagicMock()
    f._flatten = MagicMock()
    return f


def _live(f, a, b):
    m = {"0xa": _snap(a), "0xb": _snap(b)}
    return patch("bot.live.get_snapshot", side_effect=lambda addr, **k: m[addr])


def test_reconcile_adopts_both_open(farmer):
    with _live(farmer, [_pos(long=True)], [_pos(long=False)]):
        assert farmer.reconcile() == "hedged"
    assert farmer.state.slot_a.state == SlotState.OPEN and farmer.state.slot_a.side == "LONG"
    assert farmer.state.slot_b.state == SlotState.OPEN and farmer.state.slot_b.side == "SHORT"


def test_reconcile_both_flat_resets_open_slots(farmer):
    farmer.state.slot_a.state = farmer.state.slot_b.state = SlotState.OPEN
    with _live(farmer, [], []):
        assert farmer.reconcile() == "flat"
    assert farmer.state.slot_a.state == SlotState.IDLE


def test_reconcile_single_leg_with_liquidation_handles_bust(farmer):
    farmer.monitor.detect_bust.return_value = {"kind": "Liquidated"}
    with _live(farmer, [_pos(long=True)], []):
        assert farmer.reconcile() == "bust_handled"
    farmer.handle_bust.assert_called_once_with("b")


def test_reconcile_naked_leg_grace_then_flatten_and_halt(farmer):
    farmer.monitor.detect_bust.return_value = None
    with _live(farmer, [], [_pos(long=False)]):
        assert farmer.reconcile() == "orphan_pending"
        farmer._flatten.assert_not_called()
        farmer._orphan_since -= 100  # grace (90s) elapsed
        with pytest.raises(BotHalt):
            farmer.reconcile()
    farmer._flatten.assert_called_once_with("b")


def test_reconcile_dry_run_is_noop(farmer):
    farmer.config.dry_run = True
    assert farmer.reconcile() == "dry"


# ── HTTP API ───────────────────────────────────────────────────────────

@pytest.fixture
def api(tmp_path):
    f = MagicMock()
    f.config = BotConfig(wallet_a_path=tmp_path / "a", wallet_b_path=tmp_path / "b", poll_interval_s=1)
    f.state = BotState(
        slot_a=SlotInfo(label="A", address="0xa", side="LONG"),
        slot_b=SlotInfo(label="B", address="0xb", side="SHORT"),
    )
    f.config.poll_interval_s = 0.05  # type: ignore[assignment]
    f.poll_interval.return_value = 0.05   # MagicMock's float() is 1.0: would make the loop tick once a second
    f.reconcile.return_value = "hedged"
    f.flatten_all.return_value = {"a": [1], "b": [2]}
    svc: FarmerService = FarmerService(f, tmp_path / "bot.log", token="secret", port=0)
    svc.base = ""  # type: ignore[attr-defined]
    with patch("bot.live.get_snapshot", side_effect=lambda addr, **k: _snap([])):
        t = threading.Thread(target=svc.serve, daemon=True)
        t.start()
        for _ in range(100):
            if hasattr(svc, "httpd"):
                break
            time.sleep(0.02)
        svc.base = f"http://127.0.0.1:{svc.httpd.server_address[1]}"
        yield svc, f
        svc.do("stop")
        t.join(timeout=5)


def _call(svc, path, method="GET", token: "str | None" = "secret"):
    req = urllib.request.Request(svc.base + path, method=method, data=b"" if method == "POST" else None)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_api_auth(api):
    svc, _ = api
    assert _call(svc, "/health", token=None)[0] == 200
    assert _call(svc, "/status", token=None)[0] == 401
    assert _call(svc, "/status", token="wrong")[0] == 401
    assert _call(svc, "/pause", "POST", token=None)[0] == 401


def test_api_status_pause_resume(api):
    svc, f = api
    code, st = _call(svc, "/status")
    assert code == 200 and st["mode"] == "LIVE" and st["paused"] is False
    assert set(st["wallets"]) == {"A", "B"}
    assert _call(svc, "/pause", "POST")[1]["paused"] is True
    time.sleep(0.3)  # let at least one tick run while paused
    assert f.run_once.call_args.kwargs["allow_open"] is False
    assert _call(svc, "/resume", "POST")[1]["paused"] is False
    assert _call(svc, "/nope", "POST")[0] == 404


def test_api_flatten_pauses(api):
    svc, f = api
    code, out = _call(svc, "/flatten", "POST")
    assert code == 200 and out["closed"] == {"a": [1], "b": [2]} and out["paused"] is True
    f.flatten_all.assert_called_once()


def test_botholt_halts_but_keeps_monitoring(api):
    svc, f = api
    f.run_once.side_effect = BotHalt("test halt")
    time.sleep(0.4)
    assert svc.halted == "test halt"
    n = f.reconcile.call_count
    time.sleep(0.3)
    assert f.reconcile.call_count > n  # still monitoring while halted
    assert _call(svc, "/health", token=None)[1]["halted"] is True
    f.run_once.side_effect = None  # underlying problem fixed
    _call(svc, "/resume", "POST")
    time.sleep(0.2)
    assert svc.halted is None


def test_api_stake_runs_staker_under_lock(api):
    svc, f = api
    f.staker.claim_and_stake_both.return_value = {"a_paper": Decimal("1500"), "total": Decimal("1500")}
    code, out = _call(svc, "/stake", "POST")
    assert code == 200 and out["staked"]["a_paper"] == "1500"
    f.staker.claim_and_stake_both.assert_called_once()
    assert _call(svc, "/stake", "POST", token=None)[0] == 401


def test_status_shows_mirrored_slot_once(api):
    svc, f = api
    f.state.slot_b.address = f.state.slot_a.address          # single-account mode
    svc._status_cache = None
    code, st = _call(svc, "/status")
    assert code == 200 and set(st["wallets"]) == {"A"}


def test_loop_survives_a_broken_poll_interval(api):
    svc, f = api
    f.poll_interval.side_effect = RuntimeError("boom")
    assert svc._interval() == 30.0                       # falls back instead of raising inside the loop thread
    f.poll_interval.side_effect = None
    f.poll_interval.return_value = 0.05
    assert svc._interval() == 0.05
    f.poll_interval.return_value = 99999                 # nonsense value is clamped back to the configured interval
    assert svc._interval() == f.config.poll_interval_s
    assert any(t.name == "farmer-loop" and t.is_alive() for t in threading.enumerate())
