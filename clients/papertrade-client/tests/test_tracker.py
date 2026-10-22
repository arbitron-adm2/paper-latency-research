import json
from decimal import Decimal
from unittest.mock import patch

import pytest

from bot.live import OpenPosition, PendingOp, WalletSnapshot
from bot import tracker as T
from bot.tracker import (diff_positions, msg_cycle, msg_liq, msg_open, msg_stake, pos_to_dict,
                         render_dashboard, risk_dot)

E18 = 10**18


def _p(pid, long=True, lev=1000, margin=10):
    return pos_to_dict(OpenPosition(pid, 0, long, 830305, Decimal(margin), lev, 829869, 1_000_000, None))


def _snap(positions=(), staked=0, ops=(), bal=0):
    return WalletSnapshot("x", Decimal(bal), Decimal(0), Decimal(10 * len(positions)), Decimal(0), Decimal(staked),
                          open_positions=[OpenPosition(p["id"], 0, p["long"], p["entry"], Decimal(p["margin"]), p["lev"],
                                                       p["bust"], p["opened"], None) for p in positions],
                          pending_ops=list(ops))


def _trade(kind, pnl, paper=0, fee: float = 0, ts=1_000_000 + 23 * 60000):
    return {"kind": kind, "timestampMs": ts, "adjustedPnlRaw": str(int(pnl * E18)),
            "paperMintedRaw": str(int(paper * E18)), "userPaidFeeRaw": str(int(fee * E18))}


# ── pure ──────────────────────────────────────────────────────────────

def test_diff():
    opened, missing = diff_positions({"1": _p(1), "2": _p(2, False)}, [_p(2, False), _p(3)])
    assert [p["id"] for p in opened] == [3] and [p["id"] for p in missing] == [1]


def test_risk_dot_is_scale_free():
    assert risk_dot(0.0001, 1000) == "🔴" and risk_dot(0.0009, 1000) == "🟢"
    assert risk_dot(0.001, 100) == "🔴" and risk_dot(0.009, 100) == "🟢"      # same ratio, other leverage


def test_messages():
    assert "Хедж открыт" in msg_open([("A", _p(1)), ("B", _p(2, False))])
    liq = msg_liq("B", _p(2, False), _trade("Liquidated", -10, 431))
    assert "Ликвидация B SHORT" in liq and "−$10.00" in liq and "431 PAPER" in liq
    assert "Застейкано A" in msg_stake("A", Decimal(431), Decimal(431))
    assert "Снято" in msg_stake("A", Decimal(-5), Decimal(10))


def test_cycle_summary_totals():
    items = [{"label": "B", "pos": _p(2, False), "trade": _trade("Liquidated", -10, 431)},
             {"label": "A", "pos": _p(1), "trade": _trade("PositionClosed", 5.2, 0, 0.1, ts=1_000_000 + 24 * 60000)}]
    m = msg_cycle(3, items, 0)
    assert "Цикл #3" in m and "24м" in m and "−$4.90" in m and "+431 PAPER" in m and "комиссия $0.10" in m


def test_dashboard_render():
    legs = [_p(1)]
    w = [{"label": "A", "snap": _snap(legs, staked=100), "legs": legs}, {"label": "B", "snap": _snap(), "legs": []}]
    d = render_dashboard(w, 83001.5, {"ok": True}, {"cycles": 2, "paper": 862, "pnl": -9.6}, 0)
    assert "BTC 83,001.5" in d and "LONG BTC</b> x1000" in d and "до ликв." in d and "нет позиций" in d
    assert "циклов 2" in d and "+862" in d and "фармер: ✅" in d
    assert "ОСТАНОВЛЕН" in render_dashboard(w, None, {"halted": True}, {}, 0)
    assert "не отвечает" in render_dashboard(w, None, None, {}, 0)
    assert "в очереди: openPosition" in render_dashboard(
        [{"label": "A", "snap": _snap(ops=[PendingOp("0x1", "openPosition", 0, 0)]), "legs": []}], None, {}, {}, 10)


# ── stateful tracker (telegram + exchange mocked) ─────────────────────

@pytest.fixture
def tr(tmp_path):
    (tmp_path / "cfg.json").write_text(json.dumps({"token": "T", "owner_chat_id": 42}))
    for k in "ab":
        (tmp_path / f"{k}.json").write_text(json.dumps({"address": f"0x{k}"}))
    t = T.Tracker(tmp_path / "cfg.json", {"A": tmp_path / "a.json", "B": tmp_path / "b.json"}, tmp_path / "st.json")
    t.sent, t.api_calls = [], []                                           # type: ignore[attr-defined]
    t.send = lambda text, silent=False: (t.sent.append(text), 100 + len(t.sent))[1]       # type: ignore
    t._api = lambda method, **kw: (t.api_calls.append((method, kw)), {"ok": True, "result": {}})[1]  # type: ignore
    return t


def _run(t, snaps, trades=None, fh=None):
    with patch("bot.tracker.get_snapshot", side_effect=lambda a, **_k: snaps[a]), \
         patch("bot.tracker.find_trade", side_effect=lambda a, pid: (trades or {}).get((a, pid))), \
         patch("bot.tracker.mid_price", return_value=83000.0), \
         patch("bot.tracker.farmer_health", return_value=fh if fh is not None else {"ok": True}):
        t.tick()


def test_first_run_adopts_silently_then_notifies_new_hedge(tr):
    _run(tr, {"0xa": _snap([_p(1)]), "0xb": _snap([_p(2, False)])})
    assert not any("Хедж открыт" in m for m in tr.sent)                  # adopted, not announced
    _run(tr, {"0xa": _snap([_p(1), _p(5)]), "0xb": _snap([_p(2, False)])})
    assert sum("Хедж открыт" in m for m in tr.sent) == 1


def test_single_empty_snapshot_does_not_fake_a_close(tr):
    full = {"0xa": _snap([_p(1)]), "0xb": _snap([_p(2, False)])}
    _run(tr, full)
    _run(tr, {"0xa": _snap(), "0xb": _snap([_p(2, False)])})             # glitch: A empty once
    _run(tr, full)                                                       # back
    assert tr.state["pending_close"] == {} and tr.state["stats"]["cycles"] == 0
    assert not any("Ликвидация" in m for m in tr.sent)


def test_full_cycle_liquidation_then_summary(tr):
    full = {"0xa": _snap([_p(1)]), "0xb": _snap([_p(2, False)])}
    _run(tr, full)
    trades = {("0xb", 2): _trade("Liquidated", -10, 431), ("0xa", 1): _trade("PositionClosed", 5.2, 0, 0.1)}
    gone_b = {"0xa": _snap([_p(1)]), "0xb": _snap()}
    _run(tr, gone_b, trades)                                             # miss #1
    _run(tr, gone_b, trades)                                             # miss #2 -> liquidation alert
    assert sum("Ликвидация B SHORT" in m for m in tr.sent) == 1
    assert tr.state["stats"]["cycles"] == 0                              # A still open: cycle not done
    flat = {"0xa": _snap(), "0xb": _snap()}
    _run(tr, flat, trades)
    _run(tr, flat, trades)
    assert tr.state["stats"]["cycles"] == 1
    summary = [m for m in tr.sent if "Цикл #1" in m]
    assert len(summary) == 1 and "−$4.90" in summary[0] and "+431 PAPER" in summary[0]
    assert round(tr.state["stats"]["paper"]) == 431
    _run(tr, flat, trades)
    assert sum("Цикл #1" in m for m in tr.sent) == 1                     # not repeated


def test_stake_notification_on_change_only(tr):
    _run(tr, {"0xa": _snap(staked=0), "0xb": _snap(staked=0)})
    _run(tr, {"0xa": _snap(staked=0), "0xb": _snap(staked=0)})
    assert not any("Застейкано" in m or "Снято со стейка" in m for m in tr.sent)
    _run(tr, {"0xa": _snap(staked=431), "0xb": _snap(staked=0)})
    assert sum("Застейкано A" in m for m in tr.sent) == 1
    _run(tr, {"0xa": _snap(staked=431), "0xb": _snap(staked=0)})
    assert sum("Застейкано" in m for m in tr.sent) == 1


def test_farmer_halt_alert_once(tr):
    ok = {"0xa": _snap(), "0xb": _snap()}
    _run(tr, ok)
    _run(tr, ok, fh={"halted": True})
    _run(tr, ok, fh={"halted": True})
    assert sum("halt" in m for m in tr.sent) == 1


def test_dashboard_created_pinned_then_edited_in_place(tr):
    ok = {"0xa": _snap(), "0xb": _snap()}
    _run(tr, ok)
    assert tr.state["dash_id"] is not None
    assert [m for m, _ in tr.api_calls].count("pinChatMessage") == 1
    first_id = tr.state["dash_id"]
    tr.api_calls.clear()
    _run(tr, {"0xa": _snap([_p(1)]), "0xb": _snap()})                    # content changed
    methods = [m for m, _ in tr.api_calls]
    assert "editMessageText" in methods and "pinChatMessage" not in methods
    assert tr.state["dash_id"] == first_id                               # same message, not a new one


def test_dashboard_unchanged_text_makes_no_api_call(tr):
    ok = {"0xa": _snap(), "0xb": _snap()}
    _run(tr, ok)
    tr.api_calls.clear()
    with patch("bot.tracker.render_dashboard", return_value=tr.last_dash):
        _run(tr, ok)
    assert [m for m, _ in tr.api_calls if m == "editMessageText"] == []


def test_deleted_dashboard_is_recreated_and_repinned(tr):
    ok = {"0xa": _snap(), "0xb": _snap()}
    _run(tr, ok)
    old = tr.state["dash_id"]
    tr._api = lambda method, **kw: ({"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"}  # type: ignore
                                    if method == "editMessageText" else tr.api_calls.append((method, kw)) or {"ok": True, "result": {}})
    _run(tr, {"0xa": _snap([_p(1)]), "0xb": _snap()})
    assert tr.state["dash_id"] != old
    assert any(m == "pinChatMessage" for m, _ in tr.api_calls)


def test_not_modified_is_not_an_error(tr):
    _run(tr, {"0xa": _snap(), "0xb": _snap()})
    dashboards = lambda: sum("PaperTrade</b>" in m for m in tr.sent)       # noqa: E731
    assert dashboards() == 1
    tr._api = lambda _method, **_kw: {"ok": False, "error_code": 400, "description": "Bad Request: message is not modified"}  # type: ignore
    _run(tr, {"0xa": _snap([_p(1)]), "0xb": _snap()})
    assert dashboards() == 1                                             # no duplicate dashboard created


def test_find_trade_requests_lowercase_address():
    """Regression: checksummed address -> exchange answered 400 -> every close was reported as 'no record'."""
    seen = {}

    class R:
        def raise_for_status(self): pass
        def json(self): return {"items": [{"positionId": "7", "kind": "Liquidated"}]}

    def fake_get(url, timeout):
        seen["url"] = url
        return R()

    with patch("bot.tracker.httpx.get", side_effect=fake_get):
        assert T.find_trade("0x1111111111111111111111111111111111111111", 7)["kind"] == "Liquidated"
    assert seen["url"].endswith("/0x1111111111111111111111111111111111111111/trades")


def test_net_pnl_subtracts_fee():
    t = _trade("PositionClosed", 6.028, 0, 0.1206)
    assert T.net_pnl(t) == Decimal("5.9074") or abs(T.net_pnl(t) - Decimal("5.9074")) < Decimal("0.0001")
