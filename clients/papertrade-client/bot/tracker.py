"""
PaperTrade tracker: ONE pinned dashboard message that is edited in place + short event notifications.

Read-only: loads wallet ADDRESSES only (never keys), cannot trade.

Dashboard (pinned, edited every POLL_S, silent): price, both legs with distance to liquidation,
equity, PAPER, queued intents, farmer service health, cumulative stats.
Notifications (new messages): hedge opened, liquidation, cycle summary, staking, farmer halted,
intent stuck in queue, API down/up.
Commands (owner chat only): /status refresh dashboard, /pin re-create + re-pin, /help.

    python -m bot.tracker          # run
    python -m bot.tracker --once   # refresh the dashboard once and exit
"""
from __future__ import annotations

import html
import json
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

import httpx

from bot.live import BASE, WalletSnapshot, get_snapshot

E18 = Decimal(10) ** 18
POLL_S = 15
CLOSE_LOOKUP_S = 120      # wait for the trade record of a vanished position
STUCK_OP_S = 90           # alert: intent queued longer than this
API_DOWN_AFTER = 4        # failed polls in a row before the "API down" alert
ABSENT_POLLS = 2          # a position must be missing from 2 consecutive snapshots to count as gone
STAKE_MIN = Decimal("1")  # ignore staked-balance changes smaller than this (PAPER)
FARMER_HEALTH = "http://127.0.0.1:8787/health"

SYMBOL = {0: "BTC", 1: "ETH"}
SCALE = {0: 10, 1: 100}   # raw price / scale = USD


# ── formatting ────────────────────────────────────────────────────────────

def px(raw: int, inst: int) -> float | None:
    return raw / SCALE[inst] if inst in SCALE else None


def fmt_px(v: float | None) -> str:
    return "?" if v is None else f"{v:,.1f}"


def fmt_usd(v: float | Decimal, sign: bool = False) -> str:
    v = float(v)
    s = f"{abs(v):,.2f}"
    if sign:
        return ("+" if v >= 0 else "−") + "$" + s
    return ("−" if v < 0 else "") + "$" + s


def fmt_dur(ms: float) -> str:
    m = max(0, int(ms // 60000))
    return f"{m // 60}ч {m % 60:02d}м" if m >= 60 else f"{m}м"


def risk_dot(dist: float, lev: int) -> str:
    """Distance to bust relative to the full 1/leverage band: scale-free traffic light."""
    ratio = dist / (1 / lev) if lev else 1
    return "🔴" if ratio < 0.15 else "🟠" if ratio < 0.4 else "🟢"


# ── pure logic (unit-tested) ──────────────────────────────────────────────

def pos_to_dict(p) -> dict:
    return {"id": p.position_id, "inst": p.instrument_id, "long": p.is_long, "lev": p.leverage,
            "margin": float(p.margin), "entry": p.entry_price_raw, "bust": p.bust_price_raw,
            "opened": p.opened_at_ms}


def diff_positions(known: dict[str, dict], current: list[dict]) -> tuple[list[dict], list[dict]]:
    """known: {position_id(str): dict}. Returns (opened, missing-now)."""
    cur = {str(p["id"]): p for p in current}
    return [p for k, p in cur.items() if k not in known], [p for k, p in known.items() if k not in cur]


def leg_line(p: dict, now: float | None) -> str:
    sym = SYMBOL.get(p["inst"], f"#{p['inst']}")
    e, b = px(p["entry"], p["inst"]), px(p["bust"], p["inst"])
    head = f"<b>{'LONG' if p['long'] else 'SHORT'} {sym}</b> x{p['lev']} · {fmt_usd(p['margin'])}"
    lines = [head, f"   вход {fmt_px(e)} → ликв. {fmt_px(b)}"]
    if now and e and b:
        dist = abs(now - b) / now
        upnl = ((now - e) / e if p["long"] else (e - now) / e) * p["margin"] * p["lev"]
        lines.append(f"   {risk_dot(dist, p['lev'])} до ликв. {dist * 100:.3f}% · PnL {fmt_usd(upnl, True)}")
    return "\n".join(lines)


def msg_open(legs: list[tuple[str, dict]]) -> str:
    out = ["🟢 <b>Хедж открыт</b>"]
    for label, p in legs:
        sym = SYMBOL.get(p["inst"], f"#{p['inst']}")
        out.append(f"{label}: {'LONG' if p['long'] else 'SHORT'} {sym} x{p['lev']} · {fmt_usd(p['margin'])}"
                   f" · вход {fmt_px(px(p['entry'], p['inst']))}")
    return "\n".join(out)


def msg_liq(label: str, p: dict, trade: dict | None) -> str:
    side = "LONG" if p["long"] else "SHORT"
    if trade is None:
        return f"💥 <b>{label} {side}</b> позиция исчезла, запись сделки ещё не проиндексирована"
    pnl = Decimal(trade["adjustedPnlRaw"]) / E18
    paper = Decimal(trade.get("paperMintedRaw", "0")) / E18
    return f"💥 <b>Ликвидация {label} {side}</b> · {fmt_usd(pnl, True)} · +{paper:,.0f} PAPER"


def trade_summary(label: str, p: dict, trade: dict | None) -> str:
    side = "LONG" if p["long"] else "SHORT"
    if trade is None:
        return f"{label} {side}: закрыта (нет записи сделки)"
    pnl = Decimal(trade["adjustedPnlRaw"]) / E18
    paper = Decimal(trade.get("paperMintedRaw", "0")) / E18
    fee = Decimal(trade.get("userPaidFeeRaw", "0")) / E18
    if trade["kind"] == "Liquidated":
        return f"{label} {side}: 💥 ликвидирована {fmt_usd(pnl, True)} · +{paper:,.0f} PAPER"
    extra = f" · комиссия {fmt_usd(fee)}" if fee else ""
    return f"{label} {side}: ✅ закрыта {fmt_usd(pnl, True)}{extra}"


def net_pnl(trade: dict | None) -> Decimal:
    """Realised result after fees. `adjustedPnlRaw` excludes the fee (verified: 10 + 6.028 - 0.1206 == exchange balance)."""
    if not trade:
        return Decimal(0)
    return (Decimal(trade["adjustedPnlRaw"]) - Decimal(trade.get("userPaidFeeRaw", "0"))) / E18


def msg_cycle(n: int, items: list[dict], now_ms: float) -> str:
    pnl = Decimal(0)
    paper = Decimal(0)
    rows = []
    for it in items:
        t = it["trade"]
        if t:
            pnl += net_pnl(t)
            paper += Decimal(t.get("paperMintedRaw", "0")) / E18
        rows.append(trade_summary(it["label"], it["pos"], t))
    start = min((it["pos"]["opened"] for it in items if it["pos"]["opened"]), default=0)
    end = max((it["trade"]["timestampMs"] for it in items if it["trade"]), default=now_ms)
    dur = f" · {fmt_dur(end - start)}" if start else ""
    return (f"🔁 <b>Цикл #{n} завершён</b>{dur}\n" + "\n".join(rows) +
            f"\n<b>Итого после комиссий {fmt_usd(pnl, True)} · +{paper:,.0f} PAPER</b>")


def msg_stake(label: str, delta: Decimal, total: Decimal) -> str:
    verb = "Застейкано" if delta > 0 else "Снято со стейка"
    return f"🪙 <b>{verb} {label}</b> {abs(delta):,.0f} PAPER · всего в стейке {total:,.0f}"


def render_dashboard(wallets: list[dict], price: float | None, farmer: dict | None, stats: dict, now_s: float) -> str:
    """wallets: [{label, snap|None, error|None, legs:[pos dict]}]"""
    head = "📊 <b>PaperTrade</b>"
    if price:
        head += f" · BTC {price:,.1f}"
    lines = [head, ""]
    for w in wallets:
        if w.get("error"):
            lines.append(f"<b>{w['label']}</b> ⚠ нет данных ({html.escape(w['error'][:60])})\n")
            continue
        s: WalletSnapshot = w["snap"]
        if w["legs"]:
            for p in w["legs"]:
                lines.append(f"<b>{w['label']}</b> " + leg_line(p, price))
        else:
            lines.append(f"<b>{w['label']}</b> — нет позиций")
        for o in s.pending_ops:
            lines.append(f"   ⏳ в очереди: {o.action} ({fmt_dur(now_s * 1000 - o.accepted_at_ms)})")
        lines.append("")
    eq = [f"{w['label']} {fmt_usd(w['snap'].balance + w['snap'].locked)}" for w in wallets if w.get("snap")]
    free = sum((w["snap"].available for w in wallets if w.get("snap")), Decimal(0))
    pap = sum((w["snap"].paper_balance for w in wallets if w.get("snap")), Decimal(0))
    stk = sum((w["snap"].paper_staked for w in wallets if w.get("snap")), Decimal(0))
    lines.append("💰 " + " · ".join(eq) + f" · свободно {fmt_usd(free)}")
    lines.append(f"🪙 PAPER {pap:,.0f} · в стейке {stk:,.0f}")
    lines.append(f"🔁 циклов {stats.get('cycles', 0)} · PAPER за всё время +{stats.get('paper', 0):,.0f}"
                 f" · PnL {fmt_usd(stats.get('pnl', 0), True)}")
    if farmer is None:
        lines.append("🤖 фармер: ⚠ не отвечает")
    elif farmer.get("halted"):
        lines.append("🤖 фармер: 🛑 ОСТАНОВЛЕН (halt), нужен /resume")
    elif farmer.get("paused"):
        lines.append("🤖 фармер: ⏸ пауза")
    else:
        lines.append("🤖 фармер: ✅ работает")
    lines.append(f"<i>обновлено {time.strftime('%H:%M:%S', time.localtime(now_s))}</i>")
    return "\n".join(lines)


# ── io ────────────────────────────────────────────────────────────────────

def mid_price(symbol: str = "BTC") -> float | None:
    try:
        r = httpx.post("https://api.hyperliquid.xyz/info", json={"type": "allMids"}, timeout=8)
        return float(r.json()[symbol])
    except Exception:  # noqa: BLE001
        return None


def farmer_health() -> dict | None:
    try:
        return httpx.get(FARMER_HEALTH, timeout=4).json()
    except Exception:  # noqa: BLE001
        return None


def find_trade(addr: str, position_id: int) -> dict | None:
    r = httpx.get(f"{BASE}/query/accounts/{addr.lower()}/trades", timeout=15)   # API rejects mixed-case (400)
    r.raise_for_status()
    return next((it for it in r.json().get("items", []) if int(it["positionId"]) == int(position_id)), None)


class Tracker:
    def __init__(self, cfg_path: Path, wallets: dict[str, Path], state_path: Path):
        cfg = json.loads(cfg_path.read_text())
        self.token, self.owner = cfg["token"], int(cfg["owner_chat_id"])
        self.addr = {k: json.loads(p.read_text())["address"] for k, p in wallets.items()}   # address ONLY
        self.state_path = state_path
        self.fresh = not state_path.exists()          # first ever run: adopt positions silently
        self.state: dict = json.loads(state_path.read_text()) if not self.fresh else {}
        for k, v in (("known", {}), ("pending_close", {}), ("absent", {}), ("staked", {}), ("cycle", []),
                     ("stats", {"cycles": 0, "paper": 0.0, "pnl": 0.0}), ("dash_id", None), ("farmer_halted", False)):
            self.state.setdefault(k, v)
        self.fail = 0
        self.api_down_notified = False
        self.stuck_notified: set[str] = set()
        self.last_dash = ""
        self.http = httpx.Client(timeout=40)
        self.io = threading.Lock()
        self.tick_lock = threading.RLock()   # main loop and /status handler must not interleave

    # ── telegram ──
    def _api(self, method: str, **params) -> dict:
        r = self.http.post(f"https://api.telegram.org/bot{self.token}/{method}", json=params)
        return r.json()

    def send(self, text: str, silent: bool = False) -> int | None:
        for _ in range(3):
            try:
                res = self._api("sendMessage", chat_id=self.owner, text=text, parse_mode="HTML",
                                disable_web_page_preview=True, disable_notification=silent)
                if res.get("ok"):
                    return res["result"]["message_id"]
            except Exception:  # noqa: BLE001
                pass
            time.sleep(2)
        return None

    def ensure_dashboard(self, text: str, force_new: bool = False) -> None:
        """Edit the pinned dashboard in place; recreate + pin it if it is gone or unpinned."""
        with self.io:
            mid = None if force_new else self.state.get("dash_id")
            if mid:
                if text == self.last_dash:
                    return
                res = self._api("editMessageText", chat_id=self.owner, message_id=mid, text=text,
                                parse_mode="HTML", disable_web_page_preview=True)
                desc = str(res.get("description", ""))
                if res.get("ok") or "not modified" in desc:
                    self.last_dash = text
                    return
                if res.get("error_code") == 429:
                    time.sleep(int(res.get("parameters", {}).get("retry_after", 3)))
                    return
                # message deleted / cannot be edited -> fall through and recreate
            new_id = self.send(text, silent=True)
            if new_id:
                self.state["dash_id"] = new_id
                self.last_dash = text
                self._api("pinChatMessage", chat_id=self.owner, message_id=new_id, disable_notification=True)

    def verify_pin(self) -> None:
        """If the user unpinned the dashboard, pin it again (checked rarely)."""
        mid = self.state.get("dash_id")
        if not mid:
            return
        chat = self._api("getChat", chat_id=self.owner).get("result", {})
        if chat.get("pinned_message", {}).get("message_id") != mid:
            self._api("pinChatMessage", chat_id=self.owner, message_id=mid, disable_notification=True)

    # ── watcher ──
    def tick(self) -> None:
        with self.tick_lock:
            self._tick()

    def _tick(self) -> None:
        first_run = self.fresh
        now_ms = time.time() * 1000
        ok_all = True
        views, opened_all = [], []
        for label, addr in self.addr.items():
            try:
                snap = get_snapshot(addr, retries=1, timeout_s=10)
            except Exception as exc:  # noqa: BLE001
                ok_all = False
                views.append({"label": label, "error": str(exc)})
                continue
            known = self.state["known"].setdefault(addr, {})
            current = [pos_to_dict(p) for p in snap.open_positions]
            opened, missing = diff_positions(known, current)
            for p in opened:
                known[str(p["id"])] = p
                opened_all.append((label, p))
            for p in current:                                   # reappeared: reset the miss counter
                self.state["absent"].pop(f"{addr}:{p['id']}", None)
            for p in missing:                                   # gone only after N consecutive misses
                ak = f"{addr}:{p['id']}"
                self.state["absent"][ak] = self.state["absent"].get(ak, 0) + 1
                if self.state["absent"][ak] >= ABSENT_POLLS:
                    del known[str(p["id"])]
                    del self.state["absent"][ak]
                    self.state["pending_close"][ak] = {"label": label, "addr": addr, "pos": p, "since": now_ms}
            views.append({"label": label, "snap": snap, "legs": current})
            # staking: change of the staked balance
            prev = self.state["staked"].get(addr)
            cur = snap.paper_staked
            self.state["staked"][addr] = str(cur)
            if prev is not None and not first_run and abs(cur - Decimal(prev)) >= STAKE_MIN:
                self.send(msg_stake(label, cur - Decimal(prev), cur))
            for o in snap.pending_ops:                          # stuck relayer intents
                age = (now_ms - o.accepted_at_ms) / 1000
                if age > STUCK_OP_S and o.intent_id not in self.stuck_notified:
                    self.stuck_notified.add(o.intent_id)
                    self.send(f"⏳ <b>{label}</b> заявка <code>{o.action}</code> висит в очереди {age:.0f} с")

        if opened_all and not first_run:
            self.send(msg_open(opened_all))

        # vanished positions -> liquidation alert now, cycle summary when everything is flat
        for key, item in list(self.state["pending_close"].items()):
            try:
                trade = find_trade(item["addr"], item["pos"]["id"])
            except Exception:  # noqa: BLE001
                trade = None
            if trade or now_ms - item["since"] > CLOSE_LOOKUP_S * 1000:
                if trade and trade["kind"] == "Liquidated":
                    self.send(msg_liq(item["label"], item["pos"], trade))
                self.state["cycle"].append({"label": item["label"], "pos": item["pos"], "trade": trade})
                del self.state["pending_close"][key]
        flat = ok_all and not self.state["pending_close"] and not any(self.state["known"].values())
        if flat and self.state["cycle"]:
            st = self.state["stats"]
            st["cycles"] += 1
            for it in self.state["cycle"]:
                t = it["trade"]
                if t:
                    st["pnl"] += float(net_pnl(t))
                    st["paper"] += float(Decimal(t.get("paperMintedRaw", "0")) / E18)
            self.send(msg_cycle(st["cycles"], self.state["cycle"], now_ms))
            self.state["cycle"] = []

        # farmer health + api health
        fh = farmer_health()
        halted = bool(fh and fh.get("halted"))
        if halted and not self.state["farmer_halted"] and not first_run:
            self.send("🛑 <b>Фармер остановлен (halt)</b>. Новые хеджи не открываются, позиции под контролем. "
                      "Проверь /status и `POST /resume`.")
        self.state["farmer_halted"] = halted
        self.fail = 0 if ok_all else self.fail + 1
        if self.fail >= API_DOWN_AFTER and not self.api_down_notified:
            self.api_down_notified = True
            self.send("🔴 API PaperTrade недоступен — трекер слепой до восстановления")
        elif ok_all and self.api_down_notified:
            self.api_down_notified = False
            self.send("🟢 API PaperTrade снова доступен")

        self.fresh = False
        self.ensure_dashboard(render_dashboard(views, mid_price("BTC"), fh, self.state["stats"], time.time()))
        self.state_path.write_text(json.dumps(self.state))

    # ── commands ──
    def poll_commands(self) -> None:
        offset = None
        while True:
            try:
                res = self._api("getUpdates", offset=offset, timeout=25, allowed_updates=["message"])
                for u in res.get("result", []):
                    offset = u["update_id"] + 1
                    m = u.get("message") or {}
                    if m.get("chat", {}).get("id") != self.owner:        # owner only
                        continue
                    cmd = (m.get("text") or "").split()[0].split("@")[0].lower() if m.get("text") else ""
                    if cmd in ("/status", "/pos"):
                        self.last_dash = ""                              # force an immediate refresh
                        self.tick()
                    elif cmd in ("/pin", "/start"):
                        self.last_dash = ""
                        self.state["dash_id"] = None if cmd == "/pin" else self.state.get("dash_id")
                        self.tick()
                    elif cmd == "/help":
                        self.send("PaperTrade трекер (только чтение)\n/status — обновить панель\n/pin — пересоздать и закрепить\n"
                                  "Уведомления: хедж открыт, ликвидация, цикл завершён, стейкинг, halt фармера, очередь, API.")
            except Exception:  # noqa: BLE001
                time.sleep(5)

    def run(self) -> None:
        threading.Thread(target=self.poll_commands, daemon=True).start()
        n = 0
        while True:
            try:
                self.tick()
                n += 1
                if n % 40 == 1:                                          # ~ every 10 min
                    self.verify_pin()
            except Exception as exc:  # noqa: BLE001
                print("tick error:", type(exc).__name__, exc, file=sys.stderr, flush=True)
            time.sleep(POLL_S)


def build() -> Tracker:
    root = Path(__file__).resolve().parent.parent
    return Tracker(root / "wallets" / "tg_bot.json",
                   {"A": root / "wallets" / "wallet_a.json", "B": root / "wallets" / "wallet_b.json"},
                   root / "tracker_state.json")


if __name__ == "__main__":
    t = build()
    if "--once" in sys.argv:
        t.tick()
    else:
        t.run()
