"""
doctor: one command that checks every moving part. Exit code 0 = all ok, 1 = warnings, 2 = critical.

    python run_bot.py doctor            # fast checks (~10 s), meant for every 15 minutes
    python run_bot.py doctor --deep     # + test suite, git cleanliness, unit files, disk (daily)
    python run_bot.py doctor --json     # machine readable (for the scheduled supervisor)

Every check returns {name, sev: OK|WARN|CRIT, msg}. A check that itself crashes is a WARN ("could not check"),
never a silent pass and never a false CRIT.
"""
from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import time
from decimal import Decimal
from pathlib import Path
from typing import Callable

import httpx
from web3 import Web3

ROOT = Path(__file__).resolve().parent.parent
API = "http://127.0.0.1:8787"
EXPECTED_BACKUP = "0x4444444444444444444444444444444444444444"
USDC = "0xb88339cb7199b77e23db6e890353e22632ba630f"
RPC = "https://rpc.hyperliquid.xyz/evm"
OK, WARN, CRIT = "OK", "WARN", "CRIT"


def _res(name: str, sev: str, msg: str, **extra) -> dict:
    return {"name": name, "sev": sev, "msg": msg, **extra}


def _sh(cmd: list[str], timeout: int = 20) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout.strip()


def _token() -> str:
    return (ROOT / "wallets" / "api_token").read_text().strip()


def _api(path: str, timeout: float = 40.0):
    return httpx.get(API + path, headers={"Authorization": f"Bearer {_token()}"}, timeout=timeout).json()


# ── 1. processes ─────────────────────────────────────────────────────────

def check_services(_ctx) -> list[dict]:
    out = []
    for unit in ("papertrade-farmer", "papertrade-tracker"):
        state = _sh(["systemctl", "--user", "is-active", f"{unit}.service"])
        info = dict(l.split("=", 1) for l in _sh(["systemctl", "--user", "show", f"{unit}.service", "-p",
                                                    "NRestarts", "-p", "MainPID", "-p", "ActiveEnterTimestamp"]).splitlines() if "=" in l)
        if state != "active":
            out.append(_res(f"service:{unit}", CRIT, f"{unit} is {state or 'unknown'}"))
            continue
        restarts = int(info.get("NRestarts", "0") or 0)
        sev = WARN if restarts >= 3 else OK
        rss = ""
        try:
            kb = next(int(l.split()[1]) for l in Path(f"/proc/{info['MainPID']}/status").read_text().splitlines() if l.startswith("VmRSS"))
            rss = f", {kb // 1024} MB"
            if kb > 600_000:
                sev = WARN
        except Exception:  # noqa: BLE001
            pass
        out.append(_res(f"service:{unit}", sev, f"active since {info.get('ActiveEnterTimestamp', '?')}, {restarts} restarts{rss}"))
    return out


def check_farmer_api(ctx) -> list[dict]:
    try:
        h = httpx.get(API + "/health", timeout=6).json()
    except Exception as exc:  # noqa: BLE001
        return [_res("farmer:api", CRIT, f"/health unreachable: {type(exc).__name__}")]
    out = []
    age = h.get("last_tick_age_s")
    out.append(_res("farmer:loop", CRIT if age is None or age > 180 else WARN if age > 60 else OK,
                    f"last tick {age if age is None else round(age)}s ago"))
    try:
        st = _api("/status")
        ctx["status"] = st
    except Exception as exc:  # noqa: BLE001
        return out + [_res("farmer:status", WARN, f"/status failed: {type(exc).__name__}: {exc}")]
    out.append(_res("farmer:mode", OK if st["mode"] == "LIVE" else WARN, f"{st['mode']}"))
    if st.get("halted"):
        out.append(_res("farmer:halt", CRIT, f"HALTED: {st['halted']}"))
    if st.get("paused"):
        out.append(_res("farmer:paused", WARN, "paused (no new hedges)" + (f"; panic flag: {st['panic_flag']}" if st.get("panic_flag") else "")))
    if st.get("consecutive_errors", 0) >= 2:
        out.append(_res("farmer:errors", WARN, f"{st['consecutive_errors']} consecutive tick errors; last: {st.get('last_error')}"))
    if st.get("economics_block"):
        out.append(_res("farmer:economics-gate", WARN, f"new hedges blocked: {st['economics_block']}"))
    return out


# ── 2. money & panic readiness ───────────────────────────────────────────

def check_panic_readiness(ctx) -> list[dict]:
    out = []
    try:
        raw = (ROOT / "wallets" / "backup_address").read_text().strip()
        ok = Web3.is_address(raw) and Web3.to_checksum_address(raw) == EXPECTED_BACKUP
        out.append(_res("panic:backup-address", OK if ok else CRIT,
                        "matches the address you gave" if ok else f"backup address file does not match the expected address: {raw[:12]}..."))
    except Exception as exc:  # noqa: BLE001
        out.append(_res("panic:backup-address", CRIT, f"backup address unreadable: {exc}"))
    bad = []
    for p in [ROOT / "wallets", *(ROOT / "wallets").glob("*")]:
        mode = stat.S_IMODE(p.stat().st_mode)
        if mode & 0o077:
            bad.append(f"{p.name}:{oct(mode)}")
    out.append(_res("secrets:permissions", OK if not bad else CRIT, "wallets/ is private (0700/0600)" if not bad else f"group/world accessible: {bad}"))
    flag = ROOT / "panic.flag"
    if flag.exists():
        out.append(_res("panic:flag", CRIT, f"panic.flag is SET: {flag.read_text()[:200]}"))
    ack = ROOT / "wallets" / "BACKUP_CONFIRMED"
    out.append(_res("secrets:seed-backup", OK if ack.exists() else WARN,
                    "seed backup confirmed by the owner" if ack.exists() else
                    "seed phrases of the wallets are NOT confirmed backed up (run `run_bot.py ack-backup` after saving wallets/wallet_a.json elsewhere)"))
    try:
        w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 10}))
        a = json.loads((ROOT / "wallets" / "wallet_a.json").read_text())["address"]
        hype = Decimal(w3.eth.get_balance(Web3.to_checksum_address(a))) / Decimal(10**18)
        ctx["hype"] = hype
        out.append(_res("gas:HYPE", CRIT if hype < Decimal("0.0003") else WARN if hype < Decimal("0.001") else OK,
                        f"{hype:.5f} HYPE on A (a panic sweep needs ~0.0001; a deposit ~0.00003)"))
        blk = dict(w3.eth.get_block("latest"))
        lag = time.time() - int(blk.get("timestamp", 0))
        out.append(_res("chain:HyperEVM", OK if lag < 30 else WARN, f"block {blk.get('number')}, {lag:.0f}s old"))
        evm = onchain_usdc(w3, a)
        out.append(_res("funds:stuck-on-evm", OK if evm < 1 else WARN if evm < 10 else CRIT,
                        f"{evm} USDC on A's HyperEVM address" + ("" if evm < 1 else " (auto-deposit should pick up >= $10)")))
    except Exception as exc:  # noqa: BLE001
        out.append(_res("chain:HyperEVM", WARN, f"RPC check failed: {type(exc).__name__}: {exc}"))
    return out


def onchain_usdc(w3, addr: str) -> Decimal:
    data = "0x70a08231" + Web3.to_checksum_address(addr)[2:].lower().zfill(64)
    return Decimal(int(w3.eth.call({"to": Web3.to_checksum_address(USDC), "data": data}).hex(), 16)) / Decimal(10**6)


def check_account(ctx) -> list[dict]:
    from bot.live import estimated_wait_s, get_mid, get_pressure, get_snapshot
    out = []
    addr = json.loads((ROOT / "wallets" / "wallet_a.json").read_text())["address"]
    t0 = time.time()
    s = get_snapshot(addr)
    ctx["snap"] = s
    out.append(_res("exchange:api", OK if time.time() - t0 < 8 else WARN, f"snapshot ok in {time.time() - t0:.1f}s"))
    equity = s.balance + s.locked
    out.append(_res("funds:equity", OK if equity >= 30 else WARN, f"equity ${equity:,.2f} (free ${s.available:,.2f}, in positions ${s.locked:,.2f})", equity=float(equity)))
    out.append(_res("exchange:session-key", OK if s.has_session_key(addr) else CRIT,
                    "session key active" if s.has_session_key(addr) else "SESSION KEY NOT ACTIVE: no intent can be signed"))
    stuck = [o for o in s.pending_ops if time.time() - o.accepted_at_ms / 1000 > 120]
    out.append(_res("exchange:queue", OK if not stuck else WARN,
                    "no intents waiting" if not s.pending_ops else f"{len(s.pending_ops)} queued"
                    + (f", {len(stuck)} older than 2 min: {[o.action for o in stuck]}" if stuck else "")))
    pr = get_pressure()
    if pr is None:
        out.append(_res("relayer:pressure", WARN, "pressure stream unreachable (farmer fails closed: no new hedges)"))
    else:
        est = estimated_wait_s(pr)
        out.append(_res("relayer:pressure", OK if est < 60 else WARN, f"~{est:.0f}s to drain, {pr.get('queuedIntents')} queued"))
    # positions
    longs = [p for p in s.open_positions if p.is_long]
    shorts = [p for p in s.open_positions if not p.is_long]
    mid = get_mid("BTC")
    out.append(_res("market:price-feed", OK if mid else WARN, f"BTC mid {mid}" if mid else "Hyperliquid price feed unreachable (threshold close cannot trigger; liquidation still works)"))
    if not s.open_positions:
        out.append(_res("positions", OK, "flat"))
    elif bool(longs) != bool(shorts):
        out.append(_res("positions:hedge", CRIT, f"NAKED LEG: {len(longs)} long / {len(shorts)} short open"))
    else:
        oldest = min(p.opened_at_ms for p in s.open_positions)
        age_h = (time.time() * 1000 - oldest) / 3.6e6
        worst = None
        if mid:
            for p in s.open_positions:
                k = 10 if p.instrument_id == 0 else 100
                bust = p.bust_price_raw / k
                entry = p.entry_price_raw / k
                room = abs(entry - bust)
                left = abs(mid - bust) / room if room else 1
                worst = left if worst is None else min(worst, left)
        sev = OK
        if worst is not None and worst < 0.15:
            sev = CRIT
        elif (worst is not None and worst < 0.3) or age_h > 24:
            sev = WARN
        out.append(_res("positions:hedge", sev, f"hedged {len(longs)}L/{len(shorts)}S, open {age_h:.1f}h"
                        + (f", closest leg has {worst * 100:.0f}% of its liquidation distance left" if worst is not None else "")))
    # staking
    if s.paper_balance >= 1000 and not s.pending("paperStake"):
        out.append(_res("staking", WARN, f"{s.paper_balance:,.0f} PAPER unstaked (>= 1000 minimum): auto-stake should have run"))
    else:
        out.append(_res("staking", OK, f"unstaked {s.paper_balance:,.0f}, staked {s.paper_staked:,.0f} PAPER"))
    # withdrawals: ours always go to our own address
    try:
        items = httpx.get(f"https://exchange.papertrade.xyz/query/accounts/{addr.lower()}/funding", timeout=15).json().get("items", [])
        recent = [i for i in items if time.time() - i.get("createdAtMs", 0) / 1000 < 86400]
        foreign = [i for i in recent if i.get("destination", "").lower() != addr.lower()]
        if foreign:
            out.append(_res("security:withdrawals", CRIT, f"withdrawal to a foreign destination in the last 24h: {foreign[0].get('destination')} amount {foreign[0].get('amount')}"))
        else:
            out.append(_res("security:withdrawals", OK, f"{len(recent)} withdrawal(s) in 24h, all to our own address"))
    except Exception as exc:  # noqa: BLE001
        out.append(_res("security:withdrawals", WARN, f"could not read funding history: {exc}"))
    return out


def check_risk_and_economics(ctx) -> list[dict]:
    st = ctx.get("status")
    out = []
    if not st:
        return [_res("risk", WARN, "no /status: risk/economics not checked")]
    out.append(_res("risk:stop-loss", OK, str(st.get("risk"))))
    ec = st.get("economics") or {}
    pb = ec.get("payback_days")
    out.append(_res("economics", WARN if pb is not None and pb > 7 else OK, json.dumps(ec)))
    return out


# ── 3. tracker, logs, state, clock ───────────────────────────────────────

def check_tracker(_ctx) -> list[dict]:
    out = []
    f = ROOT / "tracker_state.json"
    age = time.time() - f.stat().st_mtime if f.exists() else None
    out.append(_res("tracker:loop", OK if age is not None and age < 90 else WARN if age is not None and age < 600 else CRIT,
                    f"state written {age if age is None else round(age)}s ago"))
    try:
        cfg = json.loads((ROOT / "wallets" / "tg_bot.json").read_text())
        r = httpx.get(f"https://api.telegram.org/bot{cfg['token']}/getChat", params={"chat_id": cfg["owner_chat_id"]}, timeout=15).json()
        pm = r.get("result", {}).get("pinned_message")
        if not r.get("ok"):
            out.append(_res("telegram", CRIT, f"Bot API error: {r.get('description')}"))
        elif not pm:
            out.append(_res("telegram:dashboard", WARN, "no pinned dashboard message"))
        else:
            edited = pm.get("edit_date") or pm.get("date")
            lag = time.time() - edited
            out.append(_res("telegram:dashboard", OK if lag < 300 else WARN, f"pinned dashboard updated {lag:.0f}s ago"))
    except Exception as exc:  # noqa: BLE001
        out.append(_res("telegram", WARN, f"Telegram check failed: {type(exc).__name__}"))
    return out


def check_logs_state_clock(_ctx) -> list[dict]:
    out = []
    log = ROOT / "bot.log"
    if log.exists():
        cutoff = time.time() - 3600
        errs = rate = 0
        last_err = ""
        for line in log.read_text(errors="ignore").splitlines()[-4000:]:
            try:
                d = json.loads(line)
            except ValueError:
                continue
            ts = d.get("ts") or d.get("timestamp")
            try:
                t = time.mktime(time.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S")) if isinstance(ts, str) else float(ts)
            except Exception:  # noqa: BLE001
                t = 0
            if t and t < cutoff - 7200:
                continue
            if str(d.get("level", "")).upper() == "ERROR":
                errs += 1
                last_err = str(d.get("data") or d.get("message") or d)[:160]
            if "429" in line:
                rate += 1
        out.append(_res("logs:errors", OK if errs == 0 else WARN if errs < 6 else CRIT, f"{errs} ERROR lines in the recent log" + (f"; last: {last_err}" if errs else "")))
        out.append(_res("logs:rate-limit", OK if rate < 3 else WARN, f"{rate} HTTP 429 mentions in the recent log"))
    for name in ("bot_state.json", "economics.json", "risk_state.json", "tracker_state.json"):
        p = ROOT / name
        if p.exists():
            try:
                json.loads(p.read_text())
            except ValueError:
                out.append(_res(f"state:{name}", CRIT, "corrupt JSON"))
    try:
        d = httpx.head("https://exchange.papertrade.xyz/", timeout=10).headers.get("date")
        from email.utils import parsedate_to_datetime
        skew = abs(parsedate_to_datetime(d).timestamp() - time.time()) if d else None
        out.append(_res("clock:skew", OK if skew is not None and skew < 20 else WARN, f"local clock differs from the exchange by {skew if skew is None else round(skew)}s (intent deadlines depend on it)"))
    except Exception as exc:  # noqa: BLE001
        out.append(_res("clock:skew", WARN, f"could not compare clocks: {type(exc).__name__}"))
    free_gb = shutil.disk_usage(ROOT).free / 1e9
    out.append(_res("disk", OK if free_gb > 2 else WARN if free_gb > 0.5 else CRIT, f"{free_gb:.1f} GB free"))
    return out


# ── 4. deep (daily) ──────────────────────────────────────────────────────

def check_deep(_ctx) -> list[dict]:
    out = []
    try:
        r = subprocess.run([str(ROOT / ".venv/bin/pytest"), "tests", "-q", "-p", "no:cacheprovider", "-x"], cwd=ROOT,
                           capture_output=True, text=True, timeout=240)
        tail = [l for l in r.stdout.splitlines() if "passed" in l or "failed" in l or "error" in l.lower()]
        out.append(_res("deep:tests", OK if r.returncode == 0 else CRIT, tail[-1] if tail else f"exit {r.returncode}"))
    except Exception as exc:  # noqa: BLE001
        out.append(_res("deep:tests", WARN, f"could not run the test suite: {exc}"))
    dirty = _sh(["git", "status", "--porcelain", "bot", "papertrade", "tests", "deploy", "run_bot.py"]).splitlines()
    out.append(_res("deep:git", OK if not dirty else WARN, "bot code committed" if not dirty else f"{len(dirty)} uncommitted change(s) in bot code: {dirty[:3]}"))
    for unit in ("papertrade-farmer", "papertrade-tracker"):
        a = ROOT / "deploy" / f"{unit}.service"
        b = Path.home() / ".config/systemd/user" / f"{unit}.service"
        same = a.exists() and b.exists() and a.read_text() == b.read_text()
        out.append(_res(f"deep:unit:{unit}", OK if same else WARN, "deployed unit matches deploy/" if same else "deployed unit file differs from deploy/"))
    lingering = _sh(["loginctl", "show-user", os.environ.get("USER", ""), "-p", "Linger"])
    out.append(_res("deep:linger", OK if "yes" in lingering else CRIT, f"{lingering or 'unknown'} (services survive logout only with Linger=yes)"))
    try:
        subprocess.run([str(ROOT / ".venv/bin/python"), "run_bot.py", "panic"], cwd=ROOT, capture_output=True, text=True, timeout=90, check=True)
        out.append(_res("deep:panic-dry-run", OK, "panic dry-run completes against the live exchange"))
    except Exception as exc:  # noqa: BLE001
        out.append(_res("deep:panic-dry-run", CRIT, f"PANIC DRY-RUN FAILED: {exc}"))
    return out

# ── 1b. the exchange's house (counterparty pool) ─────────────────────────
HOUSE_BASE = "https://exchange.papertrade.xyz"
HOUSE_WINDOW_H = 6          # hourly points compared against the current value
HOUSE_DROP_WARN = 0.30      # warn when the pool is down this share from its peak in the window


def evaluate_house(lp_hourly: list, lp_now, paused: bool, has_positions: bool) -> list[dict]:
    """Pure decision logic for check_house (unit-tested).

    The pool ("lp" column of /query/house/history, "head" of /state/house/current) is the counterparty of every
    trade. Large one-sided winners (e.g. oracle-pushing accounts) shrink it, and a shrinking pool can lead the
    exchange to pause trading, widen fees or change the price source. This is an early-warning signal only: the
    meaning of the column is inferred, not documented, so it never goes above WARN.
    """
    out = []
    if paused:
        out.append(_res("house:trading-paused", CRIT if has_positions else WARN,
                        "exchange reports tradingPaused=true" + (" while we hold open positions: closing may be blocked" if has_positions else "")))
    else:
        out.append(_res("house:trading-paused", OK, "trading is live on the exchange"))
    window = [float(v) for v in lp_hourly[-HOUSE_WINDOW_H:] if v]
    if lp_now:
        window.append(float(lp_now))
    if len(window) < 3:
        out.append(_res("house:pool", OK, "not enough pool history to judge"))
        return out
    peak, cur = max(window), window[-1]
    drop = 1 - cur / peak if peak else 0.0
    msg = f"pool ${cur / 1e6:,.2f}M, {drop * 100:.0f}% below its {len(window) - 1}h peak ${peak / 1e6:,.2f}M"
    if drop >= HOUSE_DROP_WARN:
        out.append(_res("house:pool", WARN, msg + " (early-warning only: if it keeps falling, consider reducing funds on the exchange)", drop=round(drop, 3)))
    else:
        out.append(_res("house:pool", OK, msg, drop=round(drop, 3)))
    return out


def check_house(ctx) -> list[dict]:
    st = httpx.get(f"{HOUSE_BASE}/state/trading", timeout=15).json()
    hist = httpx.get(f"{HOUSE_BASE}/query/house/history", params={"interval": "1h"}, timeout=15).json()
    cur = httpx.get(f"{HOUSE_BASE}/state/house/current", timeout=15).json()
    lp_now = (cur.get("head") or [None, None])[1]
    snap = ctx.get("snap")
    has_pos = bool(snap is not None and getattr(snap, "open_positions", None))
    return evaluate_house(hist.get("columns", {}).get("lp", []), lp_now, bool(st.get("tradingPaused")), has_pos)


CHECKS: list[Callable] = [check_services, check_farmer_api, check_panic_readiness, check_account,
                          check_house, check_risk_and_economics, check_tracker, check_logs_state_clock]


def run_all(deep: bool = False) -> list[dict]:
    ctx: dict = {}
    results: list[dict] = []
    for chk in CHECKS + ([check_deep] if deep else []):
        try:
            results.extend(chk(ctx))
        except Exception as exc:  # noqa: BLE001
            results.append(_res(chk.__name__, WARN, f"could not check: {type(exc).__name__}: {exc}"))
    return results


def verdict(results: list[dict]) -> int:
    sev = {r["sev"] for r in results}
    return 2 if CRIT in sev else 1 if WARN in sev else 0


def format_text(results: list[dict]) -> str:
    icon = {OK: "ok  ", WARN: "WARN", CRIT: "CRIT"}
    lines = [f"[{icon[r['sev']]}] {r['name']:<26} {r['msg']}" for r in sorted(results, key=lambda r: {CRIT: 0, WARN: 1, OK: 2}[r["sev"]])]
    n = {s: sum(1 for r in results if r["sev"] == s) for s in (OK, WARN, CRIT)}
    lines.append(f"\n{n[OK]} ok, {n[WARN]} warn, {n[CRIT]} critical")
    return "\n".join(lines)
