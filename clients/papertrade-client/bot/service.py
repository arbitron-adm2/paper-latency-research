"""
Long-running farmer service with a small HTTP control API.

  GET  /health            no auth; liveness only
  GET  /status            full picture: mode, flags, slots, live balances/positions
  GET  /logs?n=100        tail of bot.log (JSON lines)
  POST /pause             stop opening NEW hedges (open positions are still monitored/closed)
  POST /resume            clear pause AND halt, allow new hedges
  POST /flatten           close all positions, then pause
  POST /stake             stake unstaked PAPER now (needs >= 1000 PAPER)
  POST /panic             flatten + withdraw EVERYTHING to the backup address (wallets/backup_address)
  POST /panic_dry         same, but only prints the plan into the log
  POST /stop              graceful shutdown (positions stay open on the exchange!)

Auth: `Authorization: Bearer <token>`; token lives in wallets/api_token (0600).
Binds 127.0.0.1 only. Private keys / seed phrases are never exposed via the API.
Safety model: fail closed. Unknown/unsafe situations => HALT (no new positions) but
monitoring and bust handling keep running, so an open winner is never left unattended.
"""
from __future__ import annotations

import hmac
import json
import secrets
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from bot.farmer import BotHalt, PaperFarmer
from bot import panic as panic_mod
from bot.risk import RiskGuard

MAX_CONSECUTIVE_ERRORS = 5


def load_or_create_token(path: Path) -> str:
    if path.exists():
        return path.read_text().strip()
    path.parent.mkdir(exist_ok=True)
    tok = secrets.token_urlsafe(32)
    path.write_text(tok)
    path.chmod(0o600)
    return tok


class FarmerService:
    def __init__(self, farmer: PaperFarmer, log_file: Path, token: str, host: str = "127.0.0.1", port: int = 8787):
        self.farmer = farmer
        self.log_file = Path(log_file)
        self.token = token
        self.host, self.port = host, port
        self.lock = threading.RLock()          # serialises every action that signs/sends intents
        self.stop_evt = threading.Event()
        self.paused = False
        self.halted: str | None = None
        self.last_error: str | None = None
        self.consecutive_errors = 0
        self.last_tick_ts: float | None = None
        self.last_reconcile: str | None = None
        self.started_ts = time.time()
        self.halted_since: float | None = None
        self.panic_wallets: dict = {}          # label -> wallet (set by run_bot); never exposed via the API
        self.risk: RiskGuard | None = None
        self.last_risk: str = "n/a"
        self.panicking = False
        self._status_cache: tuple[float, dict] | None = None

    # ── control loop ────────────────────────────────────────────────
    def tick(self) -> None:
        with self.lock:
            flag = panic_mod.flag_active()
            if flag:
                self.paused = True
            self.last_reconcile = self.farmer.reconcile()
            self.farmer.run_once(allow_open=not (self.paused or self.halted or flag))
            self.farmer.persist()
            self.last_tick_ts = time.time()
            self.consecutive_errors = 0
            if not flag:
                self._auto_panic_checks()

    # ── automatic PANIC triggers (live mode only) ───────────────────
    def _auto_panic_checks(self) -> None:
        cfg = self.farmer.config
        if cfg.dry_run or not cfg.auto_panic or not self.panic_wallets or self.panicking:
            return
        from bot.live import get_snapshot
        addr = self.farmer.state.slot_a.address
        snap = get_snapshot(addr)
        if self.halted:
            self.halted_since = self.halted_since or time.time()
            if snap.open_positions and time.time() - self.halted_since > cfg.halt_panic_s:
                self.run_panic(f"farmer halted with open positions for >{cfg.halt_panic_s // 60} min: {self.halted}")
                return
        else:
            self.halted_since = None
        if self.risk is not None:
            def withdrawn_since(ts: float):
                from decimal import Decimal
                import httpx
                try:
                    items = httpx.get(f"https://exchange.papertrade.xyz/query/accounts/{addr.lower()}/funding", timeout=15).json().get("items", [])
                    return sum((Decimal(i["amount"]) / Decimal(10**6) for i in items if i.get("createdAtMs", 0) / 1000 >= ts), Decimal(0))
                except Exception:  # noqa: BLE001 - if we cannot tell, do not pretend a loss is explained
                    return Decimal(0)
            hit, why = self.risk.evaluate(snap.balance + snap.locked, withdrawn_since)
            self.last_risk = why
            if hit:
                self.run_panic(f"STOP-LOSS: {why}")

    def run_panic(self, reason: str, dry_run: bool = False) -> dict:
        with self.lock:
            self.panicking = True
            try:
                self.paused = True
                self.farmer.logger.error(f"PANIC: {reason}")
                rep = panic_mod.Panic(self.panic_wallets, dry_run=dry_run, log=lambda m: self.farmer.logger.info(m)).run(reason)
                self._status_cache = None
                return rep
            finally:
                self.panicking = False

    def _halt(self, reason: str) -> None:
        self.halted = reason
        self.last_error = reason
        self.farmer.logger.error(f"HALTED: {reason}")

    def loop(self) -> None:
        while not self.stop_evt.is_set():
            try:
                self.tick()
            except BotHalt as exc:
                self._halt(str(exc))
            except Exception as exc:  # noqa: BLE001 - never die; monitoring must continue
                self.consecutive_errors += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.farmer.logger.error(f"tick error #{self.consecutive_errors}: {self.last_error}\n{traceback.format_exc()}")
                if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS and not self.halted:
                    self._halt(f"{self.consecutive_errors} consecutive tick errors; last: {self.last_error}")
            self.stop_evt.wait(self._interval())

    def _interval(self) -> float:
        """Seconds to sleep between ticks; ANY problem here must not kill the loop thread (it is the safety net)."""
        try:
            pi = getattr(self.farmer, "poll_interval", None)
            v = float(pi() if callable(pi) else self.farmer.config.poll_interval_s)
            return v if 0.5 <= v <= 600 else float(self.farmer.config.poll_interval_s)
        except Exception:  # noqa: BLE001
            return 30.0

    # ── API actions ─────────────────────────────────────────────────
    def status(self) -> dict:
        now = time.time()
        if self._status_cache and now - self._status_cache[0] < 8:
            return self._status_cache[1]
        from bot.live import get_snapshot
        wallets = {}
        slots = [("A", self.farmer.state.slot_a), ("B", self.farmer.state.slot_b)]
        if slots[1][1].address == slots[0][1].address:       # single-account mode: slot B mirrors A, show it once
            slots = slots[:1]
        for key, info in slots:
            try:
                s = get_snapshot(info.address, retries=1)
                wallets[key] = {
                    "address": info.address,
                    "balance": str(s.balance), "available": str(s.available), "locked": str(s.locked),
                    "paper_unstaked": str(s.paper_balance), "paper_staked": str(s.paper_staked),
                    "session_key_active": s.has_session_key(info.address),
                    "pending_ops": [{"action": o.action, "intent": o.intent_id[:12], "age_s": round(now - o.accepted_at_ms / 1000)}
                                    for o in s.pending_ops],
                    "positions": [
                        {"id": p.position_id, "side": "LONG" if p.is_long else "SHORT", "leverage": p.leverage,
                         "margin": str(p.margin), "bust_price_raw": p.bust_price_raw, "opened_at_ms": p.opened_at_ms}
                        for p in s.open_positions
                    ],
                }
            except Exception as exc:  # noqa: BLE001
                wallets[key] = {"address": info.address, "error": str(exc)}
        st = self.farmer.state
        cfg = self.farmer.config
        from bot.live import estimated_wait_s, get_pressure
        pr = get_pressure()
        out = {
            "relayer": None if pr is None else {
                "queued_intents": pr.get("queuedIntents"), "submitted": pr.get("submittedIntents"),
                "pending_gas": pr.get("totalPendingGas"), "block_gas_limit": pr.get("blockGasLimit"),
                "est_drain_s": round(estimated_wait_s(pr), 1), "saturation_pct": pr.get("saturationPct"),
            },
            "mode": "DRY-RUN" if cfg.dry_run else "LIVE",
            "paused": self.paused, "halted": self.halted, "last_error": self.last_error,
            "panic_flag": panic_mod.flag_active(), "risk": self.last_risk,
            "economics": (getattr(self.farmer, "economics", None).summary() if getattr(self.farmer, "economics", None) else None),
            "economics_block": getattr(self.farmer, "_econ_block", ""),
            "consecutive_errors": self.consecutive_errors, "last_reconcile": self.last_reconcile,
            "last_tick_age_s": None if self.last_tick_ts is None else round(now - self.last_tick_ts, 1),
            "uptime_s": round(now - self.started_ts),
            "config": {"instrument": cfg.instrument, "leverage": cfg.leverage, "margin_usdc": cfg.margin_usdc,
                       "poll_interval_s": cfg.poll_interval_s, "auto_rebalance": cfg.auto_rebalance},
            "state": {"total_cycles": st.total_cycles,
                      "slot_a": {"state": st.slot_a.state.value, "side": st.slot_a.side, "cycles": st.slot_a.cycles,
                                 "paper_earned_total": st.slot_a.paper_earned_total, "paper_staked_total": st.slot_a.paper_staked_total},
                      "slot_b": {"state": st.slot_b.state.value, "side": st.slot_b.side, "cycles": st.slot_b.cycles,
                                 "paper_earned_total": st.slot_b.paper_earned_total, "paper_staked_total": st.slot_b.paper_staked_total}},
            "wallets": wallets,
        }
        self._status_cache = (now, out)
        return out

    def logs(self, n: int) -> list:
        try:
            lines = self.log_file.read_text(errors="ignore").splitlines()[-n:]
        except FileNotFoundError:
            return []
        out = []
        for ln in lines:
            try:
                out.append(json.loads(ln))
            except ValueError:
                out.append({"raw": ln})
        return out

    def do(self, action: str) -> dict:
        if action == "pause":
            self.paused = True
        elif action == "resume":
            self.paused, self.halted, self.consecutive_errors = False, None, 0
        elif action == "flatten":
            with self.lock:
                closed = self.farmer.flatten_all()
            self.paused = True
            self._status_cache = None
            return {"ok": True, "closed": closed, "paused": True}
        elif action == "panic":
            if not self.panic_wallets:
                raise RuntimeError("panic wallets not configured")
            return self.run_panic("manual API request", dry_run=False)
        elif action == "panic_dry":
            return self.run_panic("dry run", dry_run=True)
        elif action == "stake":
            with self.lock:
                res = self.farmer.staker.claim_and_stake_both() if self.farmer.staker else {}
            self._status_cache = None
            return {"ok": True, "staked": {k: str(v) for k, v in res.items()}}
        elif action == "stop":
            self.stop_evt.set()
            threading.Thread(target=self._shutdown_later, daemon=True).start()
        else:
            raise KeyError(action)
        self.farmer.logger.info(f"API action: {action}")
        self._status_cache = None
        return {"ok": True, "paused": self.paused, "halted": self.halted}

    def _shutdown_later(self) -> None:
        time.sleep(0.5)
        self.httpd.shutdown()

    # ── HTTP ────────────────────────────────────────────────────────
    def serve(self) -> None:
        svc = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002 - keep stdout quiet
                pass

            def _send(self, code: int, obj) -> None:
                body = json.dumps(obj, default=str).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _auth(self) -> bool:
                got = self.headers.get("Authorization", "")
                ok = got.startswith("Bearer ") and hmac.compare_digest(got[7:], svc.token)
                if not ok:
                    self._send(401, {"error": "unauthorized"})
                return ok

            def do_GET(self):
                u = urlparse(self.path)
                if u.path == "/health":
                    age = None if svc.last_tick_ts is None else time.time() - svc.last_tick_ts
                    return self._send(200, {"ok": True, "halted": bool(svc.halted), "paused": svc.paused,
                                            "last_tick_age_s": age})
                if not self._auth():
                    return
                if u.path == "/status":
                    return self._send(200, svc.status())
                if u.path == "/logs":
                    n = min(int(parse_qs(u.query).get("n", ["100"])[0]), 1000)
                    return self._send(200, svc.logs(n))
                self._send(404, {"error": "not found"})

            def do_POST(self):
                if not self._auth():
                    return
                action = urlparse(self.path).path.strip("/")
                try:
                    self._send(200, svc.do(action))
                except KeyError:
                    self._send(404, {"error": f"unknown action {action}"})
                except Exception as exc:  # noqa: BLE001
                    self._send(500, {"error": f"{type(exc).__name__}: {exc}"})

        self.httpd = ThreadingHTTPServer((self.host, self.port), H)
        t = threading.Thread(target=self.loop, name="farmer-loop", daemon=True)
        t.start()
        try:
            self.httpd.serve_forever()
        finally:
            self.stop_evt.set()
            t.join(timeout=10)
            self.farmer.persist()
