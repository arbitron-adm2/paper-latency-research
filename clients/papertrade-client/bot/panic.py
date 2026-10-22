"""
PANIC: get every dollar out to the backup address.

    flatten (cancel queued opens, close all positions)
      -> withdraw exchange balance to the wallet's own HyperEVM address   (WithdrawToCore + signed SendAsset)
      -> transfer the wallet's HyperEVM USDC to the BACKUP address        (ERC-20 transfer)

Safety properties
  * The ONLY destination is the address in wallets/backup_address (0600, loaded + checksummed here). It is not
    an argument of any function reachable from the HTTP API, and it can never be one of our own wallets.
  * Idempotent / resumable: every step re-reads live state; running it twice is harmless.
  * Withdrawals are tried at `free - $1` first (the exchange charges a flat $1 fee on top of the amount; amounts
    that did not fit with the fee were rejected as `admission_rejected`), then in shrinking steps, down to the $10 minimum.
  * Writes `panic.flag` first: the farmer never opens a new hedge while the flag exists (clear with
    `run_bot.py panic --clear` after you have looked at what happened).
  * What it cannot do: PAPER (staked or not) is left where it is; balances below the $10 exchange minimum
    stay on the exchange; if the exchange API is down it fails loudly and reports what is still pending.
"""
from __future__ import annotations

import json
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import Callable, Optional

import httpx
from web3 import Web3

from bot import onchain
from bot.live import get_snapshot

ROOT = Path(__file__).resolve().parent.parent
BACKUP_FILE = ROOT / "wallets" / "backup_address"
FLAG_FILE = ROOT / "panic.flag"
REPORT_FILE = ROOT / "panic_report.json"
TG_CONFIG = ROOT / "wallets" / "tg_bot.json"

MIN_WITHDRAW = Decimal(10)
WITHDRAW_FEE = Decimal(1)       # flat, charged ON TOP of the amount (measured: balance fell by amount + 1.00 twice)
ONWARD_FEE = Decimal("0.1")      # taken from the amount on the HyperCore -> HyperEVM hop
STEP = Decimal("0.000001")
USDC = "0xb88339cb7199b77e23db6e890353e22632ba630f"
RPC = "https://rpc.hyperliquid.xyz/evm"


# ── backup address + flag ────────────────────────────────────────────────

def load_backup(path: Path = BACKUP_FILE) -> str:
    raw = path.read_text().strip()
    if not Web3.is_address(raw):
        raise ValueError(f"{path.name}: not a valid address")
    return Web3.to_checksum_address(raw)


def flag_active(path: Path = FLAG_FILE) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except ValueError:
        return {"reason": "unreadable flag file", "ts": 0}


def set_flag(reason: str, path: Path = FLAG_FILE) -> None:
    if not path.exists():
        path.write_text(json.dumps({"reason": reason, "ts": time.time()}))


def clear_flag(path: Path = FLAG_FILE) -> bool:
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def telegram(text: str, cfg_path: Path = TG_CONFIG) -> None:
    try:
        cfg = json.loads(cfg_path.read_text())
        httpx.post(f"https://api.telegram.org/bot{cfg['token']}/sendMessage",
                   json={"chat_id": cfg["owner_chat_id"], "text": text, "parse_mode": "HTML"}, timeout=15)
    except Exception:  # noqa: BLE001 - notification must never break the panic
        pass


def withdraw_candidates(free: Decimal, factor: Decimal = Decimal("0.8")) -> list[Decimal]:
    """Descending amounts to try: full balance, then x0.8 steps, always ending at the $10 minimum."""
    out: list[Decimal] = []
    cur = free.quantize(STEP, ROUND_DOWN)
    while cur >= MIN_WITHDRAW:
        out.append(cur)
        cur = (cur * factor).quantize(STEP, ROUND_DOWN)
    if not out or out[-1] != MIN_WITHDRAW:
        if free >= MIN_WITHDRAW:
            out.append(MIN_WITHDRAW)
    return out


# ── the procedure ────────────────────────────────────────────────────────

class Panic:
    def __init__(self, wallets: dict, backup: Optional[str] = None, dry_run: bool = False,
                 log: Callable[[str], None] = print, notify: Optional[Callable[[str], None]] = telegram,
                 sleep: Callable[[float], None] = time.sleep, make_client: Optional[Callable] = None,
                 test_amount: Optional[Decimal] = None) -> None:
        self.wallets = wallets                      # {"A": LocalAccount-like with .address, ...}
        self.backup = Web3.to_checksum_address(backup) if backup else load_backup()
        for label, w in wallets.items():
            if w.address.lower() == self.backup.lower():
                raise ValueError(f"backup address equals wallet {label}: refusing")
        self.dry_run, self.log, self.notify, self.sleep = dry_run, log, notify, sleep
        self.test_amount = test_amount            # path test: move only this much, no flag, no flatten, no alarm
        if make_client is None:
            from papertrade.client import PaperTradeClient
            make_client = lambda w: PaperTradeClient(wallet=w)      # noqa: E731
        self.make_client = make_client
        # injectable I/O (tests replace these)
        self.snapshot = get_snapshot
        self.usdc_balance = lambda addr: onchain.get_usdc_balance(addr, USDC, RPC)
        self.transfer = lambda w, to, amt: onchain.transfer_usdc(sender=w, to=to, amount_usdc=amt, usdc_addr=USDC, rpc_url=RPC)
        self.report: dict = {"backup": self.backup, "dry_run": dry_run, "wallets": {}, "ok": False}

    # 1 ─ flatten
    def flatten(self, label: str, w, timeout_s: int = 180) -> bool:
        client = self.make_client(w)
        deadline = time.time() + timeout_s
        clean_polls = 0
        while True:
            s = self.snapshot(w.address)
            for o in s.pending("openPosition"):
                if not self.dry_run:
                    try:
                        client.write.cancel_intent(o.intent_id)
                    except Exception as exc:  # noqa: BLE001 - may have executed already; loop re-checks
                        self.log(f"[{label}] cancel {o.intent_id[:10]}: {exc}")
            covered = {i for o in s.pending("closePositions") for i in o.close_ids}
            todo = [p.position_id for p in s.open_positions if p.position_id not in covered]
            if todo:
                self.log(f"[{label}] closing positions {todo}")
                if not self.dry_run:
                    client.write.close_positions(todo)
            if not s.open_positions and not s.pending("openPosition"):
                clean_polls += 1
                if clean_polls >= 2 or self.dry_run:     # two clean reads: nothing left that a queued open could create
                    return True
            else:
                clean_polls = 0
            if self.dry_run:
                return not s.open_positions
            if time.time() > deadline:
                return False
            self.sleep(2.5)

    # 2 ─ exchange -> wallet's HyperEVM address
    def _wait_landed(self, w, before_evm: Decimal, sent: Decimal, timeout_s: int = 420) -> bool:
        target = before_evm + sent - Decimal("0.11")          # 0.1 USDC onward fee + rounding
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.usdc_balance(w.address) >= target:
                return True
            self.sleep(6)
        return False

    def withdraw_all(self, label: str, w) -> list[str]:
        client = self.make_client(w)
        done: list[str] = []
        for _ in range(12):
            s = self.snapshot(w.address)
            if s.pending("withdrawToCore"):
                self.log(f"[{label}] earlier withdrawal still queued; waiting")
                if self.dry_run:
                    break
                self.sleep(6)
                continue
            free = s.available.quantize(STEP, ROUND_DOWN)
            spendable = free - WITHDRAW_FEE                    # the $1 fee must fit in the balance too
            if spendable < MIN_WITHDRAW:
                self.log(f"[{label}] {free} left on the exchange (< ${MIN_WITHDRAW} minimum + ${WITHDRAW_FEE} fee): not withdrawable")
                break
            cands = [self.test_amount] if self.test_amount else withdraw_candidates(spendable)
            if self.test_amount and spendable < self.test_amount:
                self.log(f"[{label}] test amount {self.test_amount} + fee exceeds free balance {free}")
                break
            if self.dry_run:
                self.log(f"[{label}] DRY: would withdraw {cands[0]} (fallbacks {[str(c) for c in cands[1:4]]}...)")
                break
            before = self.usdc_balance(w.address)
            sent = None
            for amt in cands:
                try:
                    client.write.withdraw_to_evm(amt)
                    sent = amt
                    break
                except Exception as exc:  # noqa: BLE001 - HttpError 409 admission_rejected etc.
                    code = getattr(getattr(exc, "body", None), "get", lambda *_: None)("code")
                    self.log(f"[{label}] withdraw {amt} rejected ({code or exc})")
            if sent is None:
                self.log(f"[{label}] every withdrawal amount was rejected")
                break
            self.log(f"[{label}] withdrawal of {sent} accepted; waiting for HyperEVM")
            if not self._wait_landed(w, before, sent):
                self.log(f"[{label}] withdrawal not visible on HyperEVM yet (still in transit); stopping round")
                done.append(f"{sent} (in transit)")
                break
            done.append(str(sent))
            self.sweep(label, w)                               # move it on before the next round
            if self.test_amount:
                break
        return done

    # 3 ─ wallet's HyperEVM address -> backup
    def sweep(self, label: str, w) -> Optional[str]:
        bal = self.usdc_balance(w.address)
        if bal < Decimal("0.01"):
            return None
        self.log(f"[{label}] sweeping {bal} USDC -> {self.backup}")
        if self.dry_run:
            return None
        tx = self.transfer(w, self.backup, bal)
        self.report["wallets"][label].setdefault("transfers", []).append({"amount": str(bal), "tx": tx})
        return tx

    def run(self, reason: str) -> dict:
        self.report["reason"] = reason
        self.report["started"] = time.strftime("%F %T")
        if not self.dry_run and not self.test_amount:
            set_flag(reason)
            if self.notify:
                self.notify(f"🚨 <b>PANIC</b>: {reason}\nЗакрываю позиции и вывожу всё на резервный адрес <code>{self.backup}</code>")
        for label, w in self.wallets.items():
            rep = self.report["wallets"][label] = {"address": w.address}
            try:
                rep["flat"] = True if self.test_amount else self.flatten(label, w)
                if not rep["flat"]:
                    self.log(f"[{label}] positions still open after the wait; withdrawing what is free")
                rep["withdrawals"] = self.withdraw_all(label, w)
                self.sweep(label, w)                           # also catches USDC that was already sitting on the address
            except Exception as exc:  # noqa: BLE001 - one wallet failing must not stop the other
                rep["error"] = f"{type(exc).__name__}: {exc}"
                self.log(f"[{label}] ERROR {rep['error']}")
            try:
                s = self.snapshot(w.address)
                rep["left_on_exchange"] = str(s.balance + s.locked)
                rep["left_on_evm"] = str(self.usdc_balance(w.address))
                rep["open_positions"] = len(s.open_positions)
                rep["paper"] = {"unstaked": str(s.paper_balance), "staked": str(s.paper_staked)}
            except Exception as exc:  # noqa: BLE001
                rep["verify_error"] = str(exc)
        self.report["ok"] = all(
            not r.get("error") and r.get("open_positions", 1) == 0 and Decimal(r.get("left_on_evm", "1")) < Decimal("0.01")
            and Decimal(r.get("left_on_exchange", "999")) < MIN_WITHDRAW + WITHDRAW_FEE
            for r in self.report["wallets"].values())
        if not self.dry_run and not self.test_amount:
            REPORT_FILE.write_text(json.dumps(self.report, indent=1))
            if self.notify:
                lines = [f"{'✅' if self.report['ok'] else '⚠️'} <b>Panic завершён</b> ({reason})"]
                for label, r in self.report["wallets"].items():
                    sent = sum(Decimal(t["amount"]) for t in r.get("transfers", []))
                    lines.append(f"{label}: отправлено {sent} USDC · осталось на бирже {r.get('left_on_exchange', '?')} · "
                                 f"позиций {r.get('open_positions', '?')}" + (f" · ошибка {r['error']}" if r.get("error") else ""))
                lines.append("PAPER остаётся на кошельке. Фармер остановлен флагом panic.flag.")
                self.notify("\n".join(lines))
        return self.report
