"""
Single-account delta-neutral farmer: BOTH legs (LONG + SHORT, same instrument) live in ONE wallet.

Why: the exchange allows opposite positions in one account (10 of 29 sampled production wallets hold them).
With one account there is nothing to rebalance: the liquidated leg's margin is lost, the surviving leg's
profit is credited to the SAME balance when it is closed, and the minted PAPER piles up in one wallet
(the exchange will only stake >= 1000 PAPER, which one $10 cycle in a split setup never reaches).

Cycle:  open LONG+SHORT -> one leg is liquidated (PAPER minted) -> close the survivor -> stake PAPER -> repeat.

Sizing is derived from the live free balance, not configured blindly:
    margin   = min(cap, floor(free / 2))            per leg
    leverage = ceil(10_000 / margin)                keeps notional at the exchange minimum ($10k)
so the cheapest cycle (cost ~ 0.047% * notional + 2% of winner profit) is always used and the bot shrinks
gracefully as fees eat the balance instead of stalling.

Reuses PaperFarmer's queue-aware verification / abort / idempotent close machinery; slot B mirrors slot A.
"""
from __future__ import annotations

import time
from pathlib import Path
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Optional

from bot import onchain
from bot.farmer import BotHalt, PaperFarmer
from bot.economics import Economics, net_result, paper_minted
from bot.live import PRICE_SCALE, SYMBOL, get_mid, get_snapshot, scalp_trigger
from bot.monitor import PositionMonitor
from bot.staker import PaperStaker
from bot.state import SlotState
from papertrade.client import PaperTradeClient

MIN_NOTIONAL = Decimal(10_000)
MIN_MARGIN = Decimal(10)
MIN_DEPOSIT = Decimal(10)
MIN_GAS_HYPE = Decimal("0.0005")


class SingleFarmer(PaperFarmer):
    def __init__(self, *a, **kw) -> None:
        super().__init__(*a, **kw)
        self.wallet_a = None
        self._last_dep_check = 0.0
        self._deposit_cooldown_until = 0.0
        self.economics: Optional[Economics] = None
        self._cycle_pos: list[int] = []
        self._cycle_kind = "liquidation"
        self._cycle_notional = 0.0
        self._econ_block = ""
        self._last_econ_log = 0.0

    # ── typed accessors (setup() guarantees these exist) ─────────────────
    @property
    def _w(self):
        assert self.client_a is not None and self.client_a.write is not None, "setup() not called"
        return self.client_a.write

    @property
    def _mon(self) -> PositionMonitor:
        assert self.monitor is not None, "setup() not called"
        return self.monitor

    # ── setup ────────────────────────────────────────────────────────────
    def setup(self, wallet_a, wallet_b=None) -> None:  # type: ignore[override]
        self.wallet_a, self.wallet_b = wallet_a, None
        addr = wallet_a.address
        self.state.slot_a.address = self.state.slot_b.address = addr      # slot B mirrors the account
        self.client_a = PaperTradeClient(wallet=wallet_a)
        self.client_b = self.client_a
        snap = None
        try:
            snap = get_snapshot(addr)
        except Exception as exc:  # noqa: BLE001
            self.logger.warn(f"snapshot failed: {exc}")
        if snap is None or not snap.has_session_key(addr):
            self.logger.info("Registering session key", address=addr)
            self._w.register_session_key(dry_run=self.config.dry_run)
            if not self.config.dry_run:
                deadline = time.time() + 180                              # async, ~30s; opening earlier => 403
                while time.time() < deadline:
                    time.sleep(4)
                    try:
                        if get_snapshot(addr).has_session_key(addr):
                            break
                    except Exception:  # noqa: BLE001
                        pass
                else:
                    raise RuntimeError("session key not active after 180s")
                self.logger.info("Session key active")
        self._mirror(SlotState.REGISTERED)
        if snap and snap.open_positions:
            self._mirror(SlotState.OPEN, min(p.opened_at_ms for p in snap.open_positions))
            self.logger.info(f"adopted {len(snap.open_positions)} open position(s)")
        self.economics = Economics(Path("economics.json"))
        self.monitor = PositionMonitor()
        self.staker = PaperStaker(client_a=self.client_a, client_b=None, logger=self.logger,
                                  dry_run=self.config.dry_run)

    def _mirror(self, st: SlotState, opened_at: Optional[int] = None) -> None:
        for info, side in ((self.state.slot_a, "LONG"), (self.state.slot_b, "SHORT")):
            info.state = st
            info.side = side
            if opened_at is not None:
                info.opened_at_ms = opened_at

    # ── sizing / funding ─────────────────────────────────────────────────
    def sizing(self, available: Decimal) -> Optional[tuple[Decimal, int]]:
        """(margin per leg, leverage) from the free balance, or None if two legs cannot be funded."""
        cap = Decimal(str(self.config.margin_usdc))
        m = min(cap, ((available / 2) * 100).to_integral_value(ROUND_FLOOR) / 100)    # 2 legs never exceed free
        if m < MIN_MARGIN:
            return None
        lev = max(1, int((MIN_NOTIONAL / m).to_integral_value(ROUND_CEILING)))
        if lev > self.config.leverage:
            return None
        return m, lev

    def _funded(self) -> bool:
        if self.config.dry_run:
            return True
        free = get_snapshot(self.state.slot_a.address).available
        if self.sizing(free) is not None:
            return True
        now = time.time()
        if now - getattr(self, "_last_fund_warn", 0) > 300:
            self.logger.warn(f"free balance ${free} cannot fund two legs of >= ${MIN_MARGIN} "
                             f"(max leverage {self.config.leverage}); deposit more USDC")
            self._last_fund_warn = now
        return False

    def _economics_ok(self) -> bool:
        if self.config.dry_run or not self.economics:
            return True
        try:
            self.economics.sample_yield()
        except Exception as exc:  # noqa: BLE001 - no data never blocks
            self.logger.warn(f"yield sample failed: {exc}")
        s = get_snapshot(self.state.slot_a.address)
        ok, why = self.economics.decision(s.balance + s.locked, self.config.max_payback_days, self.config.daily_budget_frac)
        self._econ_block = "" if ok else why
        if not ok and time.time() - self._last_econ_log > 900:
            self._last_econ_log = time.time()
            self.logger.warn(f"not opening a new hedge: {why}")
        return ok

    def _maybe_deposit(self) -> None:
        """Opt-in: put any USDC (>= $10) sitting on the wallet's HyperEVM address into the exchange."""
        now = time.time()
        if now < self._deposit_cooldown_until or now - self._last_dep_check < 60:
            return
        self._last_dep_check = now
        c, addr = self.config, self.state.slot_a.address
        bal = onchain.get_usdc_balance(addr, c.usdc_address, c.hyperevm_rpc)
        if bal < MIN_DEPOSIT:
            return
        if onchain.get_hype_balance(addr, c.hyperevm_rpc) < MIN_GAS_HYPE:
            self.logger.warn(f"{bal} USDC waiting on HyperEVM but HYPE gas is too low to deposit")
            self._deposit_cooldown_until = now + 600
            return
        self._deposit_cooldown_until = now + 180            # credit takes ~1-2 min; never double-send
        self.logger.info(f"Auto-deposit {bal} USDC into the exchange")
        assert self.wallet_a is not None
        tx = onchain.deposit_usdc_to_papertrade(
            sender=self.wallet_a, amount_usdc=bal, usdc_addr=c.usdc_address, factory_addr=c.deposit_factory,
            rpc_url=c.hyperevm_rpc, collateral_addr=c.collateral_address)
        self.logger.info("deposit submitted", tx=tx, amount=float(bal))

    # ── opening ──────────────────────────────────────────────────────────
    def _hedge_snaps(self):
        s = get_snapshot(self.state.slot_a.address)
        return s, s

    def open_hedge(self, side_a: str = "LONG") -> tuple[str, str]:  # noqa: ARG002  (side_a unused: both sides open)
        addr = self.state.slot_a.address
        w = self._w
        if self.config.dry_run:
            sz = (Decimal(str(self.config.margin_usdc)), max(1, int(MIN_NOTIONAL / Decimal(str(self.config.margin_usdc)))))
        else:
            sz = self.sizing(get_snapshot(addr).available)
            if sz is None:
                return "", ""
        margin, lev = sz
        self.logger.info("Opening single-account hedge", instrument=self.config.instrument,
                         margin_per_leg=float(margin), leverage=lev, notional=float(margin * lev))
        r1 = w.open_position(instrument=self.config.instrument, is_long=True, margin_usdc=margin,
                             leverage=lev, dry_run=self.config.dry_run)
        id_long = r1.get("intentId", "") if isinstance(r1, dict) else str(r1)
        try:
            r2 = w.open_position(instrument=self.config.instrument, is_long=False, margin_usdc=margin,
                                 leverage=lev, dry_run=self.config.dry_run)
        except Exception as exc:  # noqa: BLE001
            # leg 1 may still be queued: cancel it first, then flatten whatever already filled
            self.logger.error(f"second leg submit failed ({exc}); aborting hedge")
            self._abort_hedge(f"second leg submit failed: {exc}", {"a": id_long})
            return id_long, ""
        id_short = r2.get("intentId", "") if isinstance(r2, dict) else str(r2)
        self._hedge_intents = {"a": id_long, "b": id_short}
        self._mirror(SlotState.OPEN, int(time.time() * 1000))
        self.persist()
        if not self.config.dry_run:
            self._verify_hedge()
            if self._consecutive_aborts:
                self._mirror(SlotState.IDLE)
        return id_long, id_short

    # ── closing / bust handling ──────────────────────────────────────────
    def _flatten(self, slot: str) -> None:
        if slot == "a":                                    # slot B is the same account; one close covers both
            super()._flatten("a")
            self.state.slot_b.state = self.state.slot_a.state

    def reconcile(self) -> str:
        if self.config.dry_run:
            return "dry"
        addr = self.state.slot_a.address
        ps = get_snapshot(addr).open_positions
        longs = [p for p in ps if p.is_long]
        shorts = [p for p in ps if not p.is_long]
        if longs and shorts:
            self._mirror(SlotState.OPEN, min(p.opened_at_ms for p in ps))
            self._orphan_since = None
            self._cycle_pos = [p.position_id for p in ps]
            self._cycle_notional = float(sum(p.margin * p.leverage for p in longs))
            return self._maybe_scalp_close(longs[0], shorts[0]) or "hedged"
        if not ps:
            if self._closing_since is not None:           # survivor closed between ticks: finish the bookkeeping
                self._complete_cycle()
                return "bust_handled"
            self._mirror(SlotState.IDLE)
            self._orphan_since = None
            return "flat"
        opened_ref = min(p.opened_at_ms for p in ps)
        self._mirror(SlotState.OPEN, opened_ref)
        loser_side = "short" if ps[0].is_long else "long"
        if self._closing_since is not None or self._mon.detect_bust(addr, opened_ref - 15_000):
            self.logger.info(f"Reconcile: {loser_side} leg was liquidated; closing the survivor")
            self.handle_bust("a", loser_side=loser_side)
            return "closing" if self._closing_since is not None else "bust_handled"
        now = time.time()
        if self._orphan_since is None:
            self._orphan_since = now
            self.logger.warn("Single open leg and no liquidation event yet (grace 90s)")
            return "orphan_pending"
        if now - self._orphan_since >= 90:
            self.logger.error("NAKED LEG without liquidation: flattening")
            self._flatten("a")
            self._orphan_since = None
            self.persist()
            raise BotHalt("naked leg flattened without a liquidation event; inspect before resuming")
        return "orphan_pending"

    def _maybe_scalp_close(self, lp, sp) -> Optional[str]:
        """Close BOTH legs in one intent once price travelled close_fraction of the liquidation distance.
        The losing leg's realised loss mints PAPER like a liquidation, without the liquidation penalty; the winner
        is paid at the same instant/price. Also resumes a close that is still queued."""
        frac = self.config.close_fraction
        if frac <= 0:
            return None
        if self._closing_since is None:
            sym = SYMBOL.get(lp.instrument_id, self.config.instrument)
            mid = get_mid(sym)
            if mid is None:
                return None
            k = PRICE_SCALE.get(lp.instrument_id, 10)
            entry = (lp.entry_price_raw + sp.entry_price_raw) / 2 / k
            side = scalp_trigger(entry, mid, lp.bust_price_raw / k, sp.bust_price_raw / k, frac)
            if side is None:
                return None
            self.logger.info(f"Threshold hit: price {mid:,.1f} moved {'up' if side == 'up' else 'down'} "
                             f"{abs(mid - entry) / entry * 100:.3f}% from entry {entry:,.1f}; closing both legs")
            self._closing_since = time.time()
            self._cycle_kind = "scalp"
            self.logger.bust_event(slot="A", side="long" if side == "down" else "short", bust_price=mid,
                                   paper_earned=0.0, cycle=self.state.total_cycles + 1)
        try:
            closed = self._close_slot_positions("a")
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"threshold close failed: {exc}")
            closed = False
        if closed:
            self._complete_cycle()
            return "scalp_closed"
        if time.time() - self._closing_since > 600:
            raise BotHalt("threshold close not executed after 10 min -- investigate")
        return "closing"

    def handle_bust(self, busted_slot: str = "a", loser_side: str = "?") -> None:  # type: ignore[override]
        first = self._closing_since is None
        if first:
            self.logger.bust_event(slot="A", side=loser_side, bust_price=0.0, paper_earned=0.0,
                                   cycle=self.state.total_cycles + 1)
        try:
            closed = self._close_slot_positions("a")
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"closing the surviving leg failed: {exc}")
            closed = False
        if not closed:
            self._closing_since = self._closing_since or time.time()
            self.logger.warn("surviving leg close still pending in the relayer queue; will keep checking")
            if time.time() - self._closing_since > 600:
                raise BotHalt("surviving leg not closed after 10 min -- investigate")
            return
        self._complete_cycle()

    def _complete_cycle(self) -> None:
        now_ms = int(time.time() * 1000)
        info = self.state.slot_a
        self._closing_since = None
        paper_total = Decimal(0)
        try:
            s = get_snapshot(info.address)
            paper_total = s.paper_balance + s.paper_staked
            info.paper_earned_total = float(paper_total)       # cumulative PAPER held by the account
        except Exception as exc:  # noqa: BLE001
            self.logger.warn(f"could not read PAPER balance: {exc}")
        staked: dict = {}
        if self.staker:
            try:
                staked = self.staker.claim_and_stake_both()
                info.paper_staked_total += float(staked.get("a_paper", Decimal(0)))
            except Exception as exc:  # noqa: BLE001
                self.logger.warn(f"staking failed: {exc}")
        self._record_economics()
        self.state.total_cycles += 1
        info.cycles += 1
        self._mirror(SlotState.IDLE)
        self.persist()
        self.logger.cycle_summary(
            cycle=self.state.total_cycles, active_slots=0, total_paper_earned=float(paper_total),
            total_paper_staked=float(staked.get("total", Decimal(0))), pnl_usdc=0.0,
            duration_s=(now_ms - info.opened_at_ms) / 1000.0 if info.opened_at_ms else 0.0,
            winner="A", busted="A")

    def _record_economics(self) -> None:
        """Realised result + PAPER of the closed positions, straight from the exchange's trade records."""
        if not self.economics or not self._cycle_pos or self.config.dry_run:
            self._cycle_pos = []
            return
        import httpx
        net, paper, found = Decimal(0), Decimal(0), 0
        try:
            items = httpx.get(f"https://exchange.papertrade.xyz/query/accounts/{self.state.slot_a.address.lower()}/trades",
                              timeout=15).json().get("items", [])
            by_id = {int(i["positionId"]): i for i in items}
            for pid in self._cycle_pos:
                t = by_id.get(pid)
                if t:
                    found += 1
                    net += net_result(t)
                    paper += paper_minted(t)
        except Exception as exc:  # noqa: BLE001
            self.logger.warn(f"could not read trade records for economics: {exc}")
        if found:
            self.economics.record_cycle(net, paper, self._cycle_kind, self._cycle_notional)
            cpp = f"${-net / paper * 1000:.2f}/1k PAPER" if paper > 0 else "n/a"
            self.logger.info(f"Cycle result ({self._cycle_kind}): net ${net:+.2f}, +{paper:,.0f} PAPER, {cpp}")
        self._cycle_pos = []
        self._cycle_kind = "liquidation"

    def flatten_all(self) -> dict:
        addr = self.state.slot_a.address
        ids = [p.position_id for p in get_snapshot(addr).open_positions]
        if ids and not self.config.dry_run and not self._close_slot_positions("a"):
            raise BotHalt("flatten_all: positions still open after the wait")
        self._mirror(SlotState.IDLE)
        self._closing_since = None
        self.persist()
        return {"a": ids, "b": []}

    # ── loop ─────────────────────────────────────────────────────────────
    def poll_interval(self) -> float:
        return self.config.fast_poll_s if self.state.slot_a.state == SlotState.OPEN and not self.config.dry_run else self.config.poll_interval_s

    def run_once(self, allow_open: bool = True) -> None:
        now = time.time()
        if now - self._last_stake_check_s >= self.config.stake_check_interval_s:
            if self.staker:
                try:
                    self.staker.claim_and_stake_both()
                except Exception as exc:  # noqa: BLE001
                    self.logger.warn(f"periodic stake/claim error: {exc}")
            self._last_stake_check_s = now
        if self.config.auto_deposit and not self.config.dry_run:
            try:
                self._maybe_deposit()
            except Exception as exc:  # noqa: BLE001
                self._deposit_cooldown_until = time.time() + 300
                self.logger.error(f"auto-deposit failed: {type(exc).__name__}: {exc}")
        from bot import panic as _panic
        if not allow_open or self._closing_since is not None or _panic.flag_active():
            return
        if self.state.slot_a.state not in (SlotState.IDLE, SlotState.REGISTERED, SlotState.DEPOSITED):
            return
        if not self._economics_ok():
            return
        if not self._funded():
            return
        if not self.config.dry_run and not self._queue_ok():
            return
        self.logger.info("Account is flat. Initiating new hedge cycle...")
        self.open_hedge()
