from __future__ import annotations

from decimal import Decimal
import time
from typing import Any, Optional

from eth_account.signers.local import LocalAccount

from bot.config import BotConfig
from bot.logger import BotLogger
from bot.monitor import PositionMonitor
import bot.onchain as onchain
from bot.staker import PaperStaker
from bot.state import BotState, SlotState
from papertrade.client import PaperTradeClient
from papertrade.wallet import load_wallet


class BotHalt(RuntimeError):
    """Unsafe/unknown situation: stop opening new positions until a human resumes."""


class HedgeNotConfirmed(BotHalt):
    pass


class PaperFarmer:
    """Main farming loop orchestrating delta-neutral hedging, bust detection, and PAPER staking."""

    def __init__(
        self,
        config: BotConfig,
        logger: BotLogger,
        state: BotState,
    ) -> None:
        self.config = config
        self.logger = logger
        self.state = state
        self.client_a: Optional[PaperTradeClient] = None
        self.client_b: Optional[PaperTradeClient] = None
        self.monitor: Optional[PositionMonitor] = None
        self.staker: Optional[PaperStaker] = None
        self.wallet_a: Optional[LocalAccount] = None
        self.wallet_b: Optional[LocalAccount] = None
        self._last_stake_check_s: float = 0.0
        self.state_path = None
        self._hedge_intents: dict = {}
        self._closing_since: Optional[float] = None
        self._open_cooldown_until: float = 0.0
        self._consecutive_aborts: int = 0
        self._orphan_since: Optional[float] = None

    def setup(self, wallet_a: LocalAccount, wallet_b: LocalAccount) -> None:
        """Build clients, register session keys, initialize monitor and staker."""
        self.wallet_a = wallet_a
        self.wallet_b = wallet_b

        # Update state addresses
        self.state.slot_a.address = wallet_a.address
        self.state.slot_b.address = wallet_b.address

        # Build clients
        self.client_a = PaperTradeClient(wallet=wallet_a)
        self.client_b = PaperTradeClient(wallet=wallet_b)

        # Register session keys only if not already active; adopt live positions
        from bot.live import get_snapshot
        for label, w, c, info in (("A", wallet_a, self.client_a, self.state.slot_a),
                                  ("B", wallet_b, self.client_b, self.state.slot_b)):
            snap = None
            try:
                snap = get_snapshot(w.address)
            except Exception as exc:  # noqa: BLE001
                self.logger.warn(f"snapshot failed for {label}: {exc}")
            if snap is None or not snap.has_session_key(w.address):
                self.logger.info(f"Registering session key for Slot {label}", address=w.address)
                if c.write:
                    c.write.register_session_key(dry_run=self.config.dry_run)
                if not self.config.dry_run:
                    # registration is async (indexed ~30s later); opening before that => HTTP 403
                    deadline = time.time() + 180
                    while time.time() < deadline:
                        time.sleep(4)
                        try:
                            if get_snapshot(w.address).has_session_key(w.address):
                                break
                        except Exception:  # noqa: BLE001
                            pass
                    else:
                        raise RuntimeError(f"session key for slot {label} not active after 180s")
                    self.logger.info(f"Session key active for Slot {label}")
            info.state = SlotState.REGISTERED
            if snap and snap.open_positions:
                p0 = snap.open_positions[0]
                info.state = SlotState.OPEN
                info.side = "LONG" if p0.is_long else "SHORT"
                info.opened_at_ms = p0.opened_at_ms
                self.logger.info(f"Slot {label}: adopted {len(snap.open_positions)} open position(s)")

        # Initialize monitor and staker
        self.monitor = PositionMonitor()
        self.staker = PaperStaker(
            client_a=self.client_a,
            client_b=self.client_b,
            logger=self.logger,
            dry_run=self.config.dry_run,
        )

    def open_hedge(self, side_a: str) -> tuple[str, str]:
        """
        Open LONG on A and SHORT on B (or vice versa), both same instrument+leverage+margin.
        Return (intent_a, intent_b).
        Wait 5s after each open.
        Set slots to OPEN state.
        """
        if not self.client_a or not self.client_b:
            raise RuntimeError("Clients not initialized. Call setup() first.")

        side_a_upper = side_a.upper()
        is_long_a = side_a_upper == "LONG"
        is_long_b = not is_long_a
        side_b_upper = "LONG" if is_long_b else "SHORT"

        self.logger.info(
            f"Opening hedge: Slot A = {side_a_upper}, Slot B = {side_b_upper}",
            instrument=self.config.instrument,
            margin=self.config.margin_usdc,
            leverage=self.config.leverage,
        )

        # Open Slot A
        resp_a = self.client_a.write.open_position(
            instrument=self.config.instrument,
            is_long=is_long_a,
            margin_usdc=self.config.margin_usdc,
            leverage=self.config.leverage,
            dry_run=self.config.dry_run,
        )
        intent_a = resp_a.get("intentId", "") if isinstance(resp_a, dict) else str(resp_a)
        self.logger.trade_event(
            slot="A",
            side=side_a_upper.lower(),
            size_usdc=self.config.margin_usdc,
            price=0.0,
            intent_hash=intent_a,
        )
        now_ms = int(time.time() * 1000)
        self.state.slot_a.state = SlotState.OPEN
        self.state.slot_a.side = side_a_upper
        self.state.slot_a.opened_at_ms = now_ms

        # Open Slot B immediately (both legs should land in the same relayer batch; if this fails, flatten A immediately: never stay naked at high leverage)
        try:
            resp_b = self.client_b.write.open_position(
                instrument=self.config.instrument,
                is_long=is_long_b,
                margin_usdc=self.config.margin_usdc,
                leverage=self.config.leverage,
                dry_run=self.config.dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"Slot B open failed ({exc}); flattening slot A")
            self._flatten("a")
            raise
        intent_b = resp_b.get("intentId", "") if isinstance(resp_b, dict) else str(resp_b)
        self._hedge_intents = {"a": intent_a, "b": intent_b}
        self.logger.trade_event(
            slot="B",
            side=side_b_upper.lower(),
            size_usdc=self.config.margin_usdc,
            price=0.0,
            intent_hash=intent_b,
        )
        now_ms = int(time.time() * 1000)
        self.state.slot_b.state = SlotState.OPEN
        self.state.slot_b.side = side_b_upper
        self.state.slot_b.opened_at_ms = now_ms

        time.sleep(5)

        if not self.config.dry_run:
            self._verify_hedge()

        return intent_a, intent_b

    def _hedge_snaps(self):
        """Snapshots of the account holding leg A and leg B (two wallets here; one shared account in SingleFarmer)."""
        from bot.live import get_snapshot
        return get_snapshot(self.state.slot_a.address), get_snapshot(self.state.slot_b.address)

    @staticmethod
    def _pos_for(snap, intent_id: str):
        """The open position created by this intent (falls back to the only/first position)."""
        return next((p for p in snap.open_positions if p.intent_id == intent_id), snap.open_positions[0])

    def _leg_status(self, snap, intent_id: str) -> str:
        """filled | queued | gone  (gone = neither a position nor a pending op carries this intent)."""
        if any(p.intent_id == intent_id for p in snap.open_positions):
            return "filled"
        if any(o.intent_id == intent_id for o in snap.pending_ops):
            return "queued"
        return "gone"

    def _verify_hedge(self) -> None:
        """
        Intents execute asynchronously through the relayer queue. Per-leg state machine:
          both filled                        -> confirmed
          one filled, other queued           -> tolerate up to naked_max_s, then cancel the queued leg + flatten
          both queued                        -> tolerate up to max_queue_wait_s, then cancel both
          a leg 'gone' (rejected/expired)    -> flatten the other leg (needs 2 consecutive polls: index lag)
        An aborted hedge is NOT a fault: cool down (open_cooldown_s) and retry. Never leaves a leg behind.
        """
        from bot.live import get_snapshot
        ids = getattr(self, "_hedge_intents", {})
        ia, ib = ids.get("a", ""), ids.get("b", "")
        t0 = time.time()
        first_fill_at: Optional[float] = None
        gone_polls = {"a": 0, "b": 0}
        status = {"a": "queued", "b": "queued"}
        while True:
            sa, sb = self._hedge_snaps()
            status = {"a": self._leg_status(sa, ia), "b": self._leg_status(sb, ib)}
            for k in "ab":
                gone_polls[k] = gone_polls[k] + 1 if status[k] == "gone" else 0
            elapsed = time.time() - t0
            if status["a"] == status["b"] == "filled":
                pa, pb = self._pos_for(sa, ia), self._pos_for(sb, ib)
                self.state.slot_a.opened_at_ms, self.state.slot_b.opened_at_ms = pa.opened_at_ms, pb.opened_at_ms
                skew = abs(pa.entry_price_raw - pb.entry_price_raw) / max(1, pa.entry_price_raw) * 100
                self.logger.info("Hedge confirmed", waited_s=round(elapsed, 1), entry_skew_pct=round(skew, 4),
                                 a=f"#{pa.position_id} bust@{pa.bust_price_raw}", b=f"#{pb.position_id} bust@{pb.bust_price_raw}")
                if skew > 0.02:
                    self.logger.warn(f"Legs filled at different prices (skew {skew:.4f}%): hedge is imperfect")
                self._consecutive_aborts = 0
                self.persist()
                return
            if "filled" in status.values() and first_fill_at is None:
                first_fill_at = time.time()
            reason = None
            if any(v >= 2 for v in gone_polls.values()):
                reason = f"leg rejected/expired {status}"
            elif first_fill_at is not None and time.time() - first_fill_at > self.config.naked_max_s:
                reason = f"one leg filled, other still queued after {self.config.naked_max_s}s {status}"
            elif elapsed > self.config.max_queue_wait_s:
                reason = f"queue wait exceeded {self.config.max_queue_wait_s}s {status}"
            if reason:
                self._abort_hedge(reason, ids)
                return
            time.sleep(2)

    def _abort_hedge(self, reason: str, ids: dict) -> None:
        """Cancel whatever is still queued, flatten whatever filled, then cool down."""
        self.logger.warn(f"Aborting hedge: {reason}")
        for key, client in (("a", self.client_a), ("b", self.client_b)):
            iid = ids.get(key)
            if not iid:
                continue
            try:
                client.write.cancel_intent(iid)
            except Exception as exc:  # noqa: BLE001 - already executed / not cancellable is fine
                self.logger.info(f"cancel {key.upper()} {iid[:10]}: {exc}")
        time.sleep(3)   # an intent may have executed while we cancelled: flatten against LIVE state
        for key in "ab":
            self._flatten(key)
        self._open_cooldown_until = time.time() + self.config.open_cooldown_s
        self._consecutive_aborts += 1
        self.persist()
        if self._consecutive_aborts >= 3:
            raise BotHalt(f"{self._consecutive_aborts} hedge attempts aborted in a row -- check balances / exchange rejections")

    def persist(self) -> None:
        if self.state_path:
            self.state.save(self.state_path)

    def reconcile(self) -> str:
        """
        Make local state match the exchange (source of truth). Returns one of
        hedged | flat | bust_handled | orphan_pending | dry.
        A single surviving leg is either a liquidation we have not processed yet
        (-> handle_bust) or a naked high-leverage leg (-> flatten after 90s grace).
        """
        if self.config.dry_run:
            return "dry"
        from bot.live import get_snapshot
        sa, sb = self.state.slot_a, self.state.slot_b
        snap_a, snap_b = get_snapshot(sa.address), get_snapshot(sb.address)
        for info, snap in ((sa, snap_a), (sb, snap_b)):
            if snap.open_positions:
                p = snap.open_positions[0]
                if info.state != SlotState.OPEN or info.opened_at_ms != p.opened_at_ms:
                    info.state = SlotState.OPEN
                    info.side = "LONG" if p.is_long else "SHORT"
                    info.opened_at_ms = p.opened_at_ms
            elif info.state == SlotState.OPEN:
                info.state = SlotState.IDLE
        a_open, b_open = bool(snap_a.open_positions), bool(snap_b.open_positions)
        if a_open == b_open:
            self._orphan_since = None
            return "hedged" if a_open else "flat"

        surv, empty = (sa, sb) if a_open else (sb, sa)
        empty_key = "b" if a_open else "a"
        liq = self.monitor.detect_bust(empty.address, surv.opened_at_ms - 15_000)
        if liq:
            empty.state = SlotState.OPEN
            empty.side = "SHORT" if surv.side == "LONG" else "LONG"
            empty.opened_at_ms = surv.opened_at_ms
            self._orphan_since = None
            self.logger.info(f"Reconcile: liquidation found on slot {empty_key.upper()}, handling bust")
            self.handle_bust(empty_key)
            return "bust_handled"
        now = time.time()
        if self._orphan_since is None:
            self._orphan_since = now
            self.logger.warn(f"Single open leg on slot {surv.label} and no liquidation event yet (grace 90s)")
            return "orphan_pending"
        if now - self._orphan_since >= 90:
            self.logger.error(f"NAKED LEG on slot {surv.label} without liquidation: flattening")
            self._flatten("a" if a_open else "b")
            self._orphan_since = None
            self.persist()
            raise BotHalt(f"naked leg on slot {surv.label} flattened; inspect before resuming")
        return "orphan_pending"

    def flatten_all(self) -> dict:
        """Close every open position on both wallets (verified against live state)."""
        from bot.live import get_snapshot
        result = {}
        for key, info, client in (("a", self.state.slot_a, self.client_a), ("b", self.state.slot_b, self.client_b)):
            ids = [p.position_id for p in get_snapshot(info.address).open_positions]
            if ids and not self.config.dry_run:
                client.write.close_positions(ids, dry_run=False)
            result[key] = ids
        if not self.config.dry_run:
            for _ in range(30):
                time.sleep(2)
                if not any(get_snapshot(i.address).open_positions for i in (self.state.slot_a, self.state.slot_b)):
                    break
            else:
                raise BotHalt("flatten_all: positions still open after 60s")
        for info in (self.state.slot_a, self.state.slot_b):
            info.state = SlotState.IDLE
        self.persist()
        return result

    def _close_slot_positions(self, slot: str, wait_s: Optional[int] = None) -> bool:
        """
        Close all open positions of a slot. IDEMPOTENT and queue-aware:
        - positions already covered by a queued closePositions intent are NOT re-sent;
        - waits for the relayer (up to wait_s / max_queue_wait_s) while the close is queued;
        Returns True when the slot is verifiably flat.
        """
        from bot.live import get_snapshot
        info, client = (self.state.slot_a, self.client_a) if slot == "a" else (self.state.slot_b, self.client_b)
        if self.config.dry_run:
            ids = [p.position_id for p in get_snapshot(info.address).open_positions]
            if ids:
                client.write.close_positions(ids, dry_run=True)
            return True
        deadline = time.time() + (wait_s if wait_s is not None else self.config.max_queue_wait_s)
        while True:
            snap = get_snapshot(info.address)
            if not snap.open_positions:
                return True
            covered = {i for o in snap.pending("closePositions") for i in o.close_ids}
            todo = [p.position_id for p in snap.open_positions if p.position_id not in covered]
            if todo:
                client.write.close_positions(todo, dry_run=False)
                self.logger.info(f"Close intent sent for slot {slot.upper()}: {todo}")
            if time.time() > deadline:
                return False
            time.sleep(2)

    def _flatten(self, slot: str) -> None:
        """Close every open position of a slot (partial failure / abort). Never raises."""
        info = self.state.slot_a if slot == "a" else self.state.slot_b
        try:
            if self._close_slot_positions(slot):
                info.state = SlotState.IDLE
            else:
                self.logger.error(f"FLATTEN of slot {slot.upper()} still queued after wait -- will retry via reconcile")
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"FLATTEN FAILED for slot {slot.upper()}: {exc} -- CHECK MANUALLY")

    def _queue_ok(self) -> bool:
        """
        Gate for opening a NEW hedge: the relayer queue must drain fast enough that both legs land
        together. Uses estimated drain time = pending gas / block gas limit (saturationPct is NOT a
        depth measure). Stream unreachable => fail closed (skipping an open is always safe).
        """
        from bot.live import estimated_wait_s, get_pressure
        if time.time() < self._open_cooldown_until:
            return False
        p = get_pressure()
        now = time.time()
        if p is None:
            if now - getattr(self, "_last_q_warn", 0) > 60:
                self.logger.warn("relayer pressure stream unavailable: not opening (fail-closed)")
                self._last_q_warn = now
            return False
        est = estimated_wait_s(p)
        if est > self.config.max_queue_wait_est_s:
            if now - getattr(self, "_last_q_warn", 0) > 60:
                self.logger.warn(f"relayer queue too deep: ~{est:.0f}s to drain ({p.get('queuedIntents')} queued); waiting")
                self._last_q_warn = now
            return False
        return True

    def _funded(self) -> bool:
        """Both wallets must have >= margin available before opening a hedge."""
        if self.config.dry_run:
            return True
        from bot.live import get_snapshot
        need = Decimal(str(self.config.margin_usdc))
        ok = True
        for label, info in (("A", self.state.slot_a), ("B", self.state.slot_b)):
            av = get_snapshot(info.address).available
            if av < need:
                ok = False
                now = time.time()
                if now - getattr(self, "_last_fund_warn", 0) > 60:
                    self.logger.warn(f"Slot {label} available {av} < margin {need}: NEEDS FUNDING/REBALANCE (waiting)")
                    self._last_fund_warn = now
        return ok

    def detect_bust_cycle(self) -> Optional[str]:
        """
        Poll both slots, return 'a' | 'b' | None for which busted.
        """
        if not self.monitor:
            return None

        from bot.live import get_snapshot
        if self.config.dry_run:
            return None
        for label, info in (("a", self.state.slot_a), ("b", self.state.slot_b)):
            if info.state != SlotState.OPEN or not info.opened_at_ms:
                continue
            snap = get_snapshot(info.address)
            if snap.open_positions:
                continue  # still alive
            # position gone: confirm it was a liquidation (not a manual/other close)
            if self.monitor.detect_bust(info.address, info.opened_at_ms) is not None:
                return label
            info.state = SlotState.IDLE
            self.logger.warn(f"Slot {label.upper()} position disappeared without liquidation event")
        return None

    def handle_bust(self, busted_slot: str) -> None:
        """
        Close winner, claim PAPER on busted, stake all PAPER on both, log cycle summary.
        """
        busted_slot_lower = busted_slot.lower()
        if busted_slot_lower == "a":
            busted_info = self.state.slot_a
            winner_info = self.state.slot_b
            winner_client = self.client_b
            busted_client = self.client_a
            winner_label = "B"
            loser_label = "A"
        else:
            busted_info = self.state.slot_b
            winner_info = self.state.slot_a
            winner_client = self.client_a
            busted_client = self.client_b
            winner_label = "A"
            loser_label = "B"

        now_ms = int(time.time() * 1000)
        busted_info.state = SlotState.BUSTED
        first_attempt = getattr(self, "_closing_since", None) is None
        busted_info.last_bust_ms = now_ms

        if first_attempt:
            self.logger.bust_event(
                slot=loser_label,
                side=busted_info.side.lower(),
                bust_price=0.0,          # exact price is in the exchange trade record (see tracker)
                paper_earned=0.0,        # filled in the cycle summary once the winner is closed
                cycle=self.state.total_cycles + 1,
            )

        # 1. Close winner (idempotent; the close may sit in the relayer queue for a while)
        self.logger.info(f"Closing winner slot {winner_label}")
        try:
            closed = self._close_slot_positions("b" if busted_slot_lower == "a" else "a")
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"Failed closing position for slot {winner_label}: {exc}")
            closed = False
        if not closed:
            # Still queued (or errored). Do NOT halt and do NOT double count: reconcile() calls us again
            # next tick, _close_slot_positions will see the pending close and not re-send.
            winner_info.state = SlotState.OPEN
            busted_info.state = SlotState.OPEN
            self._closing_since = getattr(self, "_closing_since", None) or time.time()
            self.logger.warn(f"Winner {winner_label} close still pending in queue; will keep checking")
            if time.time() - self._closing_since > 600:
                raise BotHalt(f"winner slot {winner_label} not closed after 10 min -- investigate")
            return
        self._closing_since = None

        winner_info.state = SlotState.IDLE

        # 2. PAPER minted by the liquidation sits in the loser's exchange balance (claim() is for fee
        #    rewards and answers 409 when there is nothing to claim) -> read it from the live snapshot.
        paper_claimed_busted = Decimal("0")
        if not self.config.dry_run:
            try:
                from bot.live import get_snapshot as _gs2
                paper_claimed_busted = _gs2(busted_info.address).paper_balance
                busted_info.paper_earned_total += float(paper_claimed_busted)
            except Exception as exc:  # noqa: BLE001
                self.logger.warn(f"Could not read minted PAPER for slot {loser_label}: {exc}")

        # 3. Stake all PAPER on both
        staked_res = {}
        if self.staker:
            try:
                staked_res = self.staker.claim_and_stake_both()
                self.state.slot_a.paper_staked_total += float(staked_res.get("a_paper", Decimal("0")))
                self.state.slot_b.paper_staked_total += float(staked_res.get("b_paper", Decimal("0")))
            except Exception as exc:
                self.logger.warn(f"Failed staking paper on both slots: {exc}")

        # 4. Rebalance winner -> loser
        self.rebalance(winner_slot=winner_label.lower(), loser_slot=loser_label.lower())

        # Update cycles
        self.state.total_cycles += 1
        busted_info.cycles += 1
        winner_info.cycles += 1
        busted_info.state = SlotState.IDLE
        self.persist()

        # 5. Log cycle summary
        cycle_duration_s = 0.0
        if busted_info.opened_at_ms:
            cycle_duration_s = (now_ms - busted_info.opened_at_ms) / 1000.0

        paper_earned = float(paper_claimed_busted)
        staked_total = float(staked_res.get("total", Decimal("0")))

        self.logger.cycle_summary(
            cycle=self.state.total_cycles,
            active_slots=0,
            total_paper_earned=paper_earned,
            total_paper_staked=staked_total,
            pnl_usdc=0.0,                # real PnL comes from exchange trade records (tracker)
            duration_s=cycle_duration_s,
            winner=winner_label,
            busted=loser_label,
        )

        busted_info.state = SlotState.IDLE

    def rebalance(self, winner_slot: str, loser_slot: str) -> None:
        """
        Winner withdraws profit via write.withdraw(),
        then call onchain.transfer_usdc() winner_eoa -> loser_eoa,
        then onchain.deposit_usdc_to_papertrade() for loser.
        Amount = margin_usdc.
        """
        winner_slot_lower = winner_slot.lower()
        if winner_slot_lower == "a":
            winner_client = self.client_a
            winner_wallet = self.wallet_a
            loser_wallet = self.wallet_b
        else:
            winner_client = self.client_b
            winner_wallet = self.wallet_b
            loser_wallet = self.wallet_a

        amount_decimal = Decimal(str(self.config.margin_usdc))

        self.logger.info(
            f"Rebalancing {self.config.margin_usdc} USDC from winner ({winner_slot.upper()}) to loser ({loser_slot.upper()})"
        )

        if self.config.dry_run:
            self.logger.info("Dry-run mode: skipping actual onchain rebalance transactions")
            return
        if not self.config.auto_rebalance:
            self.logger.info("auto_rebalance=off: not moving funds; loop waits until both slots are funded")
            return

        # 1. Winner withdraws profit via write.withdraw() (or withdraw_to_core)
        if winner_client and winner_client.write:
            withdraw_fn = getattr(
                winner_client.write,
                "withdraw",
                getattr(winner_client.write, "withdraw_to_core", None),
            )
            if withdraw_fn:
                try:
                    withdraw_fn(float(amount_decimal), dry_run=False)
                except Exception as exc:
                    self.logger.error(f"Error withdrawing from winner slot {winner_slot.upper()}: {exc}")

        # 2. Transfer USDC winner_eoa -> loser_eoa
        if winner_wallet and loser_wallet:
            try:
                onchain.transfer_usdc(
                    sender=winner_wallet,
                    to=loser_wallet.address,
                    amount_usdc=amount_decimal,
                    usdc_addr=self.config.usdc_address,
                    rpc_url=self.config.hyperevm_rpc,
                )
            except Exception as exc:
                self.logger.error(f"Error transferring USDC from winner to loser: {exc}")

        # 3. Deposit USDC to PaperTrade for loser
        if loser_wallet:
            try:
                onchain.deposit_usdc_to_papertrade(
                    sender=loser_wallet,
                    amount_usdc=amount_decimal,
                    usdc_addr=self.config.usdc_address,
                    factory_addr=self.config.deposit_factory,
                    rpc_url=self.config.hyperevm_rpc,
                    collateral_addr=self.config.collateral_address,
                )
            except Exception as exc:
                self.logger.error(f"Error depositing USDC to PaperTrade for loser: {exc}")

    def run_once(self, allow_open: bool = True) -> None:
        """Single iteration of the farming loop - check state, act accordingly."""
        # 1. Periodic stake & reward check
        now = time.time()
        if now - self._last_stake_check_s >= self.config.stake_check_interval_s:
            if self.staker:
                try:
                    self.staker.claim_and_stake_both()
                except Exception as exc:
                    self.logger.warn(f"Periodic stake/claim check error: {exc}")
            self._last_stake_check_s = now

        # 2. Check current slot states
        state_a = self.state.slot_a.state
        state_b = self.state.slot_b.state

        # If both are IDLE or REGISTERED or INIT, open hedge
        if (
            state_a in (SlotState.IDLE, SlotState.REGISTERED, SlotState.DEPOSITED)
            and state_b in (SlotState.IDLE, SlotState.REGISTERED, SlotState.DEPOSITED)
        ):
            if not allow_open:
                return
            if not self._funded():
                return
            if not self.config.dry_run and not self._queue_ok():
                return
            self.logger.info("Both slots idle. Initiating new hedge cycle...")
            # Alternate side based on total cycles: even = A long / B short, odd = A short / B long
            side_a = "LONG" if (self.state.total_cycles % 2 == 0) else "SHORT"
            self.open_hedge(side_a=side_a)
            return

        # If both slots are OPEN, detect bust cycle
        if state_a == SlotState.OPEN and state_b == SlotState.OPEN:
            busted_slot = self.detect_bust_cycle()
            if busted_slot is not None:
                self.logger.info(f"Bust detected on slot {busted_slot.upper()}!")
                self.handle_bust(busted_slot)

    def run(self, max_cycles: int = 0) -> None:
        """
        Main loop calling run_once() with sleep(poll_interval_s) between iterations.
        Ctrl+C graceful stop.
        max_cycles=0 means infinite.
        """
        self.logger.info(
            f"Starting PaperFarmer loop (max_cycles={max_cycles}, poll_interval={self.config.poll_interval_s}s)"
        )
        cycles_completed_at_start = self.state.total_cycles
        try:
            while True:
                self.run_once()

                if max_cycles > 0:
                    cycles_done = self.state.total_cycles - cycles_completed_at_start
                    if cycles_done >= max_cycles:
                        self.logger.info(f"Reached target max_cycles ({max_cycles}). Exiting...")
                        break

                time.sleep(self.config.poll_interval_s)
        except KeyboardInterrupt:
            self.logger.info("PaperFarmer stopped by KeyboardInterrupt (Ctrl+C). Graceful exit.")
        finally:
            if self.monitor:
                self.monitor.close()
            if self.client_a:
                self.client_a.close()
            if self.client_b:
                self.client_b.close()
