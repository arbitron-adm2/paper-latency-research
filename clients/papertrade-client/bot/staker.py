from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

from bot.logger import BotLogger
from papertrade.client import PaperTradeClient
from papertrade.constants import PAPER_SCALE


class PaperStaker:
    """PAPER auto-claim and staking management."""

    def __init__(
        self,
        client_a: PaperTradeClient,
        client_b: Optional[PaperTradeClient],
        logger: BotLogger,
        dry_run: bool = False,
    ) -> None:
        self.client_a = client_a
        self.client_b = client_b
        self.logger = logger
        self.dry_run = dry_run
        self.min_stake = Decimal(1000)          # exchange rule; refreshed from the 409 body if it changes
        self._below_min_logged: dict[str, float] = {}

    def claim_paper(self, slot_label: str, client: PaperTradeClient) -> Decimal:
        """
        Call client.write.claim(dry_run=self.dry_run), parse paperMintedRaw from result,
        return PAPER amount as Decimal.
        """
        if not client.write:
            self.logger.warn(f"Client for slot {slot_label} has no write capability")
            return Decimal("0")

        # Fallback to claim_paper_rewards if claim not present
        claim_fn = getattr(client.write, "claim", getattr(client.write, "claim_paper_rewards", None))
        if claim_fn is None:
            raise AttributeError("Client writer has neither claim nor claim_paper_rewards method")

        res: dict[str, Any] = claim_fn(dry_run=self.dry_run)

        # Parse paperMintedRaw from result dict or nested result
        # The result might contain paperMintedRaw directly or inside receipt/data
        raw_val = (
            res.get("paperMintedRaw")
            or res.get("paperMinted")
            or (res.get("data", {}) if isinstance(res.get("data"), dict) else {}).get("paperMintedRaw")
            or (res.get("result", {}) if isinstance(res.get("result"), dict) else {}).get("paperMintedRaw")
            or 0
        )

        try:
            amount = Decimal(str(raw_val)) / Decimal(PAPER_SCALE)
        except Exception:
            amount = Decimal("0")

        self.logger.paper_event(
            action="claim",
            amount=float(amount),
            slot=slot_label,
            tx_hash=res.get("intentId", "") or res.get("txHash", ""),
        )
        return amount

    def stake_paper(
        self, slot_label: str, client: PaperTradeClient, amount: Decimal
    ) -> dict[str, Any]:
        """
        Call client.write.stake(float(amount), dry_run=self.dry_run).
        """
        if amount <= 0:
            return {"status": "skipped", "reason": "zero_amount"}

        if not client.write:
            self.logger.warn(f"Client for slot {slot_label} has no write capability")
            return {"status": "error", "error": "no_write"}

        stake_fn = getattr(client.write, "stake", getattr(client.write, "stake_paper", None))
        if stake_fn is None:
            raise AttributeError("Client writer has neither stake nor stake_paper method")

        res: dict[str, Any] = stake_fn(amount, dry_run=self.dry_run)
        self.logger.paper_event(
            action="stake",
            amount=float(amount),
            slot=slot_label,
            tx_hash=res.get("intentId", "") or res.get("txHash", ""),
        )
        return res

    def claim_and_stake_both(self) -> dict[str, Decimal]:
        """
        State-driven: read the live snapshot of each wallet and stake the unstaked PAPER once it reaches the
        exchange minimum (1000 PAPER; below that the server answers 409 `below_minimum_paper_stake`).
        Also best-effort claim of fee rewards. Returns the PAPER amounts that were staked.
        """
        import time as _t
        from bot.live import get_snapshot
        from papertrade.exceptions import HttpError

        out = {"a_paper": Decimal("0"), "b_paper": Decimal("0")}
        for label, client in (("A", self.client_a), ("B", self.client_b)):
            if client is None or not client.write:
                continue
            try:
                addr = client.write._wallet.address
                snap = get_snapshot(addr)
                if snap.pending("paperStake", "paperClaim", "paperUnstake"):
                    self.logger.info(f"stake/claim already queued for {label}; skipping this round")
                    continue
                bal = snap.paper_balance
                if bal >= self.min_stake:
                    try:
                        self.stake_paper(label, client, bal)
                        out[f"{label.lower()}_paper"] = bal
                    except HttpError as exc:
                        body = exc.body if isinstance(exc.body, dict) else {}
                        if body.get("code") == "below_minimum_paper_stake":
                            self.min_stake = Decimal(body["minimumPaperStakeRaw"]) / Decimal(PAPER_SCALE)
                            self.logger.info(f"stake minimum is {self.min_stake} PAPER; {label} holds {bal:,.2f}")
                        else:
                            raise
                elif bal > 0 and _t.time() - self._below_min_logged.get(label, 0) > 3600:
                    self._below_min_logged[label] = _t.time()
                    self.logger.info(f"{label}: {bal:,.2f} PAPER waiting for the {self.min_stake} PAPER stake minimum")
                # fee rewards (USDC) -> collateral; the server answers 409 when nothing is claimable
                if snap.paper_staked > 0:
                    try:
                        self.claim_paper(label, client)
                    except Exception as exc:  # noqa: BLE001
                        self.logger.info(f"claim skipped for {label}: {exc}")
            except Exception as exc:  # noqa: BLE001
                self.logger.warn(f"stake cycle failed for {label}: {exc}")
        out["total"] = out["a_paper"] + out["b_paper"]
        return out

    def get_staked_balance(self, address: str) -> Decimal:
        """
        Query protocol summary or account endpoint for staked balance.
        """
        # Try client_a read first, else client_b
        client = self.client_a if self.client_a.read else self.client_b
        addr_lower = address.lower()

        try:
            # Check protocol summary
            summary = client.read.protocol_summary()
            paper_info = summary.get("paper", {})
            # If protocol summary has per-address or total staked
            if "staked" in paper_info:
                # Return total staked in protocol if address specific is not present
                raw_staked = paper_info.get("staked", 0)
                # But let's also check if there is an account endpoint
        except Exception as exc:
            self.logger.warn(f"Failed to query protocol summary: {exc}")
            raw_staked = 0

        # Also check account funding or trades if available
        try:
            funding = client.read.account_funding(addr_lower)
            if "staked" in funding:
                return Decimal(str(funding["staked"])) / Decimal(PAPER_SCALE)
        except Exception:
            pass

        # Return Decimal scaled
        try:
            return Decimal(str(raw_staked)) / Decimal(PAPER_SCALE)
        except Exception:
            return Decimal("0")
