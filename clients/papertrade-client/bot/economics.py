"""
Economics: what does a PAPER actually cost us, and what does it pay?

cost per PAPER   = sum(-net result of cycles) / sum(PAPER minted)          (from the exchange's own trade records)
yield per PAPER  = d(protocol lifetime staker fees)/dt / staked PAPER       (sampled from /query/protocol/summary)
payback (days)   = cost per PAPER / yield per PAPER per day

`decision()` refuses to open a new hedge when the money is not coming back (payback too long) or when the
rolling 24h spend exceeds a share of equity. Missing data never blocks (we cannot judge): it only blocks on facts.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path
from typing import Callable, Optional

import httpx

BASE = "https://exchange.papertrade.xyz"
E18 = Decimal(10) ** 18


def net_result(trade: Optional[dict]) -> Decimal:
    """Realised result of one closed position after the exchange fee (adjustedPnl excludes the fee)."""
    if not trade:
        return Decimal(0)
    return (Decimal(trade["adjustedPnlRaw"]) - Decimal(trade.get("userPaidFeeRaw", "0"))) / E18


def paper_minted(trade: Optional[dict]) -> Decimal:
    return Decimal(trade.get("paperMintedRaw", "0")) / E18 if trade else Decimal(0)


class Economics:
    def __init__(self, path: Path, clock: Callable[[], float] = time.time) -> None:
        self.path, self.clock = Path(path), clock
        self.cycles: list[dict] = []
        self.samples: list[list[float]] = []          # [ts, lifetime_fees_usdc, staked_paper]
        try:
            d = json.loads(self.path.read_text())
            self.cycles, self.samples = d.get("cycles", []), d.get("samples", [])
        except (FileNotFoundError, ValueError):
            pass

    def save(self) -> None:
        self.path.write_text(json.dumps({"cycles": self.cycles[-500:], "samples": self.samples[-2000:]}))

    # ── recording ──
    def record_cycle(self, net: Decimal, paper: Decimal, kind: str, notional: float = 0.0) -> None:
        self.cycles.append({"ts": self.clock(), "net": float(net), "paper": float(paper), "kind": kind, "notional": notional})
        self.save()

    def sample_yield(self, min_gap_s: int = 280) -> None:
        now = self.clock()
        if self.samples and now - self.samples[-1][0] < min_gap_s:
            return
        d = httpx.get(f"{BASE}/query/protocol/summary", timeout=15).json()
        life = float(Decimal(d["fees"]["lifetime"]) / E18)
        staked = float(Decimal(d["paper"]["staked"]) / E18)
        self.samples = [s for s in self.samples if now - s[0] < 72 * 3600] + [[now, life, staked]]
        self.save()

    # ── derived ──
    def yield_per_paper_day(self, window_h: float = 6.0, min_span_h: float = 0.75) -> Optional[float]:
        now = self.clock()
        win = [s for s in self.samples if now - s[0] <= window_h * 3600]
        if len(win) < 2 or (win[-1][0] - win[0][0]) < min_span_h * 3600:
            return None
        days = (win[-1][0] - win[0][0]) / 86400
        staked = sum(s[2] for s in win) / len(win)
        return (win[-1][1] - win[0][1]) / days / staked if staked > 0 and days > 0 else None

    def cost_per_paper(self, last_n: int = 12) -> Optional[float]:
        recent = self.cycles[-last_n:]
        paper = sum(c["paper"] for c in recent)
        return -sum(c["net"] for c in recent) / paper if paper > 0 else None

    def spent_24h(self) -> float:
        now = self.clock()
        return -sum(c["net"] for c in self.cycles if now - c["ts"] <= 86400)

    def payback_days(self) -> Optional[float]:
        c, y = self.cost_per_paper(), self.yield_per_paper_day()
        if c is None or y is None or y <= 0:
            return None
        return max(c, 0.0) / y

    def decision(self, equity: Decimal, max_payback_days: float, daily_budget_frac: float) -> tuple[bool, str]:
        eq = float(equity)
        spent = self.spent_24h()
        if eq > 0 and spent > daily_budget_frac * eq:
            return False, f"24h spend ${spent:,.0f} exceeds {daily_budget_frac * 100:.0f}% of equity ${eq:,.0f}"
        pb, c, y = self.payback_days(), self.cost_per_paper(), self.yield_per_paper_day()
        if pb is not None and c is not None and y is not None and pb > max_payback_days:
            return False, (f"PAPER does not pay back: ${c * 1000:.2f}/1k PAPER vs yield ${y * 1000:.3f}/1k/day "
                           f"= {pb:.1f} days (limit {max_payback_days:.0f})")
        return True, "ok" if pb is None else f"payback {pb:.1f} days"

    def summary(self) -> dict:
        c, y, pb = self.cost_per_paper(), self.yield_per_paper_day(), self.payback_days()
        return {"cycles_recorded": len(self.cycles), "cost_per_1k_paper": None if c is None else round(c * 1000, 3),
                "yield_per_1k_paper_day": None if y is None else round(y * 1000, 4),
                "payback_days": None if pb is None else round(pb, 2), "spent_24h": round(self.spent_24h(), 2)}
