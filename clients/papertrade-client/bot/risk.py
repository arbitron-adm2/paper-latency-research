"""
RiskGuard: stop-loss on account equity.

Tracks equity samples over a window; the peak in the window is the reference. A drop of more than
`max_drawdown` below the peak triggers PANIC - after adding back withdrawals that the exchange's own
history shows were made since the peak (so a manual withdrawal does not look like a loss).
Deposits raise the peak, so they never trigger anything.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path
from typing import Callable, Optional


class RiskGuard:
    def __init__(self, path: Path, max_drawdown: float = 0.40, window_s: int = 7 * 86400,
                 min_watch_equity: Decimal = Decimal(30), clock: Callable[[], float] = time.time) -> None:
        self.path, self.max_dd, self.window_s, self.min_watch = Path(path), max_drawdown, window_s, min_watch_equity
        self.clock = clock
        self.samples: list[tuple[float, float]] = []
        self._last_write = 0.0
        try:
            self.samples = [(float(t), float(e)) for t, e in json.loads(self.path.read_text()).get("samples", [])]
        except (FileNotFoundError, ValueError):
            pass

    def peak(self) -> Optional[tuple[float, float]]:
        return max(self.samples, key=lambda s: s[1]) if self.samples else None

    def evaluate(self, equity: Decimal, withdrawn_since: Callable[[float], Decimal]) -> tuple[bool, str]:
        """Returns (trigger, explanation). `withdrawn_since(ts)` = USDC withdrawn from the exchange after ts."""
        now = self.clock()
        eq = float(equity)
        self.samples = [(t, e) for t, e in self.samples if now - t <= self.window_s]
        self.samples.append((now, eq))
        if now - self._last_write > 60:
            self.path.write_text(json.dumps({"samples": self.samples[-5000:]}))
            self._last_write = now
        pk = self.peak()
        if pk is None or pk[1] <= 0 or equity < self.min_watch:
            return False, "below watch threshold" if equity < self.min_watch else "no peak"
        adj = eq + float(withdrawn_since(pk[0]))
        dd = 1 - adj / pk[1]
        if dd >= self.max_dd:
            return True, f"equity ${eq:,.2f} is {dd * 100:.0f}% below the {self.window_s // 86400}d peak ${pk[1]:,.2f} (limit {self.max_dd * 100:.0f}%)"
        return False, f"drawdown {max(dd, 0) * 100:.1f}% (limit {self.max_dd * 100:.0f}%)"

    def rebase(self) -> None:
        """Forget history (call after a deliberate withdrawal / strategy change)."""
        self.samples = []
        self.path.write_text(json.dumps({"samples": []}))
