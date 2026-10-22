from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Optional, Union

from rich.console import Console


class BotLogger:
    """Structured JSON logger with rich console output."""

    def __init__(
        self,
        log_file: Optional[Union[str, Path]] = None,
        console: Optional[Console] = None,
    ) -> None:
        self.log_file = Path(log_file) if log_file is not None else None
        self.console = console or Console()

        if self.log_file is not None:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _write_record(self, record: dict[str, Any]) -> None:
        if self.log_file is not None:
            line = json.dumps(record, default=str)
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def _log(
        self,
        level: str,
        event_type: str,
        data: Optional[dict[str, Any]] = None,
        console_msg: Optional[str] = None,
        color: str = "white",
    ) -> dict[str, Any]:
        payload = data or {}
        record = {
            "timestamp": self._now_iso(),
            "level": level.upper(),
            "event": event_type,
            "data": payload,
        }
        self._write_record(record)

        # Rich formatted print
        msg = console_msg
        if msg is None:
            if "message" in payload:
                msg = str(payload["message"])
            elif payload:
                msg = f"{event_type}: {payload}"
            else:
                msg = event_type

        ts_display = record["timestamp"].split("T")[-1][:12]
        self.console.print(
            f"[{color}][{ts_display}] [{level.upper():5s}] [{event_type}] {msg}[/{color}]"
        )
        return record

    def info(self, message: str, **kwargs: Any) -> dict[str, Any]:
        data = {"message": message, **kwargs}
        return self._log(
            level="INFO",
            event_type="info",
            data=data,
            console_msg=message,
            color="cyan",
        )

    def warn(self, message: str, **kwargs: Any) -> dict[str, Any]:
        data = {"message": message, **kwargs}
        return self._log(
            level="WARN",
            event_type="warn",
            data=data,
            console_msg=message,
            color="yellow",
        )

    def error(self, message: str, **kwargs: Any) -> dict[str, Any]:
        data = {"message": message, **kwargs}
        return self._log(
            level="ERROR",
            event_type="error",
            data=data,
            console_msg=message,
            color="red",
        )

    def trade_event(
        self,
        slot: str,
        side: str,
        size_usdc: float,
        price: float,
        intent_hash: Optional[str] = None,
        **extra: Any,
    ) -> dict[str, Any]:
        data = {
            "slot": slot,
            "side": side,
            "size_usdc": size_usdc,
            "price": price,
            "intent_hash": intent_hash,
            **extra,
        }
        msg = (
            f"Slot {slot} opened {side.upper()} ${size_usdc:.2f} @ ${price:,.2f}"
            + (f" (hash: {intent_hash[:10]}...)" if intent_hash else "")
        )
        return self._log(
            level="INFO",
            event_type="trade_event",
            data=data,
            console_msg=msg,
            color="bold green",
        )

    def bust_event(
        self,
        slot: str,
        side: str,
        bust_price: float,
        paper_earned: float,
        cycle: int,
        **extra: Any,
    ) -> dict[str, Any]:
        data = {
            "slot": slot,
            "side": side,
            "bust_price": bust_price,
            "paper_earned": paper_earned,
            "cycle": cycle,
            **extra,
        }
        msg = (
            f"BUST: Slot {slot} ({side.upper()}) busted @ ${bust_price:,.2f} "
            f"| Earned: {paper_earned:.4f} PAPER | Cycle: {cycle}"
        )
        return self._log(
            level="WARN",
            event_type="bust_event",
            data=data,
            console_msg=msg,
            color="bold magenta",
        )

    def paper_event(
        self,
        action: str,
        amount: float,
        slot: Optional[str] = None,
        tx_hash: Optional[str] = None,
        **extra: Any,
    ) -> dict[str, Any]:
        data = {
            "action": action,
            "amount": amount,
            "slot": slot,
            "tx_hash": tx_hash,
            **extra,
        }
        slot_str = f" Slot {slot} |" if slot else ""
        msg = (
            f"PAPER {action.upper()}:{slot_str} {amount:.4f} PAPER"
            + (f" (tx: {tx_hash[:10]}...)" if tx_hash else "")
        )
        return self._log(
            level="INFO",
            event_type="paper_event",
            data=data,
            console_msg=msg,
            color="bold blue",
        )

    def cycle_summary(
        self,
        cycle: int,
        active_slots: int,
        total_paper_earned: float,
        total_paper_staked: float,
        pnl_usdc: float,
        duration_s: float,
        **extra: Any,
    ) -> dict[str, Any]:
        data = {
            "cycle": cycle,
            "active_slots": active_slots,
            "total_paper_earned": total_paper_earned,
            "total_paper_staked": total_paper_staked,
            "pnl_usdc": pnl_usdc,
            "duration_s": duration_s,
            **extra,
        }
        msg = (
            f"Cycle {cycle} complete: {active_slots} slots active | "
            f"PAPER earned: {total_paper_earned:.4f} | staked: {total_paper_staked:.4f} | "
            f"P&L: ${pnl_usdc:+.2f} | took {duration_s:.1f}s"
        )
        return self._log(
            level="INFO",
            event_type="cycle_summary",
            data=data,
            console_msg=msg,
            color="bold cyan",
        )
