from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from pathlib import Path
import time
from typing import Any, Optional, Union


class SlotState(str, Enum):
    INIT = "INIT"
    REGISTERED = "REGISTERED"
    DEPOSITED = "DEPOSITED"
    OPEN = "OPEN"
    BUSTED = "BUSTED"
    IDLE = "IDLE"

    def __str__(self) -> str:
        return self.value


@dataclass
class SlotInfo:
    label: str
    address: str
    side: str = ""
    state: SlotState = SlotState.INIT
    opened_at_ms: Optional[int] = None
    paper_earned_total: float = 0.0
    paper_staked_total: float = 0.0
    usdc_deposited: float = 0.0
    cycles: int = 0
    last_bust_ms: Optional[int] = None

    def __post_init__(self) -> None:
        if isinstance(self.state, str):
            self.state = SlotState(self.state)
        self.paper_earned_total = float(self.paper_earned_total)
        self.paper_staked_total = float(self.paper_staked_total)
        self.usdc_deposited = float(self.usdc_deposited)
        self.cycles = int(self.cycles)
        if self.opened_at_ms is not None:
            self.opened_at_ms = int(self.opened_at_ms)
        if self.last_bust_ms is not None:
            self.last_bust_ms = int(self.last_bust_ms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "address": self.address,
            "side": self.side,
            "state": self.state.value if isinstance(self.state, SlotState) else str(self.state),
            "opened_at_ms": self.opened_at_ms,
            "paper_earned_total": self.paper_earned_total,
            "paper_staked_total": self.paper_staked_total,
            "usdc_deposited": self.usdc_deposited,
            "cycles": self.cycles,
            "last_bust_ms": self.last_bust_ms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SlotInfo:
        return cls(
            label=str(data.get("label", "")),
            address=str(data.get("address", "")),
            side=str(data.get("side", "")),
            state=SlotState(data.get("state", SlotState.INIT.value)),
            opened_at_ms=data.get("opened_at_ms"),
            paper_earned_total=float(data.get("paper_earned_total", 0.0)),
            paper_staked_total=float(data.get("paper_staked_total", 0.0)),
            usdc_deposited=float(data.get("usdc_deposited", 0.0)),
            cycles=int(data.get("cycles", 0)),
            last_bust_ms=data.get("last_bust_ms"),
        )


@dataclass
class BotState:
    slot_a: SlotInfo
    slot_b: SlotInfo
    started_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    total_cycles: int = 0
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def __post_init__(self) -> None:
        if isinstance(self.slot_a, dict):
            self.slot_a = SlotInfo.from_dict(self.slot_a)
        if isinstance(self.slot_b, dict):
            self.slot_b = SlotInfo.from_dict(self.slot_b)
        self.started_at_ms = int(self.started_at_ms)
        self.total_cycles = int(self.total_cycles)

    def to_dict(self) -> dict[str, Any]:
        return {
            "slot_a": self.slot_a.to_dict(),
            "slot_b": self.slot_b.to_dict(),
            "started_at_ms": self.started_at_ms,
            "total_cycles": self.total_cycles,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BotState:
        return cls(
            slot_a=SlotInfo.from_dict(data["slot_a"]),
            slot_b=SlotInfo.from_dict(data["slot_b"]),
            started_at_ms=int(data.get("started_at_ms", int(time.time() * 1000))),
            total_cycles=int(data.get("total_cycles", 0)),
            created_at=data.get(
                "created_at", datetime.now(timezone.utc).isoformat()
            ),
        )

    def save(self, path: Union[str, Path]) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write via temporary file
        tmp_path = p.with_suffix(f"{p.suffix}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        tmp_path.replace(p)

    @classmethod
    def load(cls, path: Union[str, Path]) -> BotState:
        p = Path(path)
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)
