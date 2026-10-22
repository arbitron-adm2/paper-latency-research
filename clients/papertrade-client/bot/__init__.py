"""Papertrade delta-neutral farming bot."""

__version__ = "0.1.0"
from .config import BotConfig
from .logger import BotLogger
from .state import BotState, SlotInfo, SlotState
from .staker import PaperStaker
from .farmer import PaperFarmer

__all__ = [
    "BotConfig",
    "BotLogger",
    "BotState",
    "SlotInfo",
    "SlotState",
    "PaperStaker",
    "PaperFarmer",
]
