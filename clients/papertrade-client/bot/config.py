from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Union


@dataclass
class BotConfig:
    wallet_a_path: Path
    wallet_b_path: Path
    instrument: str = "BTC"
    leverage: int = 100
    margin_usdc: float = 100.0
    poll_interval_s: int = 30
    stake_check_interval_s: int = 300
    dry_run: bool = False
    max_queue_wait_est_s: float = 20.0  # do not open a NEW hedge if est. queue drain time (pending gas / block gas) exceeds this
    max_queue_wait_s: int = 120        # give up waiting for a queued intent after this
    naked_max_s: int = 40              # max time one leg may be open while the other is still queued
    open_cooldown_s: int = 300         # pause new hedges after an aborted/failed one
    close_fraction: float = 0.6        # close BOTH legs once price has moved this share of the liquidation distance (0 = wait for liquidation)
    max_payback_days: float = 7.0      # economics guard: block new hedges when cost/PAPER takes longer than this to earn back
    daily_budget_frac: float = 0.30    # ... or when the last 24h spend exceeds this share of equity
    fast_poll_s: int = 8               # tick interval while a hedge is open (threshold watching)
    auto_panic: bool = True            # stop-loss / unattended-exposure triggers run the PANIC withdrawal
    max_drawdown: float = 0.40         # equity stop-loss vs 7-day peak (after adding back withdrawals)
    halt_panic_s: int = 900            # halted with open positions for this long => PANIC
    auto_deposit: bool = False         # single-account mode: deposit USDC that lands on the wallet's HyperEVM address
    auto_rebalance: bool = False  # UNTESTED: withdraw->Core->EVM->deposit path; off by default
    log_file: Path = Path("bot.log")
    hyperevm_rpc: str = "https://rpc.hyperliquid.xyz/evm"
    usdc_address: str = "0xb88339cb7199b77e23db6e890353e22632ba630f"
    collateral_address: str = "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24"
    deposit_factory: str = "0x1629b7d46bef6e13ff754174fc9f32dea309b38f"

    def __post_init__(self) -> None:
        if isinstance(self.wallet_a_path, str):
            self.wallet_a_path = Path(self.wallet_a_path)
        if isinstance(self.wallet_b_path, str):
            self.wallet_b_path = Path(self.wallet_b_path)
        if isinstance(self.log_file, str):
            self.log_file = Path(self.log_file)
        self.leverage = int(self.leverage)
        self.margin_usdc = float(self.margin_usdc)
        self.poll_interval_s = int(self.poll_interval_s)
        self.stake_check_interval_s = int(self.stake_check_interval_s)
        self.dry_run = bool(self.dry_run)

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> BotConfig:
        config_path = Path(path)
        with open(config_path, "r", encoding="utf-8") as f:
            data: dict[str, Any] = json.load(f)

        return cls(
            wallet_a_path=Path(data["wallet_a_path"]),
            wallet_b_path=Path(data["wallet_b_path"]),
            instrument=data.get("instrument", "BTC"),
            leverage=int(data.get("leverage", 100)),
            margin_usdc=float(data.get("margin_usdc", 100.0)),
            poll_interval_s=int(data.get("poll_interval_s", 30)),
            stake_check_interval_s=int(data.get("stake_check_interval_s", 300)),
            dry_run=bool(data.get("dry_run", False)),
            log_file=Path(data.get("log_file", "bot.log")),
            hyperevm_rpc=data.get("hyperevm_rpc", "https://rpc.hyperliquid.xyz/evm"),
            usdc_address=data.get("usdc_address", "0xb88339cb7199b77e23db6e890353e22632ba630f"),
            collateral_address=data.get("collateral_address", "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24"),
            deposit_factory=data.get("deposit_factory", "0x1629b7d46bef6e13ff754174fc9f32dea309b38f"),
        )

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["wallet_a_path"] = str(self.wallet_a_path)
        d["wallet_b_path"] = str(self.wallet_b_path)
        d["log_file"] = str(self.log_file)
        return d
