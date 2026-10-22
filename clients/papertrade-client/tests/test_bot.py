import json
from pathlib import Path

import pytest

from bot import __version__
from bot.config import BotConfig
from bot.logger import BotLogger
from bot.state import BotState, SlotInfo, SlotState


def test_init_version():
    assert __version__ == "0.1.0"


def test_config_defaults(tmp_path: Path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(
        json.dumps(
            {
                "wallet_a_path": "wallets/a.json",
                "wallet_b_path": "wallets/b.json",
            }
        )
    )

    cfg = BotConfig.from_file(cfg_file)
    assert cfg.wallet_a_path == Path("wallets/a.json")
    assert cfg.wallet_b_path == Path("wallets/b.json")
    assert cfg.instrument == "BTC"
    assert cfg.leverage == 100
    assert cfg.margin_usdc == 100.0
    assert cfg.poll_interval_s == 30
    assert cfg.stake_check_interval_s == 300
    assert cfg.dry_run is False
    assert cfg.log_file == Path("bot.log")
    assert cfg.hyperevm_rpc == "https://rpc.hyperliquid.xyz/evm"
    assert cfg.usdc_address == "0xb88339cb7199b77e23db6e890353e22632ba630f"
    assert cfg.deposit_factory == "0x1629b7d46bef6e13ff754174fc9f32dea309b38f"


def test_logger_methods(tmp_path: Path):
    log_file = tmp_path / "test.log"
    logger = BotLogger(log_file=log_file)

    logger.info("info message", test_key="info_val")
    logger.warn("warn message", test_key="warn_val")
    logger.error("error message", err_code=500)
    logger.trade_event("A", "long", 100.0, 65000.0, intent_hash="0x123")
    logger.bust_event("B", "short", 64000.0, 10.5, 1)
    logger.paper_event("stake", 10.5, slot="B", tx_hash="0x456")
    logger.cycle_summary(
        cycle=1,
        active_slots=2,
        total_paper_earned=10.5,
        total_paper_staked=10.5,
        pnl_usdc=0.0,
        duration_s=5.0,
    )

    lines = log_file.read_text().strip().split("\n")
    assert len(lines) == 7

    records = [json.loads(line) for line in lines]
    assert records[0]["level"] == "INFO"
    assert records[0]["event"] == "info"
    assert records[0]["data"]["test_key"] == "info_val"

    assert records[1]["level"] == "WARN"
    assert records[2]["level"] == "ERROR"
    assert records[3]["event"] == "trade_event"
    assert records[3]["data"]["price"] == 65000.0
    assert records[4]["event"] == "bust_event"
    assert records[5]["event"] == "paper_event"
    assert records[6]["event"] == "cycle_summary"


def test_state_persistence(tmp_path: Path):
    state_file = tmp_path / "state.json"
    slot_a = SlotInfo(
        label="slot_a",
        address="0xa1",
        side="long",
        state=SlotState.OPEN,
        opened_at_ms=1000,
        paper_earned_total=5.0,
        paper_staked_total=2.0,
        usdc_deposited=100.0,
        cycles=3,
        last_bust_ms=500,
    )
    slot_b = SlotInfo(
        label="slot_b",
        address="0xb1",
        side="short",
        state=SlotState.BUSTED,
        opened_at_ms=1000,
        paper_earned_total=10.0,
        paper_staked_total=10.0,
        usdc_deposited=100.0,
        cycles=4,
        last_bust_ms=1500,
    )
    bot_state = BotState(
        slot_a=slot_a,
        slot_b=slot_b,
        started_at_ms=900,
        total_cycles=5,
        created_at="2026-10-11T00:00:00Z",
    )

    bot_state.save(state_file)
    loaded = BotState.load(state_file)

    assert loaded.started_at_ms == 900
    assert loaded.total_cycles == 5
    assert loaded.created_at == "2026-10-11T00:00:00Z"
    assert loaded.slot_a.label == "slot_a"
    assert loaded.slot_a.state == SlotState.OPEN
    assert loaded.slot_a.paper_earned_total == 5.0
    assert loaded.slot_b.label == "slot_b"
    assert loaded.slot_b.state == SlotState.BUSTED
    assert loaded.slot_b.last_bust_ms == 1500
