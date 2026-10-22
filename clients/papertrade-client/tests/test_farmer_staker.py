from decimal import Decimal
from unittest.mock import MagicMock, patch, create_autospec

import pytest
from eth_account import Account

from bot.config import BotConfig
from bot.farmer import PaperFarmer
from bot.logger import BotLogger
from bot.staker import PaperStaker
from bot.state import BotState, SlotInfo, SlotState
from papertrade.client import PaperTradeClient


@pytest.fixture
def mock_logger():
    logger = create_autospec(BotLogger, instance=True)
    return logger


@pytest.fixture
def mock_clients():
    client_a = MagicMock(spec=PaperTradeClient)
    client_b = MagicMock(spec=PaperTradeClient)
    client_a.write = MagicMock()
    client_b.write = MagicMock()
    client_a.read = MagicMock()
    client_b.read = MagicMock()
    return client_a, client_b


def test_paper_staker_claim(mock_clients, mock_logger):
    client_a, client_b = mock_clients
    client_a.write.claim.return_value = {
        "intentId": "0x111",
        "paperMintedRaw": str(10 * 10**18),
    }

    staker = PaperStaker(client_a, client_b, mock_logger, dry_run=False)
    claimed = staker.claim_paper("A", client_a)

    assert claimed == Decimal("10")
    client_a.write.claim.assert_called_once_with(dry_run=False)
    mock_logger.paper_event.assert_called_once_with(
        action="claim", amount=10.0, slot="A", tx_hash="0x111"
    )


def test_paper_staker_stake(mock_clients, mock_logger):
    client_a, client_b = mock_clients
    client_a.write.stake.return_value = {"intentId": "0x222", "ok": True}

    staker = PaperStaker(client_a, client_b, mock_logger, dry_run=False)
    res = staker.stake_paper("A", client_a, Decimal("25.5"))

    assert res["ok"] is True
    client_a.write.stake.assert_called_once_with(25.5, dry_run=False)
    mock_logger.paper_event.assert_called_once_with(
        action="stake", amount=25.5, slot="A", tx_hash="0x222"
    )


def test_claim_and_stake_both(mock_clients, mock_logger):
    from unittest.mock import patch
    from bot.live import WalletSnapshot
    client_a, client_b = mock_clients
    client_a.write._wallet.address = "0xaaa"
    client_b.write._wallet.address = "0xbbb"
    snaps = {
        "0xaaa": WalletSnapshot("0xaaa", Decimal(0), Decimal(0), Decimal(0), Decimal("1500"), Decimal(0)),
        "0xbbb": WalletSnapshot("0xbbb", Decimal(0), Decimal(0), Decimal(0), Decimal("2500"), Decimal(0)),
    }
    client_a.write.claim.return_value = {"intentId": "0x1"}
    client_b.write.claim.return_value = {"intentId": "0x2"}
    client_a.write.stake.return_value = {"intentId": "0x3"}
    client_b.write.stake.return_value = {"intentId": "0x4"}

    staker = PaperStaker(client_a, client_b, mock_logger, dry_run=False)
    with patch("bot.live.get_snapshot", side_effect=lambda addr, **k: snaps[addr]):
        result = staker.claim_and_stake_both()

    assert result["a_paper"] == Decimal("1500")
    assert result["b_paper"] == Decimal("2500")
    assert result["total"] == Decimal("4000")
    client_a.write.stake.assert_called_once_with(Decimal("1500"), dry_run=False)


def test_get_staked_balance(mock_clients, mock_logger):
    client_a, client_b = mock_clients
    client_a.read.protocol_summary.return_value = {
        "paper": {"staked": str(100 * 10**18)}
    }
    client_a.read.account_funding.return_value = {"coreAccount": False}

    staker = PaperStaker(client_a, client_b, mock_logger, dry_run=False)
    bal = staker.get_staked_balance("0x1234567890123456789012345678901234567890")
    assert bal == Decimal("100")


def test_paper_farmer_setup(tmp_path):
    config = BotConfig(
        wallet_a_path=tmp_path / "a.json",
        wallet_b_path=tmp_path / "b.json",
        dry_run=True,
    )
    logger = create_autospec(BotLogger, instance=True)
    state = BotState(
        slot_a=SlotInfo(label="A", address=""),
        slot_b=SlotInfo(label="B", address=""),
    )

    farmer = PaperFarmer(config=config, logger=logger, state=state)
    wallet_a = Account.create()
    wallet_b = Account.create()

    with patch("bot.farmer.PaperTradeClient") as mock_ptc:
        farmer.setup(wallet_a, wallet_b)
        assert farmer.state.slot_a.address == wallet_a.address
        assert farmer.state.slot_b.address == wallet_b.address
        assert farmer.state.slot_a.state == SlotState.REGISTERED
        assert farmer.state.slot_b.state == SlotState.REGISTERED


def test_paper_farmer_open_hedge(tmp_path):
    config = BotConfig(
        wallet_a_path=tmp_path / "a.json",
        wallet_b_path=tmp_path / "b.json",
        margin_usdc=50.0,
        leverage=100,
        dry_run=True,
    )
    logger = create_autospec(BotLogger, instance=True)
    state = BotState(
        slot_a=SlotInfo(label="A", address="0x111"),
        slot_b=SlotInfo(label="B", address="0x222"),
    )

    farmer = PaperFarmer(config=config, logger=logger, state=state)
    farmer.client_a = MagicMock()
    farmer.client_b = MagicMock()
    farmer.client_a.write.open_position.return_value = {"intentId": "0xintentA"}
    farmer.client_b.write.open_position.return_value = {"intentId": "0xintentB"}

    with patch("time.sleep"):  # Skip sleep for test speed
        intent_a, intent_b = farmer.open_hedge("LONG")

    assert intent_a == "0xintentA"
    assert intent_b == "0xintentB"
    assert farmer.state.slot_a.state == SlotState.OPEN
    assert farmer.state.slot_a.side == "LONG"
    assert farmer.state.slot_b.state == SlotState.OPEN
    assert farmer.state.slot_b.side == "SHORT"


def test_paper_farmer_detect_bust_cycle(tmp_path):
    from unittest.mock import patch
    from bot.live import WalletSnapshot, OpenPosition
    config = BotConfig(wallet_a_path=tmp_path / "a.json", wallet_b_path=tmp_path / "b.json")
    logger = create_autospec(BotLogger, instance=True)
    state = BotState(
        slot_a=SlotInfo(label="A", address="0x111", state=SlotState.OPEN, opened_at_ms=1000),
        slot_b=SlotInfo(label="B", address="0x222", state=SlotState.OPEN, opened_at_ms=1000),
    )
    farmer = PaperFarmer(config=config, logger=logger, state=state)
    farmer.monitor = MagicMock()
    pos = OpenPosition(1, 0, True, 1, Decimal(100), 100, 1, 1000, None)
    snap = lambda open_: WalletSnapshot("x", Decimal(0), Decimal(0), Decimal(0), Decimal(0), Decimal(0), open_positions=open_)

    # A gone + liquidation event -> 'a'
    live = {"0x111": snap([]), "0x222": snap([pos])}
    farmer.monitor.detect_bust.side_effect = lambda addr, ts: {"kind": "Liquidated"}
    with patch("bot.live.get_snapshot", side_effect=lambda a, **k: live[a]):
        assert farmer.detect_bust_cycle() == "a"

    # both still open -> None
    state.slot_a.state = state.slot_b.state = SlotState.OPEN
    live = {"0x111": snap([pos]), "0x222": snap([pos])}
    with patch("bot.live.get_snapshot", side_effect=lambda a, **k: live[a]):
        assert farmer.detect_bust_cycle() is None

    # position vanished WITHOUT liquidation event -> None, slot reset to IDLE
    live = {"0x111": snap([]), "0x222": snap([pos])}
    farmer.monitor.detect_bust.side_effect = lambda addr, ts: None
    with patch("bot.live.get_snapshot", side_effect=lambda a, **k: live[a]):
        assert farmer.detect_bust_cycle() is None
    assert state.slot_a.state == SlotState.IDLE


def test_paper_farmer_handle_bust(tmp_path):
    config = BotConfig(
        wallet_a_path=tmp_path / "a.json",
        wallet_b_path=tmp_path / "b.json",
        margin_usdc=50.0,
        dry_run=True,
    )
    logger = create_autospec(BotLogger, instance=True)
    state = BotState(
        slot_a=SlotInfo(label="A", address="0x111", state=SlotState.OPEN, opened_at_ms=1000),
        slot_b=SlotInfo(label="B", address="0x222", state=SlotState.OPEN, opened_at_ms=1000),
    )
    farmer = PaperFarmer(config=config, logger=logger, state=state)
    farmer.client_a = MagicMock()
    farmer.client_b = MagicMock()
    farmer.monitor = MagicMock()
    from bot.live import WalletSnapshot, OpenPosition
    _pos = OpenPosition(1234, 0, False, 1, Decimal(50), 100, 1, 1000, None)
    _snap = WalletSnapshot("0x222", Decimal(0), Decimal(0), Decimal(0), Decimal(0), Decimal(0), open_positions=[_pos])
    farmer.staker = MagicMock()
    farmer.staker.claim_paper.return_value = Decimal("15.0")
    farmer.staker.claim_and_stake_both.return_value = {
        "a_paper": Decimal("15.0"),
        "b_paper": Decimal("0.0"),
        "total": Decimal("15.0"),
    }

    from unittest.mock import patch
    with patch("bot.live.get_snapshot", return_value=_snap):
        farmer.handle_bust("a")

    # Winner is B, should have closed positions
    farmer.client_b.write.close_positions.assert_called_once_with([1234], dry_run=True)
    farmer.staker.claim_paper.assert_not_called()   # minted PAPER is read from the snapshot (claim() answers 409)
    assert farmer.state.total_cycles == 1
    assert farmer.state.slot_a.state == SlotState.IDLE
    assert farmer.state.slot_b.state == SlotState.IDLE
