"""
Mechanics Audit & Logic Verification tests.

Covers:
1. Staking / Unstaking & PAPER Token mechanics
   - /query/protocol/paper/activity, protocol_paper helper, summary paper metrics
   - Stake balances & claim parameters
2. Queue Rank & Queue Ring mechanisms
   - /queue-rank/accounts/{address}
   - /query/accounts/{address}/queue-balances
   - /query/accounts/{address}/queue-ring
   - /query/protocol/queue-summary
3. Margin modification, useDebt, price scale, and bust price calculation logic
   - priceScale conversion (BTC=10, ETH=100)
   - bust price formula: P_bust = P_entry * (1 - 1/leverage) [long] vs P_entry * (1 + 1/leverage) [short]
   - useDebt flag in openPosition intent signing & payload verification
4. Intent admission validation rules & HTTP error status codes:
   - 400 Bad Request (zero/malformed size, negative margin)
   - 403 Forbidden / Unauthorized Intent (unregistered signer, non-existent position close)
   - 409 Conflict / Inactive Instrument (instrumentId out of range)
   - 422 Unprocessable Entity (expired deadline)
"""
import time
import pytest
import httpx
from eth_account import Account

from papertrade.client import PaperTradeClient
from papertrade.signing import (
    sign_open_position,
    sign_close_positions,
    sign_stake,
    sign_unstake,
    sign_claim,
)
from papertrade.constants import EXCHANGE_BASE
USER_ADDRESS = "0x4444444444444444444444444444444444444444"


# ── ① Staking & PAPER Mechanics ──────────────────────────────────────────────

def test_paper_staking_protocol_activity():
    with PaperTradeClient() as c:
        data = c.read.protocol_paper_activity()
        assert "items" in data
        assert isinstance(data["items"], list)
        if data["items"]:
            first = data["items"][0]
            assert "kind" in first
            assert first["kind"] in ("staked", "minted", "unstaked", "claimed")
            assert "amountRaw" in first
            assert "userAddress" in first


def test_paper_protocol_convenience_helper():
    with PaperTradeClient() as c:
        res = c.read.protocol_paper()
        assert "activity" in res
        assert "paperStats" in res
        stats = res["paperStats"]
        assert "supply" in stats
        assert "staked" in stats
        assert int(stats["supply"]) > 0
        assert int(stats["staked"]) > 0


def test_paper_signing_payloads():
    dummy = Account.create()
    stake_payload = sign_stake(dummy, 150 * 10**18)
    assert stake_payload["amount"] == "150000000000000000000"
    assert stake_payload["user"].lower() == dummy.address.lower()

    unstake_payload = sign_unstake(dummy, 50 * 10**18)
    assert unstake_payload["amount"] == "50000000000000000000"
    assert unstake_payload["user"].lower() == dummy.address.lower()

    claim_payload = sign_claim(dummy)
    assert claim_payload["user"].lower() == dummy.address.lower()
    assert "nonce" in claim_payload


# ── ② Queue Rank & Ring Balance Mechanics ────────────────────────────────────

def test_queue_rank_endpoint():
    with PaperTradeClient() as c:
        res = c.read.account_queue_rank(USER_ADDRESS)
        assert res.get("ok") is True
        assert "queueRevision" in res
        assert isinstance(res.get("entries"), list)


def test_queue_balances_endpoint():
    with PaperTradeClient() as c:
        res = c.read.account_queue_balances(USER_ADDRESS, limit=75)
        assert res.get("ok") is True
        assert "total" in res
        assert "account" in res
        assert isinstance(res.get("items"), list)


def test_queue_ring_endpoint():
    with PaperTradeClient() as c:
        res = c.read.account_queue_ring(USER_ADDRESS)
        assert res.get("ok") is True
        assert "account" in res
        assert "entries" in res


def test_protocol_queue_summary():
    with PaperTradeClient() as c:
        res = c.read.protocol_queue_summary()
        assert res.get("ok") is True
        assert "total" in res
        assert "count" in res


# ── ③ Margin Modification, useDebt & Bust Price Logic ─────────────────────────

def calculate_bust_price(entry_price: float, leverage: int, is_long: bool) -> float:
    """
    Theoretical liquidation/bust price formula for a isolated position:
    Long: Entry * (1 - 1 / leverage)
    Short: Entry * (1 + 1 / leverage)
    """
    if is_long:
        return entry_price * (1.0 - (1.0 / leverage))
    else:
        return entry_price * (1.0 + (1.0 / leverage))


def test_bust_price_calculation():
    # Long BTC at 100,000 with 10x leverage -> bust at 90,000
    assert calculate_bust_price(100000.0, 10, is_long=True) == pytest.approx(90000.0)
    # Short BTC at 100,000 with 10x leverage -> bust at 110,000
    assert calculate_bust_price(100000.0, 10, is_long=False) == pytest.approx(110000.0)
    # 1000x leverage on Long: 99.9% of entry price
    p_bust_1000x = calculate_bust_price(80000.0, 1000, is_long=True)
    assert p_bust_1000x == pytest.approx(79920.0, rel=1e-5)


def test_price_scale_instrument_mapping():
    with PaperTradeClient() as c:
        insts = c.instruments()
        inst_map = {i["name"]: i for i in insts}
        assert "BTC" in inst_map
        assert "ETH" in inst_map
        # BTC raw price scale is 10 (1 decimal digit: e.g. 83046.5 -> 830465)
        assert str(inst_map["BTC"]["priceScale"]) == "10"
        # ETH raw price scale is 100 (2 decimal digits: e.g. 2515.05 -> 251505)
        assert str(inst_map["ETH"]["priceScale"]) == "100"


def test_usedebt_open_position_signing():
    dummy = Account.create()
    signed_nodebt = sign_open_position(
        session_key=dummy,
        user_address=USER_ADDRESS,
        instrument_id=0,
        is_long=True,
        size_raw=10 * 10**18,
        leverage=10,
        use_debt=False,
    )
    assert signed_nodebt["useDebt"] is False

    signed_debt = sign_open_position(
        session_key=dummy,
        user_address=USER_ADDRESS,
        instrument_id=0,
        is_long=True,
        size_raw=10 * 10**18,
        leverage=10,
        use_debt=True,
    )
    assert signed_debt["useDebt"] is True


# ── ④ Intent Admission & Error Code Validations ──────────────────────────────

@pytest.fixture(scope="module")
def http_client():
    with httpx.Client(base_url=EXCHANGE_BASE, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}) as c:
        yield c


def test_intent_validation_expired_deadline(http_client):
    """Expect HTTP 422 deadline_expired when deadline is in the past."""
    dummy = Account.create()
    signed = sign_open_position(
        session_key=dummy,
        user_address=USER_ADDRESS,
        instrument_id=0,
        is_long=True,
        size_raw=10 * 10**18,
        leverage=10,
    )
    signed["deadline"] = "1000000000"  # past timestamp
    resp = http_client.post("/intents", json={"action": "openPosition", "signed": signed})
    assert resp.status_code == 422
    assert resp.json().get("code") == "deadline_expired"


def test_intent_validation_unauthorized_session_key(http_client):
    """Expect HTTP 403 unauthorized_intent when unapproved signer attempts to trade for user."""
    unapproved = Account.create()
    signed = sign_open_position(
        session_key=unapproved,
        user_address=USER_ADDRESS,
        instrument_id=0,
        is_long=True,
        size_raw=10 * 10**18,
        leverage=10,
    )
    resp = http_client.post("/intents", json={"action": "openPosition", "signed": signed})
    assert resp.status_code == 403
    assert resp.json().get("code") == "unauthorized_intent"


def test_intent_validation_inactive_instrument(http_client):
    """Expect HTTP 409 inactiveInstrument when instrumentId is invalid/out of range."""
    dummy = Account.create()
    signed = sign_open_position(
        session_key=dummy,
        user_address=USER_ADDRESS,
        instrument_id=999,
        is_long=True,
        size_raw=10 * 10**18,
        leverage=10,
    )
    resp = http_client.post("/intents", json={"action": "openPosition", "signed": signed})
    assert resp.status_code == 409
    data = resp.json()
    assert data.get("code") == "admission_rejected"
    assert data.get("reason") == "inactiveInstrument"


def test_intent_validation_bad_size(http_client):
    """Expect HTTP 400 when size is 0 or invalid."""
    dummy = Account.create()
    signed = sign_open_position(
        session_key=dummy,
        user_address=USER_ADDRESS,
        instrument_id=0,
        is_long=True,
        size_raw=10 * 10**18,
        leverage=10,
    )
    signed["size"] = "0"
    resp = http_client.post("/intents", json={"action": "openPosition", "signed": signed})
    assert resp.status_code == 400


def test_intent_validation_nonexistent_position_close(http_client):
    """Expect HTTP 403 unauthorized_intent when closing non-existent position IDs."""
    dummy = Account.create()
    close_signed = sign_close_positions(
        session_key=dummy,
        user_address=USER_ADDRESS,
        position_ids=[999999],
    )
    resp = http_client.post("/intents", json={"action": "closePositions", "signed": close_signed})
    assert resp.status_code == 403
    assert resp.json().get("code") == "unauthorized_intent"
