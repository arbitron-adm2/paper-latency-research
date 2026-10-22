"""Withdrawal to HyperEVM: scale, onward signature, body shape (all verified against the production UI bundle)."""
from decimal import Decimal

from eth_account import Account
from eth_account.messages import encode_typed_data

from papertrade import write as W
from papertrade.client import PaperTradeClient


def _client():
    acct = Account.create()
    return acct, PaperTradeClient(wallet=acct)


def test_fmt_usdc6_matches_ui_formatting():
    assert W._fmt_usdc6(15_807_471) == "15.807471"
    assert W._fmt_usdc6(15_000_000) == "15"
    assert W._fmt_usdc6(100_001) == "0.100001"
    assert W._fmt_usdc6(1) == "0.000001"


def test_withdraw_to_core_amount_is_6_decimals_not_18():
    _, c = _client()
    body = c.write.withdraw_to_core(12.5, dry_run=True)["would_post"]
    assert body["signed"]["amount"] == "12500000"          # was 12.5e18 before the fix -> would never be accepted


def test_withdraw_to_evm_body_and_onward_signature_recovers_to_wallet():
    acct, c = _client()
    body = c.write.withdraw_to_evm("15.907471", dry_run=True)["would_post"]
    assert body["action"] == "withdrawToCore" and body["signed"]["amount"] == "15907471"
    wd = body["withdrawal"]
    assert wd["route"] == "hyperevm" and "destination" not in wd
    on = wd["onward"]
    assert on["maximumFee"] == "0" and on["createRecipient"] is False
    msg = {"hyperliquidChain": "Mainnet", "destination": W.HL_EVM_SYSTEM_ADDRESS, "sourceDex": "spot",
           "destinationDex": "spot", "token": "USDC", "amount": "15.807471", "fromSubAccount": "",
           "nonce": int(on["nonce"])}                                  # onward = sent - 0.1 USDC
    em = encode_typed_data(domain_data={**W.HL_SIGN_DOMAIN, "chainId": 999},
                           message_types={W.HL_SENDASSET: W.HL_SENDASSET_FIELDS}, message_data=msg)
    sg = on["signature"]
    assert sg["v"] in ("27", "28") and sg["r"].startswith("0x") and len(sg["r"]) == 66
    assert Account.recover_message(em, vrs=(int(sg["v"]), int(sg["r"], 16), int(sg["s"], 16))) == acct.address


def test_withdraw_to_evm_rejects_amount_not_above_fee():
    _, c = _client()
    import pytest
    with pytest.raises(ValueError):
        c.write.withdraw_to_evm(Decimal("0.1"), dry_run=True)
