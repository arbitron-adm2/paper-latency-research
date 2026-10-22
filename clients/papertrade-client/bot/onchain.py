from __future__ import annotations

from decimal import Decimal
import logging
from typing import Any, Optional, Union

from eth_account.signers.local import LocalAccount
import httpx
from web3 import Web3

logger = logging.getLogger(__name__)

# Defaults for HyperEVM
DEFAULT_RPC_URL = "https://rpc.hyperliquid.xyz/evm"
DEFAULT_UNDERLYING_USDC = "0xb88339cb7199b77e23db6e890353e22632ba630f"
DEFAULT_COLLATERAL_ADDR = "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24"
DEFAULT_FACTORY_ADDR = "0x1629b7d46bef6e13ff754174fc9f32dea309b38f"
DEFAULT_EXCHANGE_BASE = "https://exchange.papertrade.xyz"
USDC_DECIMALS = 6
DESTINATION_DEX_HYPEREVM = 4294967295

# Minimal ERC20 ABI (balanceOf, transfer, approve)
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "value", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

# Factory ABI to compute deposit proxy address
FACTORY_ABI = [
    {
        "inputs": [{"name": "user", "type": "address"}],
        "name": "computeProxyAddress",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Collateral contract ABI for depositFor
COLLATERAL_DEPOSIT_ABI = [
    {
        "inputs": [
            {"name": "recipient", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "destinationDex", "type": "uint32"},
        ],
        "name": "depositFor",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


def get_w3(rpc_url: str = DEFAULT_RPC_URL) -> Web3:
    """Connect to RPC endpoint and return Web3 instance."""
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not w3.is_connected():
            raise RuntimeError(f"Failed to connect to HyperEVM RPC at {rpc_url}")
        return w3
    except Exception as exc:
        if isinstance(exc, RuntimeError):
            raise
        raise RuntimeError(f"Error initializing Web3 connection to {rpc_url}: {exc}") from exc


def get_hype_balance(
    address: str,
    rpc_url: str = DEFAULT_RPC_URL,
) -> Decimal:
    """Return native HYPE balance for the given address in Decimal ether units."""
    try:
        w3 = get_w3(rpc_url)
        checksum_addr = Web3.to_checksum_address(address)
        balance_wei = w3.eth.get_balance(checksum_addr)
        return Decimal(balance_wei) / Decimal(10**18)
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch HYPE balance for {address}: {exc}") from exc


def get_usdc_balance(
    address: str,
    usdc_addr: str = DEFAULT_UNDERLYING_USDC,
    rpc_url: str = DEFAULT_RPC_URL,
) -> Decimal:
    """Return USDC balance (6 decimals) for the given address."""
    try:
        w3 = get_w3(rpc_url)
        checksum_user = Web3.to_checksum_address(address)
        checksum_usdc = Web3.to_checksum_address(usdc_addr)
        contract = w3.eth.contract(address=checksum_usdc, abi=ERC20_ABI)
        balance_raw = contract.functions.balanceOf(checksum_user).call()
        return Decimal(balance_raw) / Decimal(10**USDC_DECIMALS)
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch USDC balance for {address}: {exc}") from exc


def _send_signed_transaction(
    w3: Web3,
    sender: LocalAccount,
    tx: dict[str, Any],
) -> str:
    """Sign and send a transaction, returning the tx hash hex string."""
    signed_tx = sender.sign_transaction(tx)
    raw_tx = getattr(signed_tx, "raw_transaction", None)
    if raw_tx is None:
        raw_tx = getattr(signed_tx, "rawTransaction")
    tx_hash = w3.eth.send_raw_transaction(raw_tx)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt.get("status") == 0:
        raise RuntimeError(f"Transaction reverted onchain: {tx_hash.hex()}")
    return tx_hash.hex()


def transfer_usdc(
    sender: LocalAccount,
    to: str,
    amount_usdc: Decimal,
    usdc_addr: str = DEFAULT_UNDERLYING_USDC,
    rpc_url: str = DEFAULT_RPC_URL,
) -> str:
    """Transfer USDC to a recipient address and return the txhash."""
    try:
        w3 = get_w3(rpc_url)
        checksum_sender = Web3.to_checksum_address(sender.address)
        checksum_to = Web3.to_checksum_address(to)
        checksum_usdc = Web3.to_checksum_address(usdc_addr)

        amount_raw = int(amount_usdc * (Decimal(10) ** USDC_DECIMALS))
        if amount_raw <= 0:
            raise ValueError(f"Transfer amount must be positive, got {amount_usdc}")

        contract = w3.eth.contract(address=checksum_usdc, abi=ERC20_ABI)
        nonce = w3.eth.get_transaction_count(checksum_sender, "pending")
        chain_id = w3.eth.chain_id

        gas_price = w3.eth.gas_price
        tx_data = contract.functions.transfer(checksum_to, amount_raw).build_transaction({
            "from": checksum_sender,
            "nonce": nonce,
            "gasPrice": gas_price,
            "chainId": chain_id,
        })
        try:
            gas_est = w3.eth.estimate_gas(tx_data)
            tx_data["gas"] = int(gas_est * 1.2)
        except Exception:
            tx_data["gas"] = 100_000

        return _send_signed_transaction(w3, sender, tx_data)
    except Exception as exc:
        raise RuntimeError(f"Failed to transfer USDC from {sender.address} to {to}: {exc}") from exc


def deposit_usdc_to_papertrade(
    sender: LocalAccount,
    amount_usdc: Decimal,
    usdc_addr: str = DEFAULT_UNDERLYING_USDC,
    factory_addr: str = DEFAULT_FACTORY_ADDR,
    rpc_url: str = DEFAULT_RPC_URL,
    collateral_addr: str = DEFAULT_COLLATERAL_ADDR,
    exchange_base_url: str = DEFAULT_EXCHANGE_BASE,
) -> str:
    """
    Deposit USDC into PaperTrade:
    1) proxy = DepositProxyFactory(factory_addr).computeProxyAddress(sender.address)
    2) approve spender=collateral_addr for amount_raw on underlying USDC token
    3) call depositFor(proxy, amount_raw, 4294967295) on collateral_addr
    4) POST /funding/deposits/check {"user": sender.address.lower()}
    Returns the deposit transaction hash hex string.
    """
    try:
        w3 = get_w3(rpc_url)
        sender_addr = Web3.to_checksum_address(sender.address)
        factory_checksum = Web3.to_checksum_address(factory_addr)
        collateral_checksum = Web3.to_checksum_address(collateral_addr)
        usdc_checksum = Web3.to_checksum_address(usdc_addr)

        amount_raw = int(amount_usdc * (Decimal(10) ** USDC_DECIMALS))
        if amount_raw <= 0:
            raise ValueError(f"Deposit amount must be positive, got {amount_usdc}")

        # Step 1: Compute proxy address
        factory_contract = w3.eth.contract(address=factory_checksum, abi=FACTORY_ABI)
        proxy_address = factory_contract.functions.computeProxyAddress(sender_addr).call()
        proxy_checksum = Web3.to_checksum_address(proxy_address)

        # Step 2: Approve collateral contract to spend underlying USDC
        usdc_contract = w3.eth.contract(address=usdc_checksum, abi=ERC20_ABI)
        chain_id = w3.eth.chain_id
        nonce = w3.eth.get_transaction_count(sender_addr, "pending")
        gas_price = w3.eth.gas_price

        approve_tx_data = usdc_contract.functions.approve(collateral_checksum, amount_raw).build_transaction({
            "from": sender_addr,
            "nonce": nonce,
            "gasPrice": gas_price,
            "chainId": chain_id,
        })
        try:
            gas_est = w3.eth.estimate_gas(approve_tx_data)
            approve_tx_data["gas"] = int(gas_est * 1.2)
        except Exception:
            approve_tx_data["gas"] = 80_000

        _send_signed_transaction(w3, sender, approve_tx_data)

        # Step 3: Call collateral.depositFor(proxy, amount_raw, destinationDex=4294967295)
        collateral_contract = w3.eth.contract(address=collateral_checksum, abi=COLLATERAL_DEPOSIT_ABI)
        nonce = w3.eth.get_transaction_count(sender_addr, "pending")
        deposit_tx_data = collateral_contract.functions.depositFor(
            proxy_checksum,
            amount_raw,
            DESTINATION_DEX_HYPEREVM,
        ).build_transaction({
            "from": sender_addr,
            "nonce": nonce,
            "gasPrice": gas_price,
            "chainId": chain_id,
        })
        try:
            gas_est = w3.eth.estimate_gas(deposit_tx_data)
            deposit_tx_data["gas"] = int(gas_est * 1.2)
        except Exception:
            deposit_tx_data["gas"] = 150_000

        deposit_tx_hash = _send_signed_transaction(w3, sender, deposit_tx_data)

        # Step 4: Notify PaperTrade backend
        try:
            check_url = f"{exchange_base_url.rstrip('/')}/funding/deposits/check"
            with httpx.Client(timeout=15.0) as client:
                res = client.post(check_url, json={"user": sender.address.lower()})
                if res.status_code != 200:
                    logger.warning(
                        "Deposit check notification returned status %s: %s",
                        res.status_code,
                        res.text,
                    )
        except Exception as check_exc:
            logger.warning("Failed to call deposit check endpoint: %s", check_exc)

        return deposit_tx_hash

    except Exception as exc:
        raise RuntimeError(
            f"Failed deposit of {amount_usdc} USDC for {sender.address}: {exc}"
        ) from exc


def check_funding_requirements(
    address: str,
    min_hype: Decimal = Decimal("0.01"),
    min_usdc: Decimal = Decimal("0.5"),
    usdc_addr: str = DEFAULT_UNDERLYING_USDC,
    rpc_url: str = DEFAULT_RPC_URL,
) -> dict[str, Any]:
    """
    Check if the account holds enough HYPE for gas and enough USDC for trading.
    Returns: {ok: bool, hype: Decimal, usdc: Decimal, errors: list}
    """
    errors: list[str] = []
    hype_bal = Decimal("0")
    usdc_bal = Decimal("0")

    try:
        hype_bal = get_hype_balance(address, rpc_url=rpc_url)
        if hype_bal < min_hype:
            errors.append(f"Insufficient HYPE balance: {hype_bal} < {min_hype}")
    except Exception as exc:
        errors.append(f"Failed to query HYPE balance: {exc}")

    try:
        usdc_bal = get_usdc_balance(address, usdc_addr=usdc_addr, rpc_url=rpc_url)
        if usdc_bal < min_usdc:
            errors.append(f"Insufficient USDC balance: {usdc_bal} < {min_usdc}")
    except Exception as exc:
        errors.append(f"Failed to query USDC balance: {exc}")

    return {
        "ok": len(errors) == 0,
        "hype": hype_bal,
        "usdc": usdc_bal,
        "errors": errors,
    }
