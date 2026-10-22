#!/usr/bin/env python3
"""
PaperTrade delta-neutral farmer.

  python run_bot.py setup            # create wallets/wallet_a.json + wallet_b.json (0600), print ADDRESSES only
  python run_bot.py status           # balances: HYPE gas, USDC (EVM), exchange collateral, open positions, PAPER
  python run_bot.py fund A 100       # deposit 100 USDC from wallet A EVM balance into PaperTrade (needs --yes)
  python run_bot.py run              # farming loop, DRY-RUN (signs but never sends)
  python run_bot.py run --live       # real money
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path

import click

from bot.config import BotConfig
from bot.live import get_snapshot
from bot.logger import BotLogger
from bot.state import BotState
import bot.onchain as onchain
from papertrade.wallet import generate_okx_wallet, save_wallet, load_wallet

WALLET_DIR = Path("wallets")
STATE_FILE = Path("bot_state.json")


def _paths():
    return WALLET_DIR / "wallet_a.json", WALLET_DIR / "wallet_b.json"


@click.group()
def main():
    """PaperTrade farmer."""


@main.command()
def setup():
    """Generate two wallets. Keys stay in wallets/*.json (chmod 600, gitignored)."""
    WALLET_DIR.mkdir(exist_ok=True)
    WALLET_DIR.chmod(0o700)
    for label, path in zip("AB", _paths()):
        if path.exists():
            click.echo(f"Wallet {label} exists: {load_wallet(path).address}")
            continue
        info = generate_okx_wallet()
        save_wallet(info, path)
        click.echo(f"Wallet {label} created: {info['address']}")
    click.echo(
        "\nFund EACH address on HyperEVM (chain 999):\n"
        "  - ~0.1-0.3 HYPE for gas\n"
        "  - USDC (native Circle USDC 0xb883...630f, NOT USDT0): margin + buffer, min deposit 10 USDC\n"
        "Back up wallets/*.json (they contain the seed phrases). Never share them.\n"
        "Optional: import the mnemonic into OKX Wallet to view/withdraw funds."
    )


def _cfg(**kw) -> BotConfig:
    a, b = _paths()
    return BotConfig(wallet_a_path=a, wallet_b_path=b, **kw)


@main.command()
def status():
    """Show balances and positions of both wallets."""
    cfg = _cfg()
    for label, path in zip("AB", _paths()):
        if not path.exists():
            click.echo(f"{label}: wallet file missing (run setup)")
            continue
        w = load_wallet(path)
        click.echo(f"\n[{label}] {w.address}")
        try:
            click.echo(f"  HYPE gas     : {onchain.get_hype_balance(w.address, cfg.hyperevm_rpc)}")
            click.echo(f"  USDC (EVM)   : {onchain.get_usdc_balance(w.address, cfg.usdc_address, cfg.hyperevm_rpc)}")
        except Exception as exc:  # noqa: BLE001
            click.echo(f"  on-chain read failed: {exc}")
        try:
            s = get_snapshot(w.address)
            click.echo(f"  Exchange     : balance={s.balance} available={s.available} locked={s.locked}")
            click.echo(f"  PAPER        : unstaked={s.paper_balance} staked={s.paper_staked}")
            click.echo(f"  Session key  : {'active' if s.has_session_key(w.address) else 'NOT registered'}")
            for p in s.open_positions:
                click.echo(f"  Position #{p.position_id}: {'LONG' if p.is_long else 'SHORT'} x{p.leverage} margin={p.margin} bust@{p.bust_price_raw}")
        except Exception as exc:  # noqa: BLE001
            click.echo(f"  exchange read failed: {exc}")


@main.command()
@click.argument("slot", type=click.Choice(["A", "B"]))
@click.argument("amount", type=float)
@click.option("--yes", is_flag=True, help="Actually send the transactions.")
def fund(slot, amount, yes):
    """Deposit USDC from the wallet's EVM balance into PaperTrade (approve + depositFor + notify)."""
    if amount < 10:
        raise click.UsageError("Minimum deposit is 10 USDC (first deposit also pays an activation fee).")
    cfg = _cfg()
    w = load_wallet(_paths()["AB".index(slot)])
    bal = onchain.get_usdc_balance(w.address, cfg.usdc_address, cfg.hyperevm_rpc)
    click.echo(f"Wallet {slot} {w.address}: USDC {bal}, depositing {amount}")
    if Decimal(str(amount)) > bal:
        raise click.ClickException("Insufficient USDC on HyperEVM.")
    if not yes:
        click.echo("Dry check OK. Re-run with --yes to send.")
        return
    tx = onchain.deposit_usdc_to_papertrade(
        sender=w, amount_usdc=Decimal(str(amount)), usdc_addr=cfg.usdc_address,
        factory_addr=cfg.deposit_factory, rpc_url=cfg.hyperevm_rpc, collateral_addr=cfg.collateral_address,
    )
    click.echo(f"deposit tx: {tx}")


@main.command()
@click.option("--live", is_flag=True, help="Send real intents. Without it: dry-run.")
@click.option("--margin", default=100.0, show_default=True)
@click.option("--leverage", default=100, show_default=True)
@click.option("--instrument", default="BTC", type=click.Choice(["BTC", "ETH"]))
@click.option("--max-cycles", default=0, show_default=True, help="0 = infinite")
@click.option("--auto-rebalance", is_flag=True, help="UNTESTED fund-moving rebalance path.")
def run(live, margin, leverage, instrument, max_cycles, auto_rebalance):
    """Run the farming loop."""
    from bot.farmer import PaperFarmer

    if margin * leverage < 10_000:
        raise click.UsageError(
            f"Notional {margin * leverage:,.0f} < $10,000 minimum position value. "
            f"Use leverage >= {10_000 / margin:.0f} for margin {margin}."
        )
    cfg = _cfg(margin_usdc=margin, leverage=leverage, instrument=instrument,
               dry_run=not live, auto_rebalance=auto_rebalance)
    a, b = _paths()
    if not (a.exists() and b.exists()):
        raise click.ClickException("Run `setup` first.")
    logger = BotLogger(cfg.log_file)
    from bot.state import SlotInfo
    if STATE_FILE.exists():
        state = BotState.load(STATE_FILE)
    else:
        state = BotState(slot_a=SlotInfo(label="A", address="", side="LONG"),
                         slot_b=SlotInfo(label="B", address="", side="SHORT"))
    click.echo(f"mode={'LIVE' if live else 'DRY-RUN'} {instrument} x{leverage} margin={margin}")
    farmer = PaperFarmer(cfg, logger, state)
    farmer.setup(load_wallet(a), load_wallet(b))
    farmer.run(max_cycles=max_cycles)


@main.command()
@click.option("--deep", is_flag=True, help="Also run the test suite, git/unit-file checks and a panic dry-run (daily).")
@click.option("--as-json", "as_json", is_flag=True, help="Machine readable output.")
def doctor(deep, as_json):
    """Check every system. Exit code: 0 ok, 1 warnings, 2 critical."""
    from bot import doctor as D
    res = D.run_all(deep=deep)
    if as_json:
        click.echo(json.dumps({"verdict": D.verdict(res), "results": res}, default=str))
    else:
        click.echo(D.format_text(res))
    raise SystemExit(D.verdict(res))


@main.command("ack-backup")
def ack_backup():
    """Record that you have saved wallets/wallet_a.json (seed phrase) somewhere safe: silences the reminder."""
    f = WALLET_DIR / "BACKUP_CONFIRMED"
    f.write_text(time.strftime("%F %T"))
    f.chmod(0o600)
    click.echo("recorded")


@main.command()
@click.option("--yes", is_flag=True, help="Actually do it. Without --yes: print the plan only.")
@click.option("--clear", is_flag=True, help="Remove panic.flag so the farmer may open hedges again.")
@click.option("--reason", default="manual CLI")
@click.option("--test-amount", type=float, default=None, help="Path test: move only this many USDC (>=10) from wallet A to the backup. No flag, positions untouched.")
def panic(yes, clear, reason, test_amount):
    """Flatten everything and withdraw ALL funds to the backup address (wallets/backup_address)."""
    from bot import panic as P
    if clear:
        click.echo("flag cleared" if P.clear_flag() else "no flag set")
        return
    a, b = _paths()
    wallets = {"A": load_wallet(a)}
    if b.exists() and test_amount is None:
        wallets["B"] = load_wallet(b)
    p = P.Panic(wallets, dry_run=not yes, log=click.echo,
                test_amount=Decimal(str(test_amount)) if test_amount else None)
    click.echo(f"backup address: {p.backup}   mode: {'LIVE' if yes else 'DRY-RUN (add --yes)'}")
    rep = p.run(reason)
    click.echo(json.dumps({k: v for k, v in rep.items() if k != "wallets"}, indent=1))
    for k, v in rep["wallets"].items():
        click.echo(f"{k}: {json.dumps(v)}")


@main.command()
@click.option("--live", is_flag=True, help="Send real intents. Without it: dry-run.")
@click.option("--margin", default=10.0, show_default=True)
@click.option("--leverage", default=1000, show_default=True)
@click.option("--instrument", default="BTC", type=click.Choice(["BTC", "ETH"]))
@click.option("--port", default=8787, show_default=True)
@click.option("--paused", is_flag=True, help="Start paused: monitor/close only, no new hedges.")
@click.option("--single/--two-wallet", default=True, show_default=True,
              help="single: both legs in wallet A (no rebalancing needed). two-wallet: legacy A=LONG / B=SHORT.")
@click.option("--close-fraction", default=0.6, show_default=True, help="Close both legs at this share of the liquidation distance (0 = wait for liquidation).")
@click.option("--max-drawdown", default=0.40, show_default=True, help="Equity stop-loss vs 7d peak => PANIC withdrawal.")
@click.option("--auto-deposit", is_flag=True, help="Single mode: deposit USDC (>= $10) that appears on wallet A's HyperEVM address.")
@click.option("--auto-rebalance", is_flag=True, help="UNTESTED fund-moving rebalance path (two-wallet only).")
def serve(live, margin, leverage, instrument, port, paused, single, close_fraction, max_drawdown, auto_deposit, auto_rebalance):
    """Run as a long-lived service with an HTTP control API (see bot/service.py)."""
    from bot.farmer import PaperFarmer
    from bot.service import FarmerService, load_or_create_token
    from bot.state import SlotInfo

    if margin * leverage < 10_000:
        raise click.UsageError(f"Notional {margin * leverage:,.0f} < $10,000 minimum position value.")
    a, b = _paths()
    if not (a.exists() and b.exists()):
        raise click.ClickException("Run `setup` first.")
    cfg = _cfg(margin_usdc=margin, leverage=leverage, instrument=instrument,
               dry_run=not live, auto_rebalance=auto_rebalance, auto_deposit=auto_deposit,
               close_fraction=close_fraction, max_drawdown=max_drawdown)
    logger = BotLogger(cfg.log_file)
    if STATE_FILE.exists():
        state = BotState.load(STATE_FILE)
    else:
        state = BotState(slot_a=SlotInfo(label="A", address="", side="LONG"),
                         slot_b=SlotInfo(label="B", address="", side="SHORT"))
    if single:
        from bot.single import SingleFarmer
        farmer: PaperFarmer = SingleFarmer(cfg, logger, state)
        farmer.state_path = STATE_FILE.resolve()
        farmer.setup(load_wallet(a))
    else:
        farmer = PaperFarmer(cfg, logger, state)
        farmer.state_path = STATE_FILE.resolve()
        farmer.setup(load_wallet(a), load_wallet(b))
    svc = FarmerService(farmer, cfg.log_file, load_or_create_token(WALLET_DIR / "api_token"), port=port)
    if live:
        from bot.risk import RiskGuard
        svc.panic_wallets = {"A": load_wallet(a)}                 # single mode: the account that holds the funds
        if not single and b.exists():
            svc.panic_wallets["B"] = load_wallet(b)
        svc.risk = RiskGuard(Path("risk_state.json"), max_drawdown=cfg.max_drawdown)
    svc.paused = paused
    logger.info(f"service up: {'LIVE' if live else 'DRY-RUN'} {'single-account' if single else 'two-wallet'} "
                f"{instrument} max-lev={leverage} margin-cap={margin} auto-deposit={auto_deposit} "
                f"api=127.0.0.1:{port} paused={paused}")
    svc.serve()


if __name__ == "__main__":
    main()
