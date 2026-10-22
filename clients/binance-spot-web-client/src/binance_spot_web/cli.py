from __future__ import annotations

import json
import os
import sys

import click

from .client import BinanceSpotWeb
from .exceptions import BinanceWebError, PlanOnly


def _client() -> BinanceSpotWeb:
    return BinanceSpotWeb(
        cookie=os.environ.get("BINANCE_COOKIE"),
        csrf_token=os.environ.get("BINANCE_CSRF_TOKEN"),
        weight_per_minute=int(os.environ.get("BINANCE_WEIGHT_PER_MINUTE", "1200")),
    )


def _emit(payload) -> None:
    click.echo(json.dumps(payload, ensure_ascii=False))


def _run(fn) -> None:
    try:
        _emit(fn())
    except PlanOnly as exc:
        click.echo(str(exc), err=True)
        sys.exit(2)
    except BinanceWebError as exc:
        click.echo(f"error: {exc}", err=True)
        click.echo(
            "hint: public market calls need no key; order entry needs "
            "BINANCE_COOKIE and BINANCE_CSRF_TOKEN plus --execute.",
            err=True,
        )
        sys.exit(1)


@click.group()
def main() -> None:
    """Unofficial binance.com spot and USD-M futures client. Orders print a plan."""


@main.command()
@click.option("--symbol", required=True)
def price(symbol: str) -> None:
    _run(lambda: _client().market.price(symbol))


@main.command()
@click.option("--symbol", required=True)
@click.option("--limit", default=20, show_default=True, type=int)
def depth(symbol: str, limit: int) -> None:
    _run(lambda: _client().market.depth(symbol, limit))


@main.command("agg-trades")
@click.option("--symbol", required=True)
@click.option("--limit", default=5, show_default=True, type=int)
def agg_trades(symbol: str, limit: int) -> None:
    _run(lambda: _client().market.agg_trades(symbol, limit))


@main.command("aor-agg-trades")
@click.option("--symbol", required=True)
@click.option("--limit", default=5, show_default=True, type=int)
def aor_agg_trades(symbol: str, limit: int) -> None:
    _run(lambda: _client().market.aor_agg_trades(symbol, limit))


@main.command()
@click.option("--symbol", required=True)
@click.option("--interval", default="1m", show_default=True)
@click.option("--limit", default=2, show_default=True, type=int)
def klines(symbol: str, interval: str, limit: int) -> None:
    _run(lambda: _client().market.klines(symbol, interval, limit))


@main.command("exchange-info")
@click.option("--symbol", required=True)
def exchange_info(symbol: str) -> None:
    _run(lambda: _client().market.exchange_info(symbol))


@main.command()
@click.option("--stream", "streams", multiple=True, required=True, help="Repeat. Example: btcusdt@trade")
@click.option("--max-events", default=3, show_default=True, type=int)
def watch(streams: tuple[str, ...], max_events: int) -> None:
    def run():
        socket = _client().stream(list(streams))
        try:
            return list(socket.events(limit=max_events))
        finally:
            socket.close()

    _run(run)


@main.command("place-aor")
@click.option("--symbol", required=True)
@click.option("--side", required=True, type=click.Choice(["BUY", "SELL"], case_sensitive=False))
@click.option("--type", "order_type", required=True, type=click.Choice(["LIMIT", "MARKET"], case_sensitive=False))
@click.option("--quantity", default=None)
@click.option("--quote-order-qty", default=None)
@click.option("--price", default=None)
@click.option("--execute", is_flag=True, help="Actually POST. Default is a plan only.")
def place_aor(symbol, side, order_type, quantity, quote_order_qty, price, execute) -> None:
    client = _client()
    kwargs = dict(
        symbol=symbol,
        side=side,
        order_type=order_type,
        quantity=quantity,
        quote_order_qty=quote_order_qty,
        price=price,
    )
    if not execute:
        _emit(client.trading.plan_aor_order(**kwargs))
        click.echo("plan only; pass --execute to POST with your own session", err=True)
        return
    _run(lambda: client.trading.place_aor_order(confirm=True, **kwargs))


@main.command("fut-mark")
@click.option("--symbol", default="BTCUSDT", show_default=True)
def fut_mark(symbol: str) -> None:
    _run(lambda: _client().futures.premium_index(symbol))


@main.command("fut-depth")
@click.option("--symbol", default="BTCUSDT", show_default=True)
@click.option("--limit", default=5, show_default=True, type=int)
def fut_depth(symbol: str, limit: int) -> None:
    _run(lambda: _client().futures.depth(symbol, limit))


@main.command("fut-watch")
@click.option("--stream", "streams", multiple=True, required=True)
@click.option("--product", default="um", show_default=True, type=click.Choice(["um", "cm"]))
@click.option("--channel", default="public", show_default=True, type=click.Choice(["public", "market"]))
@click.option("--max-events", default=2, show_default=True, type=int)
def fut_watch(streams: tuple[str, ...], product: str, channel: str, max_events: int) -> None:
    def run():
        socket = _client().futures.stream(list(streams), product=product, channel=channel)
        try:
            return list(socket.events(limit=max_events))
        finally:
            socket.close()

    _run(run)


@main.command("fut-place")
@click.option("--symbol", required=True)
@click.option("--side", required=True, type=click.Choice(["BUY", "SELL"], case_sensitive=False))
@click.option("--type", "order_type", required=True, type=click.Choice(["LIMIT", "MARKET"], case_sensitive=False))
@click.option("--quantity", required=True)
@click.option("--price", default=None)
@click.option("--position-side", default="BOTH", show_default=True)
@click.option("--product", default="um", show_default=True, type=click.Choice(["um", "cm"]))
@click.option("--execute", is_flag=True, help="Actually POST. Default is a plan only.")
def fut_place(symbol, side, order_type, quantity, price, position_side, product, execute) -> None:
    client = _client()
    kwargs = dict(
        symbol=symbol,
        side=side,
        order_type=order_type,
        quantity=quantity,
        price=price,
        position_side=position_side,
        product=product,
    )
    if not execute:
        _emit(client.futures.plan_order(**kwargs))
        click.echo("plan only; pass --execute to POST with your own session", err=True)
        return
    _run(lambda: client.futures.place_order(confirm=True, **kwargs))


@main.command("ws-ping")
@click.option("--product", default="spot", show_default=True, type=click.Choice(["spot", "um"]))
def ws_ping(product: str) -> None:
    from .official_ws import OfficialTradeSocket

    socket = OfficialTradeSocket(product=product, api_key="", api_secret="")
    try:
        _run(socket.ping)
    finally:
        socket.close()


@main.command("ws-place")
@click.option("--product", default="spot", show_default=True, type=click.Choice(["spot", "um"]))
@click.option("--symbol", required=True)
@click.option("--side", required=True, type=click.Choice(["BUY", "SELL"], case_sensitive=False))
@click.option("--type", "order_type", required=True, type=click.Choice(["LIMIT", "MARKET"], case_sensitive=False))
@click.option("--quantity", required=True)
@click.option("--price", default=None)
@click.option("--position-side", default=None)
@click.option("--execute", is_flag=True, help="Sign and send order.place. Default is a plan only.")
def ws_place(product, symbol, side, order_type, quantity, price, position_side, execute) -> None:
    from .official_ws import OfficialTradeSocket

    socket = OfficialTradeSocket(product=product)
    kwargs = dict(
        symbol=symbol,
        side=side,
        order_type=order_type,
        quantity=quantity,
        price=price,
        position_side=position_side,
    )
    try:
        if not execute:
            _emit(socket.plan_order(**kwargs))
            click.echo("plan only; pass --execute with BINANCE_API_KEY and BINANCE_API_SECRET", err=True)
            return
        _run(lambda: socket.place_order(confirm=True, **kwargs))
    finally:
        socket.close()
