"""Futures web client, from www.binance.com/en/futures/BTCUSDT.

The page's SDK builds three sockets off one base:

    {base}/public/stream    depth, rpiDepth, markPrice
    {base}/market/stream    aggTrade
    {base}/private          user events, needs a listen key

The configured USD-M template is ``wss://sfstream.{{rootDomain}}`` and
COIN-M is ``wss://sdstream.{{rootDomain}}``. Both hosts fail the websocket
handshake here (HTTP 302 to the site, browser close 1006). The same
SUBSCRIBE protocol works on the public edges the page can actually open:

    wss://fstream.binance.com/public/stream
    wss://fstream.binance.com/market/stream
    wss://dstream.binance.com/public/stream
    wss://dstream.binance.com/market/stream

When cookie ``cr00`` is present the bundle appends ``/swift`` to that
base. ``wss://fstream.binance.com/swift/...`` is 404, and the sfstream
host never completes a handshake, so /swift is not a working method.

Orders are not sent on either socket. A plain USD-M ticket POSTs
``/bapi/futures/v1/private/future/order/place-order``. Anonymous call
returns 401 ``100001005``. This module only builds that body unless
``confirm=True``.
"""

from __future__ import annotations

from typing import Any

from .exceptions import NotAuthenticated, PlanOnly
from .http import HttpTransport
from .ws import MarketStream

FAPI = "https://fapi.binance.com"
DAPI = "https://dapi.binance.com"
UM_PUBLIC_WS = "wss://fstream.binance.com/public/stream"
UM_MARKET_WS = "wss://fstream.binance.com/market/stream"
CM_PUBLIC_WS = "wss://dstream.binance.com/public/stream"
CM_MARKET_WS = "wss://dstream.binance.com/market/stream"

UM_PLACE = "https://www.binance.com/bapi/futures/v1/private/future/order/place-order"
CM_PLACE = "https://www.binance.com/bapi/futures/v1/private/delivery/order/place-order"


def _depth_weight(limit: int) -> int:
    if limit <= 50:
        return 2
    if limit <= 100:
        return 5
    if limit <= 500:
        return 10
    return 20


class Futures:
    def __init__(self, http: HttpTransport):
        self.http = http

    def premium_index(self, symbol: str) -> dict:
        return self.http.request(
            "GET",
            f"{FAPI}/fapi/v1/premiumIndex",
            params={"symbol": symbol.upper()},
            weight=1,
        )

    def depth(self, symbol: str, limit: int = 5) -> dict:
        return self.http.request(
            "GET",
            f"{FAPI}/fapi/v1/depth",
            params={"symbol": symbol.upper(), "limit": limit},
            weight=_depth_weight(limit),
        )

    def rpi_depth(self, symbol: str, limit: int = 1000) -> dict:
        """Order-book snapshot the futures page requests.

        ``/fapi/v1/rpiDepth`` rejects every limit except 1000
        (HTTP 400 ``-4021``). The page asks for 1000.
        """
        if limit != 1000:
            raise ValueError("rpiDepth only accepts limit=1000")
        return self.http.request(
            "GET",
            f"{FAPI}/fapi/v1/rpiDepth",
            params={"symbol": symbol.upper(), "limit": limit},
            weight=20,
        )

    def agg_trades(self, symbol: str, limit: int = 2) -> list:
        return self.http.request(
            "GET",
            f"{FAPI}/fapi/v1/aggTrades",
            params={"symbol": symbol.upper(), "limit": limit},
            weight=20,
        )

    def exchange_info(self, symbol: str | None = None) -> dict:
        params = {"symbol": symbol.upper()} if symbol else None
        return self.http.request("GET", f"{FAPI}/fapi/v1/exchangeInfo", params=params, weight=1)

    def cm_premium_index(self, symbol: str = "BTCUSD_PERP") -> list | dict:
        return self.http.request(
            "GET",
            f"{DAPI}/dapi/v1/premiumIndex",
            params={"symbol": symbol.upper()},
            weight=1,
        )

    def stream(self, streams: list[str], *, product: str = "um", channel: str = "public") -> MarketStream:
        product = product.lower()
        channel = channel.lower()
        if product not in {"um", "cm"}:
            raise ValueError("product must be um or cm")
        if channel not in {"public", "market"}:
            raise ValueError("channel must be public or market")
        urls = {
            ("um", "public"): UM_PUBLIC_WS,
            ("um", "market"): UM_MARKET_WS,
            ("cm", "public"): CM_PUBLIC_WS,
            ("cm", "market"): CM_MARKET_WS,
        }
        socket = MarketStream(urls[(product, channel)])
        socket.connect()
        socket.subscribe(streams)
        return socket

    def plan_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        price: str | None = None,
        position_side: str = "BOTH",
        time_in_force: str = "GTC",
        reduce_only: bool | None = None,
        product: str = "um",
    ) -> dict[str, Any]:
        side = side.upper()
        order_type = order_type.upper()
        position_side = position_side.upper()
        product = product.lower()
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("only LIMIT and MARKET are built; conditional types are a different endpoint")
        if position_side not in {"BOTH", "LONG", "SHORT"}:
            raise ValueError("position_side must be BOTH, LONG, or SHORT")
        if product not in {"um", "cm"}:
            raise ValueError("product must be um or cm")
        if order_type == "LIMIT" and not price:
            raise ValueError("LIMIT requires price")
        body: dict[str, Any] = {
            "symbol": symbol.upper(),
            "type": order_type,
            "side": side,
            "positionSide": position_side,
            "quantity": quantity,
        }
        if position_side == "BOTH" and reduce_only is not None:
            body["reduceOnly"] = reduce_only
        if order_type == "LIMIT":
            body["price"] = price
            body["timeInForce"] = time_in_force.upper()
        url = UM_PLACE if product == "um" else CM_PLACE
        return {
            "would": "POST",
            "url": url,
            "json": body,
            "auth": "web session cookie + csrftoken; not sent",
            "note": "Plan only. The futures page sends quantity as a JS number. Pass confirm=True to POST.",
        }

    def place_order(self, *, confirm: bool = False, **kwargs: Any) -> Any:
        plan = self.plan_order(**kwargs)
        if not confirm:
            raise PlanOnly(str(plan))
        if not self.http.session.ready:
            raise NotAuthenticated(
                "Set BINANCE_COOKIE and BINANCE_CSRF_TOKEN. Refusing to POST without them."
            )
        return self.http.request("POST", plan["url"], json_body=plan["json"], weight=10, session=True)
