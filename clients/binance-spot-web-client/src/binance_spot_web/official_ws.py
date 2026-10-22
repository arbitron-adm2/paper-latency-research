"""Official trading websockets. Not the website, and not the VIP/SBE feed.

The spot and futures pages do not place orders on a websocket. The sockets
that do are the documented WebSocket API, open to any API key:

    wss://ws-api.binance.com/ws-api/v3       spot  method order.place
    wss://ws-fapi.binance.com/ws-fapi/v1     USD-M method order.place

An unsigned ``order.place`` does not create an order. Measured 2026-10-08:

    spot    status 400  code -1102  Mandatory parameter 'apiKey' was not sent
    USD-M   status 400  code -1103  param apikey not found

A real order needs the caller's own HMAC key in ``BINANCE_API_KEY`` and
``BINANCE_API_SECRET``. Those are never read from the web session and never
written into the repo. ``place`` sends nothing unless ``confirm=True``.
Ed25519 and RSA keys are documented by Binance and are not implemented here.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any

from curl_cffi import requests
from curl_cffi.const import CurlWsFlag

from .exceptions import BinanceNetworkError, NotAuthenticated, PlanOnly

SPOT_TRADE_WS = "wss://ws-api.binance.com/ws-api/v3"
UM_TRADE_WS = "wss://ws-fapi.binance.com/ws-fapi/v1"

URLS = {"spot": SPOT_TRADE_WS, "um": UM_TRADE_WS}


def hmac_sha256(secret: str, params: dict[str, Any]) -> str:
    """Sign params in iteration order, excluding ``signature`` itself."""
    payload = "&".join(f"{key}={params[key]}" for key in params if key != "signature")
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


class OfficialTradeSocket:
    def __init__(
        self,
        product: str = "spot",
        api_key: str | None = None,
        api_secret: str | None = None,
    ):
        product = product.lower()
        if product not in URLS:
            raise ValueError("product must be spot or um")
        self.product = product
        self.url = URLS[product]
        self.api_key = api_key if api_key is not None else os.environ.get("BINANCE_API_KEY", "")
        self.api_secret = api_secret if api_secret is not None else os.environ.get("BINANCE_API_SECRET", "")
        self._session = requests.Session(impersonate="chrome")
        self._ws = None
        self._next_id = 1

    def connect(self) -> None:
        try:
            self._ws = self._session.ws_connect(
                self.url,
                headers={"Origin": "https://www.binance.com"},
                timeout=20,
            )
        except Exception as exc:
            raise BinanceNetworkError(f"websocket connect {self.url} failed: {exc}") from exc

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict:
        if self._ws is None:
            self.connect()
        request_id = self._next_id
        self._next_id += 1
        payload: dict[str, Any] = {"id": request_id, "method": method}
        if params:
            payload["params"] = params
        try:
            self._ws.send(json.dumps(payload, separators=(",", ":")), flags=CurlWsFlag.TEXT)
            data, _flags = self._ws.recv()
        except Exception as exc:
            raise BinanceNetworkError(f"{method} on {self.url} failed: {exc}") from exc
        text = data.decode() if isinstance(data, bytes) else str(data)
        try:
            body = json.loads(text)
        except json.JSONDecodeError as exc:
            raise BinanceNetworkError(f"non-json frame: {text[:160]}") from exc
        return body

    def ping(self) -> dict:
        return self.request("ping")

    def server_time(self) -> int:
        body = self.request("time")
        if body.get("status") != 200:
            raise BinanceNetworkError(f"time failed: {body}")
        return int(body["result"]["serverTime"])

    def order_params(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        price: str | None = None,
        time_in_force: str = "GTC",
        position_side: str | None = None,
    ) -> dict[str, Any]:
        side = side.upper()
        order_type = order_type.upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("type must be LIMIT or MARKET")
        if order_type == "LIMIT" and not price:
            raise ValueError("LIMIT requires price")
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side,
            "type": order_type,
            "quantity": quantity,
        }
        if order_type == "LIMIT":
            params["timeInForce"] = time_in_force.upper()
            params["price"] = price
        if self.product == "um" and position_side:
            params["positionSide"] = position_side.upper()
        return params

    def plan_order(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "would": "websocket",
            "url": self.url,
            "method": "order.place",
            "params": self.order_params(**kwargs),
            "auth": "BINANCE_API_KEY + HMAC-SHA256 BINANCE_API_SECRET; not sent",
            "note": "Plan only. Pass confirm=True to sign and send.",
        }

    def place_order(self, *, confirm: bool = False, **kwargs: Any) -> Any:
        plan = self.plan_order(**kwargs)
        if not confirm:
            raise PlanOnly(str(plan))
        if not self.api_key or not self.api_secret:
            raise NotAuthenticated(
                "Set BINANCE_API_KEY and BINANCE_API_SECRET. Refusing to send order.place."
            )
        timestamp = self.server_time()
        params = dict(plan["params"])
        params["apiKey"] = self.api_key
        params["timestamp"] = timestamp
        params["signature"] = hmac_sha256(self.api_secret, params)
        return self.request("order.place", params)

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            finally:
                self._ws = None
