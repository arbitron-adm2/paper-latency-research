"""Market-data WebSocket used by www.binance.com/en/trade.

Live capture on 2026-10-08: after an offline/online cycle the spot page
opened ``wss://stream.binance.com/stream`` and sent text frames
``{"method":"GET_PROPERTY","params":["combined"],"id":N}`` then the
standard SUBSCRIBE protocol.

Proven streams (text frame in, JSON event out):
  btcusdt@trade, btcusdt@depth@100ms, btcusdt@kline_1m, btcusdt@aorAggTrade

Referenced by the page bundle but silent for 3.5s on this public host:
  @aorDepth100, @aorMiniTicker, @aggSnap

This is not SBE and not the VIP market-data product. Frames are JSON.
"""

from __future__ import annotations

import json
from typing import Iterator

from curl_cffi import requests
from curl_cffi.const import CurlWsFlag

from .exceptions import BinanceNetworkError

MARKET_WS = "wss://stream.binance.com/stream"
# Official WebSocket API. The spot page uses it only for
# userDataStream.subscribe.listenToken when a desktop session exists.
# order.place on this socket is the documented API-key API, not the
# path the website uses to submit orders.
USER_WS_API = "wss://ws-api.binance.com/ws-api/v3"

PROVEN_PUBLIC_STREAMS = (
    "{symbol}@trade",
    "{symbol}@aggTrade",
    "{symbol}@depth@100ms",
    "{symbol}@kline_{interval}",
    "{symbol}@aorAggTrade",
)


class MarketStream:
    def __init__(self, url: str = MARKET_WS, timeout: float = 20.0):
        self.url = url
        self.timeout = timeout
        self._session = requests.Session(impersonate="chrome")
        self._ws = None
        self._next_id = 1

    def connect(self) -> None:
        try:
            self._ws = self._session.ws_connect(
                self.url,
                headers={
                    "Origin": "https://www.binance.com",
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                    ),
                },
                timeout=self.timeout,
            )
        except Exception as exc:
            raise BinanceNetworkError(f"websocket connect {self.url} failed: {exc}") from exc

    def send_json(self, payload: dict) -> None:
        if self._ws is None:
            self.connect()
        # curl_cffi defaults to a binary frame. Binance closes the socket
        # with "Invalid request" / "binary frames require preliminary negotiation".
        self._ws.send(json.dumps(payload, separators=(",", ":")), flags=CurlWsFlag.TEXT)

    def subscribe(self, streams: list[str]) -> int:
        request_id = self._next_id
        self._next_id += 1
        self.send_json({"method": "SUBSCRIBE", "params": streams, "id": request_id})
        return request_id

    def recv_json(self) -> dict:
        if self._ws is None:
            raise BinanceNetworkError("websocket is not connected")
        try:
            data, _flags = self._ws.recv()
        except Exception as exc:
            raise BinanceNetworkError(f"websocket recv failed: {exc}") from exc
        if isinstance(data, bytes):
            try:
                text = data.decode()
            except UnicodeDecodeError as exc:
                raise BinanceNetworkError(f"non-text frame ({len(data)} bytes)") from exc
        else:
            text = str(data)
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise BinanceNetworkError(f"non-json frame: {text[:120]}") from exc

    def events(self, limit: int | None = None) -> Iterator[dict]:
        seen = 0
        while limit is None or seen < limit:
            message = self.recv_json()
            if "result" in message and "id" in message and "stream" not in message:
                continue
            seen += 1
            yield message

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            finally:
                self._ws = None
