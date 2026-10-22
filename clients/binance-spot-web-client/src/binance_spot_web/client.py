from __future__ import annotations

from .auth import WebSession
from .http import DEFAULT_WEIGHT_PER_MINUTE, HttpTransport
from .market import Market
from .futures import Futures
from .trading import Trading
from .ws import MarketStream


class BinanceSpotWeb:
    def __init__(
        self,
        cookie: str | None = None,
        csrf_token: str | None = None,
        weight_per_minute: int = DEFAULT_WEIGHT_PER_MINUTE,
    ):
        self.http = HttpTransport(
            session=WebSession(cookie, csrf_token),
            weight_per_minute=weight_per_minute,
        )
        self.market = Market(self.http)
        self.trading = Trading(self.http)
        self.futures = Futures(self.http)

    def stream(self, streams: list[str]) -> MarketStream:
        socket = MarketStream()
        socket.connect()
        socket.subscribe(streams)
        return socket
