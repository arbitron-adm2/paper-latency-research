from __future__ import annotations

from typing import Any

from .http import HttpTransport

REST = "https://api.binance.com"

# Weights measured 2026-10-08 from x-mbx-used-weight-1m deltas on api.binance.com.
# depth limit=5 -> +5. aggTrades -> +4. aor/aggTrades -> +4.
# ticker/price single symbol -> +4. klines limit=2 -> +2. exchangeInfo?symbol -> +20.


def depth_weight(limit: int) -> int:
    if limit <= 100:
        return 5
    if limit <= 500:
        return 25
    if limit <= 1000:
        return 50
    return 250


class Market:
    def __init__(self, http: HttpTransport):
        self.http = http

    def ping(self) -> dict:
        return self.http.request("GET", f"{REST}/api/v3/ping", weight=1)

    def time(self) -> dict:
        return self.http.request("GET", f"{REST}/api/v3/time", weight=1)

    def exchange_info(self, symbol: str | None = None) -> dict:
        params = {"symbol": symbol.upper()} if symbol else None
        # Full exchangeInfo is heavier. The symbol-scoped call measured +20.
        weight = 20 if symbol else 40
        return self.http.request("GET", f"{REST}/api/v3/exchangeInfo", params=params, weight=weight)

    def depth(self, symbol: str, limit: int = 100) -> dict:
        return self.http.request(
            "GET",
            f"{REST}/api/v3/depth",
            params={"symbol": symbol.upper(), "limit": limit},
            weight=depth_weight(limit),
        )

    def agg_trades(self, symbol: str, limit: int = 50) -> list:
        return self.http.request(
            "GET",
            f"{REST}/api/v3/aggTrades",
            params={"symbol": symbol.upper(), "limit": limit},
            weight=4,
        )

    def aor_agg_trades(self, symbol: str, limit: int = 50) -> list:
        """Undocumented REST twin of the page's @aorAggTrade stream.

        Same host and weight class as /api/v3/aggTrades. Payload drops the
        first/last trade id fields (``f``/``l``) and ``M``.
        """
        return self.http.request(
            "GET",
            f"{REST}/api/v3/aor/aggTrades",
            params={"symbol": symbol.upper(), "limit": limit},
            weight=4,
        )

    def klines(self, symbol: str, interval: str = "1m", limit: int = 10) -> list:
        return self.http.request(
            "GET",
            f"{REST}/api/v3/klines",
            params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
            weight=2,
        )

    def price(self, symbol: str) -> dict:
        return self.http.request(
            "GET",
            f"{REST}/api/v3/ticker/price",
            params={"symbol": symbol.upper()},
            weight=4,
        )

    def book_ticker(self, symbol: str) -> dict[str, Any]:
        return self.http.request(
            "GET",
            f"{REST}/api/v3/ticker/bookTicker",
            params={"symbol": symbol.upper()},
            weight=2,
        )
