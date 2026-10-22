from __future__ import annotations

from decimal import Decimal
import logging
import time
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

DEFAULT_EXCHANGE_BASE = "https://exchange.papertrade.xyz"
MAX_RETRIES = 3
BACKOFF_S = 2.0


class PositionMonitor:
    """PaperTrade position monitoring helper."""

    def __init__(self, base_url: str = DEFAULT_EXCHANGE_BASE) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=15.0,
            headers={"User-Agent": "PaperTradeBot/1.0"},
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> PositionMonitor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _request_with_retry(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
    ) -> httpx.Response:
        """Execute HTTP request with up to 3 retries and 2s backoff on failures."""
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._client.request(method, path, params=params, json=json)
                resp.raise_for_status()
                return resp
            except (httpx.HTTPError, httpx.RequestError) as exc:
                last_exc = exc
                logger.warning(
                    "HTTP %s %s failed (attempt %d/%d): %s",
                    method,
                    path,
                    attempt,
                    MAX_RETRIES,
                    exc,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(BACKOFF_S)

        raise RuntimeError(
            f"HTTP request {method} {path} failed after {MAX_RETRIES} retries: {last_exc}"
        ) from last_exc

    def get_account_trades(self, address: str, since_ms: int = 0) -> list[dict[str, Any]]:
        """
        GET /query/accounts/{address}/trades, filter by timestampMs > since_ms,
        return items sorted by timestampMs desc.
        """
        try:
            addr = address.lower()
            resp = self._request_with_retry("GET", f"/query/accounts/{addr}/trades")
            data = resp.json()
            items = data.get("items", []) if isinstance(data, dict) else []

            # Filter items with timestampMs > since_ms
            filtered: list[dict[str, Any]] = []
            for item in items:
                ts = item.get("timestampMs")
                if ts is not None and ts > since_ms:
                    filtered.append(item)

            # Sort descending by timestampMs
            filtered.sort(key=lambda x: x.get("timestampMs", 0), reverse=True)
            return filtered
        except Exception as exc:
            logger.error("Failed to fetch account trades for %s: %s", address, exc)
            raise RuntimeError(f"Failed to fetch account trades for {address}: {exc}") from exc

    def detect_bust(self, address: str, opened_at_ms: int) -> Optional[dict[str, Any]]:
        """
        Return the Liquidated trade event if found after opened_at_ms, else None.
        Matches if item['kind'] == 'Liquidated' and timestampMs > opened_at_ms
        (or matching openedAtMs).
        """
        try:
            trades = self.get_account_trades(address, since_ms=opened_at_ms)
            for trade in trades:
                kind = trade.get("kind")
                if kind == "Liquidated":
                    trade_opened_at = trade.get("openedAtMs")
                    # If trade specifies openedAtMs, match it or match timestampMs > opened_at_ms
                    if trade_opened_at is None or trade_opened_at == opened_at_ms or trade.get("timestampMs", 0) > opened_at_ms:
                        return trade
            return None
        except Exception as exc:
            logger.error("Error detecting bust for %s: %s", address, exc)
            return None

    def detect_close(self, address: str, opened_at_ms: int) -> Optional[dict[str, Any]]:
        """
        Return PositionClosed event if found after opened_at_ms, else None.
        """
        try:
            trades = self.get_account_trades(address, since_ms=opened_at_ms)
            for trade in trades:
                kind = trade.get("kind")
                if kind == "PositionClosed":
                    trade_opened_at = trade.get("openedAtMs")
                    if trade_opened_at is None or trade_opened_at == opened_at_ms or trade.get("timestampMs", 0) > opened_at_ms:
                        return trade
            return None
        except Exception as exc:
            logger.error("Error detecting close for %s: %s", address, exc)
            return None

    def get_latest_paper_earned(self, address: str, since_ms: int) -> Decimal:
        """
        Sum of paperMintedRaw / 1e18 for all events since since_ms.
        """
        try:
            trades = self.get_account_trades(address, since_ms=since_ms)
            total = Decimal("0")
            for trade in trades:
                minted_raw = trade.get("paperMintedRaw")
                if minted_raw is not None:
                    try:
                        total += Decimal(str(minted_raw)) / Decimal(10**18)
                    except Exception:
                        pass
            return total
        except Exception as exc:
            logger.error("Error calculating paper earned for %s: %s", address, exc)
            raise RuntimeError(f"Error calculating paper earned for {address}: {exc}") from exc

    def get_account_state(self, address: str, opened_at_ms: int) -> str:
        """
        Return 'open' | 'busted' | 'closed' | 'unknown'.
        """
        try:
            bust = self.detect_bust(address, opened_at_ms)
            if bust is not None:
                return "busted"

            close = self.detect_close(address, opened_at_ms)
            if close is not None:
                return "closed"

            return "open"
        except Exception as exc:
            logger.error("Error determining account state for %s: %s", address, exc)
            return "unknown"

    def wait_for_open_confirmation(
        self,
        address: str,
        after_ms: int,
        timeout_s: int = 120,
    ) -> bool:
        """
        Poll until a position open is confirmed or timeout.
        Confirmed by seeing if no close events exist after after_ms.
        """
        deadline = time.time() + timeout_s
        poll_interval = 2.0

        while time.time() < deadline:
            try:
                # Check trades since after_ms
                close = self.detect_close(address, after_ms)
                bust = self.detect_bust(address, after_ms)

                # If it already closed or busted, it's definitely confirmed (and finished)
                if close is not None or bust is not None:
                    return True

                # Check if position is open (no close events exist after after_ms)
                state = self.get_account_state(address, after_ms)
                if state == "open":
                    return True
            except Exception as exc:
                logger.debug("Polling open confirmation exception: %s", exc)

            time.sleep(poll_interval)

        return False
