"""Auth tiers observed on the binance.com spot trade page.

Tier 0 — public market REST on api.binance.com and the public market
WebSocket on stream.binance.com. A stdlib HTTP client already gets HTTP 200;
no signature, no API key.

Tier 7 — web order entry. POST
/bapi/mbx/v1/private/mbxgateway/aor/order/place returns
{"code":"100001005","message":"Please log in first."} with no session.
The page sends header ``csrftoken`` derived from cookie ``cr00`` by a
bundled hasher, plus ``clienttype: web`` and the session cookie. This
client does not reimplement that hasher and never stores a captured
cookie. Pass your own cookie and csrftoken at runtime.
"""

from __future__ import annotations

from typing import Mapping


class PublicHeaders:
    """Tier 0. Browser-like headers only. No secret."""

    def headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Origin": "https://www.binance.com",
            "Referer": "https://www.binance.com/en/trade/BTC_USDT",
        }


class WebSession:
    """Tier 7. User-supplied cookie and csrftoken. Values are not logged."""

    def __init__(self, cookie: str | None, csrf_token: str | None):
        self._cookie = (cookie or "").strip()
        self._csrf = (csrf_token or "").strip()

    @property
    def ready(self) -> bool:
        return bool(self._cookie and self._csrf)

    def headers(self) -> dict[str, str]:
        if not self.ready:
            raise RuntimeError("web session is not configured")
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.binance.com",
            "Referer": "https://www.binance.com/en/trade/BTC_USDT",
            "clienttype": "web",
            "csrftoken": self._csrf,
            "Cookie": self._cookie,
        }

    def redacted(self) -> Mapping[str, str]:
        return {
            "clienttype": "web",
            "csrftoken": "<redacted>" if self._csrf else "",
            "Cookie": "<redacted>" if self._cookie else "",
        }
