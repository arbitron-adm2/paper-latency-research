from __future__ import annotations

import time
from typing import Any

from curl_cffi import requests

from .auth import PublicHeaders, WebSession
from .exceptions import BinanceHTTPError, BinanceNetworkError, BinanceThrottleError

# exchangeInfo on 2026-10-08: REQUEST_WEIGHT 6000 / minute.
# Client default stays well under that and is overridable.
DEFAULT_WEIGHT_PER_MINUTE = 1200


class WeightBucket:
    def __init__(self, per_minute: int = DEFAULT_WEIGHT_PER_MINUTE):
        if per_minute <= 0:
            raise ValueError("per_minute must be positive")
        self.per_minute = per_minute
        self._tokens = float(per_minute)
        self._updated = time.monotonic()

    def take(self, weight: int) -> None:
        if weight <= 0:
            return
        need = float(weight)
        while True:
            now = time.monotonic()
            elapsed = now - self._updated
            self._updated = now
            self._tokens = min(float(self.per_minute), self._tokens + elapsed * (self.per_minute / 60.0))
            if self._tokens >= need:
                self._tokens -= need
                return
            missing = need - self._tokens
            time.sleep(missing * 60.0 / self.per_minute)


class HttpTransport:
    def __init__(
        self,
        session: WebSession | None = None,
        weight_per_minute: int = DEFAULT_WEIGHT_PER_MINUTE,
        timeout: float = 20.0,
        max_attempts: int = 3,
    ):
        self.public = PublicHeaders()
        self.session = session or WebSession(None, None)
        self.bucket = WeightBucket(weight_per_minute)
        self.timeout = timeout
        self.max_attempts = max_attempts
        self._http = requests.Session(impersonate="chrome")

    def request(
        self,
        method: str,
        url: str,
        *,
        weight: int = 1,
        params: dict | None = None,
        json_body: Any = None,
        session: bool = False,
        allow_statuses: tuple[int, ...] = (),
    ) -> Any:
        self.bucket.take(weight)
        headers = self.session.headers() if session else self.public.headers()
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._http.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=self.timeout,
                )
            except Exception as exc:
                if attempt >= self.max_attempts:
                    raise BinanceNetworkError(f"{method} {url} failed: {exc}") from exc
                time.sleep(0.4 * (2 ** (attempt - 1)))
                continue

            status = response.status_code
            if status == 429 or status == 418:
                retry_after = response.headers.get("Retry-After")
                if attempt < self.max_attempts and retry_after and retry_after.isdigit() and int(retry_after) <= 2:
                    time.sleep(int(retry_after))
                    continue
                raise BinanceThrottleError(status, response.text, url, retry_after)
            if status >= 500 and attempt < self.max_attempts:
                time.sleep(0.4 * (2 ** (attempt - 1)))
                continue
            if status >= 400 and status not in allow_statuses:
                raise BinanceHTTPError(status, response.text, url)
            if not response.content:
                return None
            content_type = response.headers.get("content-type", "")
            if "json" in content_type or response.text[:1] in "{[":
                return response.json()
            return response.text
