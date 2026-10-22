class BinanceWebError(Exception):
    """Base error for this client. Callers should catch this, not None."""


class BinanceHTTPError(BinanceWebError):
    def __init__(self, status_code: int, body: str, url: str):
        self.status_code = status_code
        self.body = body
        self.url = url
        snippet = body.replace("\n", " ")[:300]
        super().__init__(f"HTTP {status_code} for {url}: {snippet}")


class BinanceThrottleError(BinanceHTTPError):
    def __init__(self, status_code: int, body: str, url: str, retry_after: str | None):
        self.retry_after = retry_after
        super().__init__(status_code, body, url)


class BinanceNetworkError(BinanceWebError):
    pass


class NotAuthenticated(BinanceWebError):
    """Web order entry needs the user's own logged-in session. None was sent."""


class PlanOnly(BinanceWebError):
    """Mutating call refused because confirm/execute was not set."""
