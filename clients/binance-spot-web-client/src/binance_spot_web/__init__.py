from .client import BinanceSpotWeb
from .exceptions import (
    BinanceHTTPError,
    BinanceNetworkError,
    BinanceThrottleError,
    BinanceWebError,
    NotAuthenticated,
    PlanOnly,
)

__all__ = [
    "BinanceSpotWeb",
    "BinanceHTTPError",
    "BinanceNetworkError",
    "BinanceThrottleError",
    "BinanceWebError",
    "NotAuthenticated",
    "PlanOnly",
]
