"""Spot order entry reconstructed from the trade-page bundle.

The website does not submit orders on the market WebSocket. For a plain
LIMIT or MARKET spot ticket (not a conditional/strategy order) it POSTs:

    https://www.binance.com/bapi/mbx/v1/private/mbxgateway/aor/order/place

Body fields taken from the form: symbol, side, type, quantity,
quoteOrderQty, price. A successful payload's ``data.aorId`` is what the
UI displays as orderId.

An anonymous POST returns HTTP 401
``{"code":"100001005","message":"Please log in first.","success":false}``
and does not create an order. A real call needs the caller's own session
cookie and csrftoken. This module never sends unless ``confirm=True``.
"""

from __future__ import annotations

from typing import Any

from .exceptions import NotAuthenticated, PlanOnly
from .http import HttpTransport

AOR_PLACE = "https://www.binance.com/bapi/mbx/v1/private/mbxgateway/aor/order/place"


def aor_order_body(
    symbol: str,
    side: str,
    order_type: str,
    quantity: str | None = None,
    quote_order_qty: str | None = None,
    price: str | None = None,
) -> dict[str, str]:
    side = side.upper()
    order_type = order_type.upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if order_type not in {"LIMIT", "MARKET"}:
        raise ValueError("web AOR place path is only used for LIMIT and MARKET")
    body: dict[str, str] = {
        "symbol": symbol.upper(),
        "side": side,
        "type": order_type,
    }
    if quantity is not None:
        body["quantity"] = str(quantity)
    if quote_order_qty is not None:
        body["quoteOrderQty"] = str(quote_order_qty)
    if price is not None:
        body["price"] = str(price)
    if order_type == "LIMIT" and "price" not in body:
        raise ValueError("LIMIT order requires price")
    if "quantity" not in body and "quoteOrderQty" not in body:
        raise ValueError("quantity or quoteOrderQty is required")
    return body


class Trading:
    def __init__(self, http: HttpTransport):
        self.http = http

    def plan_aor_order(self, **kwargs: Any) -> dict[str, Any]:
        body = aor_order_body(**kwargs)
        return {
            "would": "POST",
            "url": AOR_PLACE,
            "json": body,
            "auth": "web session cookie + csrftoken header; not sent",
            "note": "Plan only. Pass confirm=True to execute with your own session.",
        }

    def place_aor_order(self, *, confirm: bool = False, **kwargs: Any) -> Any:
        plan = self.plan_aor_order(**kwargs)
        if not confirm:
            raise PlanOnly(str(plan))
        if not self.http.session.ready:
            raise NotAuthenticated(
                "Set BINANCE_COOKIE and BINANCE_CSRF_TOKEN. Refusing to POST without them."
            )
        # Weight is unknown (session endpoint, no public weight header).
        # Count it as a heavy call so a script cannot loop it by accident.
        return self.http.request(
            "POST",
            AOR_PLACE,
            json_body=plan["json"],
            weight=10,
            session=True,
        )
