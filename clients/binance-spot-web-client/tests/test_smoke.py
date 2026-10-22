import os
import unittest

from binance_spot_web import BinanceSpotWeb, PlanOnly
from binance_spot_web.official_ws import OfficialTradeSocket, hmac_sha256
from binance_spot_web.ws import MARKET_WS


class LivePublicTests(unittest.TestCase):
    def setUp(self):
        self.client = BinanceSpotWeb(weight_per_minute=600)

    def test_price(self):
        body = self.client.market.price("BTCUSDT")
        self.assertEqual(body["symbol"], "BTCUSDT")
        self.assertIn("price", body)

    def test_depth(self):
        body = self.client.market.depth("BTCUSDT", limit=5)
        self.assertIn("lastUpdateId", body)
        self.assertGreaterEqual(len(body["bids"]), 1)
        self.assertGreaterEqual(len(body["asks"]), 1)

    def test_agg_and_aor_agg(self):
        regular = self.client.market.agg_trades("BTCUSDT", limit=2)
        aor = self.client.market.aor_agg_trades("BTCUSDT", limit=2)
        self.assertGreaterEqual(len(regular), 1)
        self.assertGreaterEqual(len(aor), 1)
        self.assertIn("p", aor[0])
        self.assertNotIn("f", aor[0])

    def test_klines(self):
        rows = self.client.market.klines("BTCUSDT", "1m", 2)
        self.assertGreaterEqual(len(rows), 1)
        self.assertGreaterEqual(len(rows[0]), 6)

    def test_exchange_info_weight_limit(self):
        info = self.client.market.exchange_info("BTCUSDT")
        limits = {row["rateLimitType"]: row for row in info["rateLimits"]}
        self.assertGreaterEqual(limits["REQUEST_WEIGHT"]["limit"], 1000)

    def test_trade_stream_and_aor_agg_trade(self):
        socket = self.client.stream(["btcusdt@trade", "btcusdt@aorAggTrade"])
        self.assertEqual(socket.url, MARKET_WS)
        try:
            kinds = set()
            for _ in range(8):
                event = socket.recv_json()
                stream = event.get("stream")
                if stream:
                    kinds.add(stream)
                if kinds >= {"btcusdt@trade", "btcusdt@aorAggTrade"}:
                    break
            self.assertIn("btcusdt@trade", kinds)
            self.assertIn("btcusdt@aorAggTrade", kinds)
        finally:
            socket.close()

    def test_order_is_plan_only(self):
        plan = self.client.trading.plan_aor_order(
            symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity="0.001", price="1"
        )
        self.assertEqual(plan["would"], "POST")
        self.assertTrue(plan["url"].endswith("/aor/order/place"))
        self.assertEqual(plan["json"]["price"], "1")
        with self.assertRaises(PlanOnly):
            self.client.trading.place_aor_order(
                symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity="0.001", price="1"
            )
        self.assertFalse(self.client.http.session.ready)
        self.assertIsNone(os.environ.get("BINANCE_COOKIE_SHOULD_NOT_BE_SET_BY_TESTS"))


class LiveFuturesTests(unittest.TestCase):
    def setUp(self):
        self.client = BinanceSpotWeb(weight_per_minute=600)

    def test_um_mark_and_depth(self):
        mark = self.client.futures.premium_index("BTCUSDT")
        self.assertEqual(mark["symbol"], "BTCUSDT")
        self.assertIn("markPrice", mark)
        book = self.client.futures.depth("BTCUSDT", limit=5)
        self.assertGreaterEqual(len(book["bids"]), 1)
        rpi = self.client.futures.rpi_depth("BTCUSDT")
        self.assertGreaterEqual(len(rpi["bids"]), 1)

    def test_um_public_depth_and_market_trade(self):
        public = self.client.futures.stream(["btcusdt@depth@500ms"], product="um", channel="public")
        market = self.client.futures.stream(["btcusdt@aggTrade"], product="um", channel="market")
        try:
            self.assertEqual(public.url, "wss://fstream.binance.com/public/stream")
            self.assertEqual(market.url, "wss://fstream.binance.com/market/stream")
            pub_names = set()
            mkt_names = set()
            for _ in range(6):
                event = public.recv_json()
                if event.get("stream"):
                    pub_names.add(event["stream"])
                    break
            for _ in range(6):
                event = market.recv_json()
                if event.get("stream"):
                    mkt_names.add(event["stream"])
                    break
            self.assertIn("btcusdt@depth@500ms", pub_names)
            self.assertIn("btcusdt@aggTrade", mkt_names)
        finally:
            public.close()
            market.close()

    def test_cm_market_trade(self):
        socket = self.client.futures.stream(["btcusd_perp@aggTrade"], product="cm", channel="market")
        try:
            names = set()
            for _ in range(6):
                event = socket.recv_json()
                if event.get("stream"):
                    names.add(event["stream"])
                    break
            self.assertIn("btcusd_perp@aggTrade", names)
        finally:
            socket.close()

    def test_um_order_is_plan_only(self):
        plan = self.client.futures.plan_order(
            symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity="0.001", price="1"
        )
        self.assertTrue(plan["url"].endswith("/future/order/place-order"))
        self.assertEqual(plan["json"]["positionSide"], "BOTH")
        self.assertEqual(plan["json"]["timeInForce"], "GTC")
        with self.assertRaises(PlanOnly):
            self.client.futures.place_order(
                symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity="0.001", price="1"
            )


class OfficialTradeSocketTests(unittest.TestCase):
    def test_hmac_is_stable_and_not_sent_by_plan(self):
        signature = hmac_sha256("secret", {"symbol": "BTCUSDT", "timestamp": 1})
        self.assertEqual(signature, hmac_sha256("secret", {"symbol": "BTCUSDT", "timestamp": 1}))
        self.assertEqual(len(signature), 64)
        socket = OfficialTradeSocket(product="spot", api_key="", api_secret="")
        plan = socket.plan_order(
            symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity="0.001", price="1"
        )
        self.assertEqual(plan["url"], "wss://ws-api.binance.com/ws-api/v3")
        self.assertEqual(plan["method"], "order.place")
        self.assertNotIn("signature", plan["params"])
        with self.assertRaises(PlanOnly):
            socket.place_order(
                symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity="0.001", price="1"
            )
        self.assertIsNone(socket._ws)

    def test_unsigned_order_place_is_rejected(self):
        expected = {"spot": -1102, "um": -1103}
        for product, code in expected.items():
            socket = OfficialTradeSocket(product=product, api_key="", api_secret="")
            try:
                ping = socket.ping()
                self.assertEqual(ping.get("status"), 200, product)
                rejected = socket.request(
                    "order.place",
                    {
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "type": "LIMIT",
                        "timeInForce": "GTC",
                        "price": "1",
                        "quantity": "0.001",
                        "timestamp": 0,
                    },
                )
                self.assertEqual(rejected.get("status"), 400, product)
                self.assertEqual(rejected["error"]["code"], code, product)
                self.assertNotIn("orderId", rejected.get("result") or {})
            finally:
                socket.close()

if __name__ == "__main__":
    unittest.main()
