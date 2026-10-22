"""Print a handful of public BTCUSDT events from the same socket the website uses."""

from binance_spot_web import BinanceSpotWeb

def main() -> None:
    client = BinanceSpotWeb()
    socket = client.stream(["btcusdt@trade", "btcusdt@aorAggTrade"])
    try:
        for event in socket.events(limit=5):
            data = event.get("data", {})
            print(event.get("stream"), data.get("e"), data.get("p"), data.get("q"))
    finally:
        socket.close()

if __name__ == "__main__":
    main()
