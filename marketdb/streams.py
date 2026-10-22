"""Price/trade research subscriptions. No order book depth or downsampling."""
import os
DEPTH_ENABLED = os.environ.get('PAPER_CAPTURE_DEPTH') == '1'
COINS = ('BTC', 'ETH')
PAIRS = ('btcusdt', 'ethusdt')
HL_URL = 'wss://api.hyperliquid.xyz/ws'
HL_SUBSCRIPTIONS = [{'method': 'subscribe', 'subscription': {'type': kind, 'coin': coin}}
                    for coin in COINS for kind in ['bbo', 'trades']]
if DEPTH_ENABLED:
    HL_SUBSCRIPTIONS += [{'method':'subscribe','subscription':{'type':'l2Book','coin':coin}} for coin in COINS]
HL_FAST_SUBSCRIPTIONS = [{'method':'subscribe','subscription':{'type':'l2Book','coin':coin,'fast':True}} for coin in COINS] if DEPTH_ENABLED else []
HL_AGG_SUBSCRIPTIONS = [{'method':'subscribe','subscription':{'type':'l2Book','coin':coin,'nSigFigs':4}} for coin in COINS] if DEPTH_ENABLED else []
FUTURES_QUOTES_URL = 'wss://fstream.binance.com/public/stream?streams=' + '/'.join(p+'@bookTicker' for p in PAIRS)
FUTURES_TRADES_URL = 'wss://fstream.binance.com/market/stream?streams=' + '/'.join(p+'@aggTrade' for p in PAIRS)
SPOT_URL = 'wss://stream.binance.com/stream?streams=' + '/'.join(
    p+'@'+kind for p in PAIRS for kind in ['bookTicker', 'aggTrade'])
if DEPTH_ENABLED:
    SPOT_URL += '/' + '/'.join(p+'@depth20@100ms' for p in PAIRS)
FUTURES_DEPTH_URL = 'wss://fstream.binance.com/public/stream?streams=' + '/'.join(p+'@depth20@100ms' for p in PAIRS) if DEPTH_ENABLED else None
PAPER_URL = 'wss://stream.papertrade.xyz/connection/websocket'
PAPER_SUBSCRIPTIONS = [{'id': 1, 'connect': {'name': 'js'}}] + [
    {'id': i, 'subscribe': {'channel': channel}} for i, channel in
    [(2, 'pressure:current'), (3, 'public:trades'), (4, 'trading:current')]]


def profile():
    return {'name': 'backtest_depth20' if DEPTH_ENABLED else 'prices_and_trades', 'orderbook_depth': DEPTH_ENABLED, 'downsampling': False,
            'hl': {'url': HL_URL, 'subscriptions': HL_SUBSCRIPTIONS},
            'hl_fast': HL_FAST_SUBSCRIPTIONS, 'hl_aggregated': HL_AGG_SUBSCRIPTIONS, 'depth_levels': 20 if DEPTH_ENABLED else 0,
            'binance_futures_quotes': FUTURES_QUOTES_URL, 'binance_futures_depth': FUTURES_DEPTH_URL,
            'binance_futures_trades': FUTURES_TRADES_URL,
            'binance_spot': SPOT_URL, 'paper_channels': [x['subscribe']['channel'] for x in PAPER_SUBSCRIPTIONS[1:]]}
