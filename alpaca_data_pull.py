from dotenv import load_dotenv
import os
from alpaca.data.live.stock import StockDataStream
from alpaca.data.live.crypto import CryptoDataStream

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")

# Crypto for testing purposes only since stock market is closed on weekends
class Mode:
    CRYPTO = 'crypto'
    STOCK = 'stock'

class Symbols:
    CRYPTO = ['BTC/USD', 'ETH/USD']
    STOCK = ['AMD','TSLA', 'NVDA', 'AMZN']

# based on https://medium.com/@trademamba/alpaca-algorithmic-trading-api-in-python-part-3-real-time-websocket-data-14bae3927929
def run_stream(mode, symbols):
    """
    Runs Data stream based on mode
    args:
        mode: 'crypto' | 'stock'
        symbols: crypto | stock symbols
    """
    async def handle_quote(data):
        print("New Quote")
        print(data)

    async def handle_trade(data):
        print('New Trade')
        print(data)

    async def handle_bar(data):
        print('New Bar')
        print(data)

    if mode == Mode.STOCK:
        stream = StockDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)
    elif mode == Mode.CRYPTO:
        stream = CryptoDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    stream.subscribe_quotes(handle_quote, *symbols)
    stream.subscribe_trades(handle_trade, *symbols)
    stream.subscribe_bars(handle_bar, *symbols)
    stream.run()

run_stream(Mode.CRYPTO, Symbols.CRYPTO)

#blabla