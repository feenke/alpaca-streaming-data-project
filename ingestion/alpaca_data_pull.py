import os
from alpaca.data.live.stock import StockDataStream
from alpaca.data.live.crypto import CryptoDataStream
from confluent_kafka import Producer
from datetime import datetime
import json
import time

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

# Crypto for testing purposes only since stock market is closed on weekends
class Mode:
    CRYPTO = 'crypto'
    STOCK = 'stock'

class Symbols:
    CRYPTO = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'DOGE/USD', 'XRP/USD', 'AVAX/USD', 'LINK/USD', 'MATIC/USD', 'ADA/USD', 'DOT/USD']
    STOCK = ['AMD','TSLA', 'NVDA', 'AMZN']


def create_producer_with_retry(max_retries=10, retry_delay=5):
    """Retries for connecting with Kafka"""
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'alpaca-ingestion'
    }
    for attempt in range(max_retries):
        try:
            producer = Producer(producer_config)
            producer.list_topics(timeout=5)
            print(f"Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Kafka not available (Try {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"Wating for {retry_delay} seconds")
                time.sleep(retry_delay)
    
    raise Exception("Can't reach Kafka :(")

def delivery_callback(err, msg):
    """Checking if Kafka received messages"""
    if err:
        print(f"Error: {err}")
    else:
        print(f"Message send successfully to {msg.topic()} [{msg.partition()}]")

def serialize_message(data, msg_type):
    """Alpaca data to JSON"""
    message = {
        'type': msg_type,
        'symbol': data.symbol,
        'timestamp': data.timestamp.isoformat(),
        'ingestion_time': datetime.utcnow().isoformat()
    }
    message.update({
            'price': float(data.price),
            'size': float(data.size),
            'exchange': data.exchange
    })

# based on https://medium.com/@trademamba/alpaca-algorithmic-trading-api-in-python-part-3-real-time-websocket-data-14bae3927929
def run_stream(mode, symbols):
    """
    Runs Data stream based on mode and writes to Kafka
    args:
        mode: 'crypto' | 'stock'
        symbols: crypto | stock symbols
    """
    producer = create_producer_with_retry()

    async def handle_trade(data):
        message = {
            'symbol': data.symbol,
            'timestamp': data.timestamp.isoformat(),
            'price': float(data.price),
            'size': float(data.size),
            'exchange': data.exchange,
            'ingestion_time': datetime.utcnow().isoformat()
        }
        
        producer.produce(
            topic='raw',
            key=data.symbol.encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0)
        
        # Debug-Output
        print(f"Trade: {data.symbol} ${data.price} x {data.size}")

    if mode == Mode.STOCK:
        stream = StockDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)
    elif mode == Mode.CRYPTO:
        stream = CryptoDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    stream.subscribe_trades(handle_trade, *symbols)
    print("Stream started! Get ready for the Alpaca Herd!")

    # make sure that all received messages are still send to Kafka if stream is stopped
    try:
        stream.run()
    finally:
        producer.flush()

run_stream(Mode.STOCK, Symbols.STOCK)