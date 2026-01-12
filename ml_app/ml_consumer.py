import json
import os
from collections import defaultdict
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import time

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

class TradingSignalGenerator:
    """Generates trading signals based on simple technical indicators"""
    
    def __init__(self, window_size):
        self.window_size = window_size
        # store candles per symbol
        self.candles = defaultdict(list)
    
    def add_candle(self, candle: dict) -> dict | None:
        """Add a candle and generate signal if there is enough data"""
        symbol = candle['symbol']
        self.candles[symbol].append(candle)
        
        # keep only last N candles
        if len(self.candles[symbol]) > self.window_size:
            self.candles[symbol] = self.candles[symbol][-self.window_size:]
        
        # generate signal if there is enough data
        if len(self.candles[symbol]) >= self.window_size:
            return self.generate_signal(symbol)
        else:
            return {
                'symbol': symbol,
                'signal': 'WAIT',
                'reason': f'Collecting data ({len(self.candles[symbol])}/{self.window_size})',
                'confidence': 0.0
            }
    
    def generate_signal(self, symbol: str) -> dict:
        """Generate Buy/Sell/Hold signal based on technical indicators"""
        candles = self.candles[symbol]
        
        # extract prices
        closes = [c['close_price'] for c in candles]
        highs = [c['high_price'] for c in candles]
        lows = [c['low_price'] for c in candles]
        volumes = [c['volume'] for c in candles]
        
        # calculate indicators
        sma_short = sum(closes[-3:]) / 3  # 3-period SMA
        sma_long = sum(closes) / len(closes)  # 10-period SMA
        
        # price momentum (% change over window)
        momentum = (closes[-1] - closes[0]) / closes[0] * 100
        
        # volume trend
        vol_recent = sum(volumes[-3:]) / 3
        vol_avg = sum(volumes) / len(volumes)
        volume_increase = vol_recent > vol_avg * 1.2
        
        # RSI (simplified)
        gains = []
        losses = []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0.0001
        rs = avg_gain / avg_loss if avg_loss else 0
        rsi = 100 - (100 / (1 + rs))
        
        # price position (where is current price in recent range)
        price_range = max(highs) - min(lows)
        if price_range > 0:
            price_position = (closes[-1] - min(lows)) / price_range
        else:
            price_position = 0.5
        
        # generate signal based on indicators
        signal = 'HOLD'
        reasons = []
        confidence = 0.5
        
        # bullish signals
        bullish_points = 0
        if sma_short > sma_long:
            bullish_points += 1
            reasons.append('SMA crossover bullish')
        if momentum > 0.5:
            bullish_points += 1
            reasons.append(f'Positive momentum ({momentum:.2f}%)')
        if rsi < 30:
            bullish_points += 1
            reasons.append(f'RSI oversold ({rsi:.1f})')
        if volume_increase and momentum > 0:
            bullish_points += 1
            reasons.append('Volume confirming uptrend')
        
        # bearish signals
        bearish_points = 0
        if sma_short < sma_long:
            bearish_points += 1
            reasons.append('SMA crossover bearish')
        if momentum < -0.5:
            bearish_points += 1
            reasons.append(f'Negative momentum ({momentum:.2f}%)')
        if rsi > 70:
            bearish_points += 1
            reasons.append(f'RSI overbought ({rsi:.1f})')
        if volume_increase and momentum < 0:
            bearish_points += 1
            reasons.append('Volume confirming downtrend')
        
        # determine signal
        if bullish_points >= 2 and bullish_points > bearish_points:
            signal = 'BUY'
            confidence = min(0.5 + (bullish_points * 0.15), 0.95)
        elif bearish_points >= 2 and bearish_points > bullish_points:
            signal = 'SELL'
            confidence = min(0.5 + (bearish_points * 0.15), 0.95)
        else:
            signal = 'HOLD'
            confidence = 0.5
            if not reasons:
                reasons.append('No clear trend detected')
        
        return {
            'symbol': symbol,
            'signal': signal,
            'confidence': round(confidence, 2),
            'reasons': reasons,
            'indicators': {
                'sma_short': round(sma_short, 4),
                'sma_long': round(sma_long, 4),
                'momentum': round(momentum, 2),
                'rsi': round(rsi, 1),
                'price_position': round(price_position, 2)
            },
            'current_price': closes[-1],
            'timestamp': datetime.utcnow().isoformat()
        }


def create_consumer():
    """create Kafka consumer with retry logic"""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ml-trading-signals',
        'auto.offset.reset': 'latest'
    }
    
    max_retries = 10
    for attempt in range(max_retries):
        try:
            consumer = Consumer(config)
            consumer.subscribe(['processed'])
            print(f"✓ Connected to Kafka, subscribed to 'processed' topic")
            return consumer
        except Exception as e:
            print(f"Kafka not available (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(5)
    
    raise Exception("Could not connect to Kafka")


def run_signal_generator(window_size):
    """main loop: consume OHLCV data and generate signals"""
    print("=" * 60)
    print("🤖 Trading Signal Generator")
    print("=" * 60)
    print("=" * 60)

    time.sleep(10) # initial wait

    consumer = create_consumer()
    generator = TradingSignalGenerator(window_size)

    
    
    print("\n📊 Waiting for OHLCV data...\n")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue
            
            # parse OHLCV candle
            try:
                candle = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError:
                continue
            
            # print received candle
            print(f"📥 {candle['symbol']} | "
                  f"O:{candle['open_price']:.4f} H:{candle['high_price']:.4f} "
                  f"L:{candle['low_price']:.4f} C:{candle['close_price']:.4f} | "
                  f"Vol:{candle['volume']:.2f}")
            
            # generate signal
            signal = generator.add_candle(candle)
            
            # print signal
            if signal['signal'] == 'BUY':
                emoji = '🟢'
            elif signal['signal'] == 'SELL':
                emoji = '🔴'
            elif signal['signal'] == 'HOLD':
                emoji = '🟡'
            else:
                emoji = '⏳'
            
            print(f"   {emoji} {signal['signal']} | "
                  f"Confidence: {signal.get('confidence', 0):.0%}")
            
            if 'reasons' in signal and signal['reasons']:
                for reason in signal['reasons']:
                    print(f"      → {reason}")
            
            if 'indicators' in signal:
                ind = signal['indicators']
                print(f"      📈 RSI:{ind['rsi']} | "
                      f"Momentum:{ind['momentum']}% | "
                      f"SMA:{ind['sma_short']:.4f}/{ind['sma_long']:.4f}")
            
            print()
    
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_signal_generator(window_size=10)