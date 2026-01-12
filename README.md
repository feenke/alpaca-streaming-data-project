# Alpaca Streaming Data Pipeline

Real-time OHLCV aggregation with Alpaca, Kafka, and Flink and simple Rule-based Buy/Hold/Sell Recommendation App in Docker environment.

## Prerequisites

- Docker Desktop https://www.docker.com/products/docker-desktop/
  
- Alpaca Account (free tier): https://alpaca.markets/
  
   - Get API Keys by scrolling down in the Paper Account view and click on Generate.

     
  <img width="1368" height="668" alt="grafik" src="https://github.com/user-attachments/assets/243d9f2d-f8df-4992-9ba5-49479fd37e0b" />

## Setup

1. Create `.env` file:
```
   ALPACA_API_KEY=your_key
   ALPACA_API_SECRET=your_secret
   KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```
2. The stream can use either the Crypto or Stock market Stream. Default is `Stock`.
   
   To switch the mode, change input parameters of run_stream() in ingestion/alpaca_data_pull.py to either CRYPTO or STOCK.
   
   Crypto Mode: ```run_stream(Mode.CRYPTO, Symbols.CRYPTO)```
   
   Stock Mode: ```run_stream(Mode.STOCK, Symbols.STOCK)```
   
   If you want to add or remove Symbols, please adjust the class Symbols in ingestion/alpaca_data_pull.py
   Default setting:
```
   CRYPTO = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'DOGE/USD', 'XRP/USD', 'AVAX/USD', 'LINK/USD', 'MATIC/USD', 'ADA/USD', 'DOT/USD']
   STOCK = ['AMD','TSLA', 'NVDA', 'AMZN']
```
  
3. Start:
```bash
   docker-compose up --build
```

4. Flink Dashboard is available at: http://localhost:8081

## Usage
- The Ingestion service prints out pulled data from Alpaca and a confirmation that it sent the data to Kafka raw topic:
```
   2026-01-12 15:33:44.521 | Trade: NVDA $184.66 x 200.0
   2026-01-12 15:33:45.159 | Message send successfully to raw [0]
   2026-01-12 15:33:45.159 | Trade: AMZN $247.17 x 100.0
   2026-01-12 15:33:45.377 | Message send successfully to raw [0]
   2026-01-12 15:33:45.377 | Trade: AMD $202.18 x 30.0
   2026-01-12 15:33:45.531 | Message send successfully to raw [0]
```

- The ML app (rather rule-based app) pings will occurr if at least 10 datapoints were collected for a symbol (No real recommendation! Just for real-time pipeline demonstration).
```
   2026-01-12 16:11:28.419 | 📥 NVDA | O:184.9700 H:185.0300 L:184.9700 C:185.0000 | Vol:1537.00
   2026-01-12 16:11:28.419 |    🟢 BUY | Confidence: 80%
   2026-01-12 16:11:28.419 |       → SMA crossover bullish
   2026-01-12 16:11:28.419 |       → Volume confirming uptrend
   2026-01-12 16:11:28.419 |       📈 RSI:56.5 | Momentum:0.03% | SMA:184.9850/184.9075
   2026-01-12 16:11:28.419 | 
   2026-01-12 16:11:38.033 | 📥 NVDA | O:185.0100 H:185.0450 L:184.9700 C:184.9800 | Vol:527.00
   2026-01-12 16:11:38.033 |    🟢 BUY | Confidence: 80%
   2026-01-12 16:11:38.033 |       → SMA crossover bullish
   2026-01-12 16:11:38.033 |       → Volume confirming uptrend
   2026-01-12 16:11:38.033 |       📈 RSI:65.0 | Momentum:0.06% | SMA:184.9933/184.9115
   2026-01-12 16:11:38.033 | 
   2026-01-12 16:11:38.035 | 📥 AMD | O:206.5550 H:206.5650 L:206.4350 C:206.4350 | Vol:568.00
   2026-01-12 16:11:38.035 |    🟡 HOLD | Confidence: 50%
   2026-01-12 16:11:38.035 |       → SMA crossover bearish
   2026-01-12 16:11:38.035 |       📈 RSI:37.5 | Momentum:-0.13% | SMA:206.5017/206.5235
   2026-01-12 16:11:38.035 | 
   2026-01-12 16:11:38.035 | 📥 AMZN | O:247.5800 H:247.5800 L:247.5100 C:247.5300 | Vol:568.00
   2026-01-12 16:11:38.035 |    🟡 HOLD | Confidence: 50%
   2026-01-12 16:11:38.035 |       → RSI oversold (7.3)
   2026-01-12 16:11:38.035 |       → SMA crossover bearish
   2026-01-12 16:11:38.035 |       📈 RSI:7.3 | Momentum:-0.17% | SMA:247.5583/247.7725
```
