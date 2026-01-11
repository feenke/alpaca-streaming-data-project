# Alpaca Streaming Data Pipeline

Real-time OHLCV aggregation with Alpaca, Kafka, and Flink.

## Setup

1. Create `.env` file:
```
   ALPACA_API_KEY=your_key
   ALPACA_API_SECRET=your_secret
```

2. Start:
```bash
   docker-compose up --build
```

3. Flink Dashboard: http://localhost:8081