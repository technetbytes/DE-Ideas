# Kafka Stock Streaming Pipeline

Real-time stock market data streaming pipeline using Apache Kafka, Python, and PostgreSQL with full DataOps monitoring.

## Architecture

```
┌──────────────┐     ┌─────────┐     ┌──────────────┐     ┌────────────┐
│   Producer   │────▶│  Kafka  │────▶│   Consumer   │────▶│ PostgreSQL │
│ (25 symbols) │     │ (6 part)│     │ (batch write)│     │  (TimeSeries)│
└──────────────┘     └─────────┘     └──────────────┘     └────────────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────┐
│              Prometheus + Grafana                     │
│         (metrics, dashboards, alerting)              │
└─────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Start everything
make up

# Check health
make health

# View logs
make logs

# Access services
# Kafka UI:    http://localhost:8080
# Grafana:     http://localhost:3000 (admin/admin)
# Prometheus:  http://localhost:9090
# PostgreSQL:  localhost:5432
```

## Stock Symbols (25)

MSFT, AAPL, NVDA, GOOGL, AMZN, TSLA, META, BRK.B, JPM, V, JNJ, WMT, UNH, MA, PG, HD, XOM, AVGO, LLY, COST, NFLX, AMD, ADBE, CRM, INTC

## Components

### Producer
- Generates millisecond-level tick data using Geometric Brownian Motion
- Configurable symbols via `config/symbols.json`
- LZ4 compression, batched delivery
- Prometheus metrics on port 8000

### Consumer
- Batch writes to PostgreSQL (500 records or 1s timeout)
- Exactly-once semantics with manual offset commits
- Connection retry logic
- Prometheus metrics on port 8001

### Database
- Optimized schema with proper indexes
- Materialized views for 1s and 1m OHLCV bars
- Data retention (7-day auto-cleanup)
- Unique constraints for deduplication

### Monitoring
- Grafana dashboard: pipeline throughput, latency percentiles, stock prices, consumer lag
- Prometheus: scrapes producer, consumer, and Kafka exporter metrics
- Kafka UI: topic inspection, consumer group monitoring

## DataOps

### CI/CD (GitHub Actions)
- Linting (flake8, black, mypy)
- Docker image builds with layer caching
- Integration tests with real Kafka + PostgreSQL
- Full stack health checks

### Useful Commands

```bash
make db-count       # Tick counts per symbol
make db-latest      # Latest 20 ticks
make consumer-lag   # Check consumer group lag
make topics         # List Kafka topics
make db-shell       # PostgreSQL interactive shell
```

## Configuration

All config via environment variables (see `.env.example`). Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| PRODUCE_INTERVAL_MS | 1 | Tick interval in milliseconds |
| BATCH_SIZE | 500 | Consumer batch size before flush |
| BATCH_TIMEOUT_MS | 1000 | Max time before batch flush |

## Stopping

```bash
make down           # Stop containers
make clean          # Stop + remove volumes + prune
```
