"""Prometheus metrics for the stock producer."""

from prometheus_client import Counter, Histogram, Gauge, start_http_server


# Counters
messages_produced_total = Counter(
    'stock_producer_messages_total',
    'Total messages produced to Kafka',
    ['symbol', 'status']
)

# Histograms
produce_latency = Histogram(
    'stock_producer_latency_seconds',
    'Time taken to produce a message to Kafka',
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
)

# Gauges
last_price = Gauge(
    'stock_producer_last_price',
    'Last generated price for a symbol',
    ['symbol']
)

producer_active = Gauge(
    'stock_producer_active',
    'Whether the producer is actively running (1=active, 0=stopped)'
)

symbols_count = Gauge(
    'stock_producer_symbols_count',
    'Number of symbols being produced'
)

messages_per_second = Gauge(
    'stock_producer_messages_per_second',
    'Current message production rate'
)


def start_metrics_server(port: int = 8000):
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
    print(f"[Metrics] Prometheus metrics server started on port {port}")
