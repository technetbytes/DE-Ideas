"""Prometheus metrics for the stock consumer."""

from prometheus_client import Counter, Histogram, Gauge, start_http_server


# Counters
messages_consumed_total = Counter(
    'stock_consumer_messages_total',
    'Total messages consumed from Kafka',
    ['symbol', 'status']
)

db_inserts_total = Counter(
    'stock_consumer_db_inserts_total',
    'Total rows inserted into PostgreSQL',
    ['status']
)

batches_processed_total = Counter(
    'stock_consumer_batches_total',
    'Total batches written to PostgreSQL',
    ['status']
)

# Histograms
consume_latency = Histogram(
    'stock_consumer_processing_latency_seconds',
    'End-to-end processing latency (Kafka produce to DB write)',
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
)

batch_write_duration = Histogram(
    'stock_consumer_batch_write_seconds',
    'Time to write a batch to PostgreSQL',
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# Gauges
consumer_lag = Gauge(
    'stock_consumer_lag',
    'Consumer group lag (messages behind)',
    ['partition']
)

consumer_active = Gauge(
    'stock_consumer_active',
    'Whether the consumer is actively running'
)

batch_size_current = Gauge(
    'stock_consumer_batch_size',
    'Current batch size being processed'
)

db_connection_pool_active = Gauge(
    'stock_consumer_db_pool_active',
    'Active database connections'
)


def start_metrics_server(port: int = 8001):
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
    print(f"[Metrics] Prometheus metrics server started on port {port}")
