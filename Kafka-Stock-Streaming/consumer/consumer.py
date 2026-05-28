"""
Stock Market Data Consumer
Consumes tick data from Kafka and writes to PostgreSQL in optimized batches.
"""

import os
import time
import signal
import logging
from datetime import datetime, timezone
from typing import List, Dict

import orjson
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException

from metrics import (
    messages_consumed_total,
    db_inserts_total,
    batches_processed_total,
    consume_latency,
    batch_write_duration,
    consumer_active,
    batch_size_current,
    db_connection_pool_active,
    start_metrics_server,
)

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-ticks")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "stock-consumer-group")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "stockdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "stockuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "stockpass123")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "1000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stock-consumer")


class PostgresWriter:
    """Handles batch writes to PostgreSQL with connection management."""

    INSERT_SQL = """
        INSERT INTO stock_ticks (
            symbol, timestamp, timestamp_ms, sequence, price,
            bid, ask, spread, volume, volume_24h, change_pct, volatility
        ) VALUES %s
        ON CONFLICT (symbol, timestamp_ms, sequence) DO NOTHING
    """

    def __init__(self):
        self.conn = None
        self._connect()

    def _connect(self):
        """Establish database connection with retries."""
        max_retries = 10
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    connect_timeout=10,
                )
                self.conn.autocommit = False
                db_connection_pool_active.set(1)
                logger.info(f"Connected to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"DB connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise

    def write_batch(self, records: List[Dict]) -> int:
        """Write a batch of records to PostgreSQL using execute_values."""
        if not records:
            return 0

        start = time.perf_counter()

        try:
            values = [
                (
                    r["symbol"],
                    r["timestamp"],
                    r["timestamp_ms"],
                    r["sequence"],
                    r["price"],
                    r["bid"],
                    r["ask"],
                    r["spread"],
                    r["volume"],
                    r["volume_24h"],
                    r["change_pct"],
                    r["volatility"],
                )
                for r in records
            ]

            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, self.INSERT_SQL, values, page_size=1000
                )
            self.conn.commit()

            elapsed = time.perf_counter() - start
            batch_write_duration.observe(elapsed)
            db_inserts_total.labels(status="success").inc(len(records))
            batches_processed_total.labels(status="success").inc()

            return len(records)

        except psycopg2.Error as e:
            self.conn.rollback()
            db_inserts_total.labels(status="error").inc(len(records))
            batches_processed_total.labels(status="error").inc()
            logger.error(f"Batch write failed: {e}")

            # Reconnect if connection is broken
            if self.conn.closed:
                logger.info("Reconnecting to database...")
                self._connect()

            return 0

    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            db_connection_pool_active.set(0)
            logger.info("Database connection closed")


class StockConsumer:
    """High-throughput Kafka consumer with batch PostgreSQL writes."""

    def __init__(self):
        self._running = False
        self._batch: List[Dict] = []
        self._batch_start_time = time.time()

        # Kafka consumer config
        self.consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 30000,
            "fetch.min.bytes": 1024,
            "fetch.max.wait.ms": 100,
        }

        self.consumer = Consumer(self.consumer_config)
        self.db_writer = PostgresWriter()

        logger.info(
            f"Consumer initialized | Broker: {KAFKA_BOOTSTRAP_SERVERS} | "
            f"Topic: {KAFKA_TOPIC} | Group: {KAFKA_GROUP_ID} | "
            f"Batch size: {BATCH_SIZE} | Batch timeout: {BATCH_TIMEOUT_MS}ms"
        )

    def _should_flush(self) -> bool:
        """Check if batch should be flushed."""
        if len(self._batch) >= BATCH_SIZE:
            return True
        elapsed_ms = (time.time() - self._batch_start_time) * 1000
        if elapsed_ms >= BATCH_TIMEOUT_MS and len(self._batch) > 0:
            return True
        return False

    def _flush_batch(self):
        """Flush current batch to PostgreSQL."""
        if not self._batch:
            return

        batch_size_current.set(len(self._batch))
        count = self.db_writer.write_batch(self._batch)

        if count > 0:
            logger.debug(f"Flushed {count} records to PostgreSQL")

        self._batch = []
        self._batch_start_time = time.time()

        # Commit offsets after successful write
        self.consumer.commit(asynchronous=False)

    def run(self):
        """Main consumption loop."""
        self._running = True
        consumer_active.set(1)

        self.consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")

        total_consumed = 0
        last_log_time = time.time()

        try:
            while self._running:
                msg = self.consumer.poll(timeout=0.1)

                if msg is None:
                    # Check timeout flush even when no messages
                    if self._should_flush():
                        self._flush_batch()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Deserialize message
                try:
                    tick = orjson.loads(msg.value())
                    symbol = tick.get("symbol", "unknown")
                    messages_consumed_total.labels(symbol=symbol, status="success").inc()

                    # Track end-to-end latency
                    if "timestamp_ms" in tick:
                        latency = (time.time() * 1000 - tick["timestamp_ms"]) / 1000.0
                        consume_latency.observe(max(latency, 0))

                    self._batch.append(tick)
                    total_consumed += 1

                except (orjson.JSONDecodeError, KeyError) as e:
                    messages_consumed_total.labels(symbol="unknown", status="error").inc()
                    logger.warning(f"Failed to deserialize message: {e}")
                    continue

                # Check if batch is ready to flush
                if self._should_flush():
                    self._flush_batch()

                # Log stats every 10 seconds
                if time.time() - last_log_time >= 10:
                    logger.info(f"Consumed {total_consumed} total messages | Batch buffer: {len(self._batch)}")
                    last_log_time = time.time()

        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")
        finally:
            self.shutdown()

    def shutdown(self):
        """Graceful shutdown - flush remaining and close connections."""
        logger.info("Shutting down consumer...")
        self._running = False
        consumer_active.set(0)

        # Flush remaining batch
        if self._batch:
            logger.info(f"Flushing remaining {len(self._batch)} records...")
            self._flush_batch()

        self.consumer.close()
        self.db_writer.close()
        logger.info("Consumer shutdown complete")


def main():
    start_metrics_server(METRICS_PORT)

    consumer = StockConsumer()

    def signal_handler(sig, frame):
        logger.info(f"Signal {sig} received, stopping...")
        consumer._running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer.run()


if __name__ == "__main__":
    main()
