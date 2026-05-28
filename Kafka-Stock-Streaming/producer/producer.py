"""
Stock Market Data Producer
Generates realistic millisecond-level tick data for 25+ stock symbols
and publishes to Apache Kafka.
"""

import os
import json
import time
import signal
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List

import numpy as np
import orjson
from confluent_kafka import Producer, KafkaError

from metrics import (
    messages_produced_total,
    produce_latency,
    last_price,
    producer_active,
    symbols_count,
    messages_per_second,
    start_metrics_server,
)

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-ticks")
PRODUCE_INTERVAL_MS = int(os.getenv("PRODUCE_INTERVAL_MS", "1"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
SYMBOLS_CONFIG_PATH = os.getenv("SYMBOLS_CONFIG_PATH", "/app/config/symbols.json")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stock-producer")


class StockTickGenerator:
    """Generates realistic stock tick data using geometric Brownian motion."""

    def __init__(self, symbols_config: List[Dict]):
        self.symbols = {}
        for s in symbols_config:
            self.symbols[s["symbol"]] = {
                "name": s["name"],
                "price": s["base_price"],
                "volatility": s["volatility"],
                "base_price": s["base_price"],
                "bid_ask_spread": s["base_price"] * 0.0002,  # 2 bps spread
                "volume_24h": 0,
            }
        self._sequence = 0

    def generate_tick(self, symbol: str) -> Dict:
        """Generate a single tick for a symbol using GBM."""
        state = self.symbols[symbol]
        dt = PRODUCE_INTERVAL_MS / 1000.0  # Convert to seconds

        # Geometric Brownian Motion
        drift = 0.0  # No long-term drift for simulation
        vol = state["volatility"]
        random_shock = np.random.normal(0, 1)
        price_change = state["price"] * (drift * dt + vol * np.sqrt(dt) * random_shock)

        # Update price with mean reversion (prevents runaway prices)
        mean_reversion_strength = 0.001
        mean_reversion = mean_reversion_strength * (state["base_price"] - state["price"])
        new_price = max(state["price"] + price_change + mean_reversion, 0.01)

        state["price"] = new_price

        # Generate bid/ask
        half_spread = state["bid_ask_spread"] * (1 + abs(random_shock) * 0.5)
        bid = new_price - half_spread
        ask = new_price + half_spread

        # Generate volume (higher during volatile moments)
        base_volume = np.random.randint(10, 500)
        volume = int(base_volume * (1 + abs(random_shock) * 2))
        state["volume_24h"] += volume

        self._sequence += 1
        now = datetime.now(timezone.utc)

        tick = {
            "symbol": symbol,
            "timestamp": now.isoformat(),
            "timestamp_ms": int(now.timestamp() * 1000),
            "sequence": self._sequence,
            "price": round(new_price, 4),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "spread": round(ask - bid, 4),
            "volume": volume,
            "volume_24h": state["volume_24h"],
            "change_pct": round(
                ((new_price - state["base_price"]) / state["base_price"]) * 100, 4
            ),
            "volatility": vol,
        }

        return tick


class StockProducer:
    """High-throughput Kafka producer for stock tick data."""

    def __init__(self):
        self._running = False
        self._lock = threading.Lock()
        self._msg_count = 0
        self._last_rate_check = time.time()

        # Load symbols config
        self.symbols_config = self._load_symbols_config()
        self.generator = StockTickGenerator(self.symbols_config)

        # Kafka producer config optimized for high throughput
        self.producer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "stock-producer-1",
            "acks": "1",  # Leader ack only for speed
            "linger.ms": 5,  # Batch messages for 5ms
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.kbytes": 1048576,  # 1GB buffer
            "compression.type": "lz4",
            "message.send.max.retries": 3,
            "retry.backoff.ms": 100,
        }

        self.producer = Producer(self.producer_config)
        logger.info(
            f"Producer initialized | Broker: {KAFKA_BOOTSTRAP_SERVERS} | "
            f"Topic: {KAFKA_TOPIC} | Symbols: {len(self.symbols_config)} | "
            f"Interval: {PRODUCE_INTERVAL_MS}ms"
        )

    def _load_symbols_config(self) -> List[Dict]:
        """Load stock symbols from config file."""
        try:
            with open(SYMBOLS_CONFIG_PATH, "r") as f:
                config = json.load(f)
            symbols = config["symbols"]
            logger.info(f"Loaded {len(symbols)} symbols from {SYMBOLS_CONFIG_PATH}")
            return symbols
        except FileNotFoundError:
            logger.warning(f"Config not found at {SYMBOLS_CONFIG_PATH}, using defaults")
            return [
                {"symbol": "MSFT", "name": "Microsoft", "base_price": 420.0, "volatility": 0.015},
                {"symbol": "AAPL", "name": "Apple", "base_price": 195.0, "volatility": 0.012},
                {"symbol": "NVDA", "name": "NVIDIA", "base_price": 880.0, "volatility": 0.025},
            ]

    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err:
            messages_produced_total.labels(symbol="unknown", status="error").inc()
            logger.error(f"Delivery failed: {err}")
        else:
            symbol = msg.key().decode("utf-8") if msg.key() else "unknown"
            messages_produced_total.labels(symbol=symbol, status="success").inc()

    def _update_rate_metrics(self):
        """Update messages-per-second gauge."""
        now = time.time()
        elapsed = now - self._last_rate_check
        if elapsed >= 1.0:
            rate = self._msg_count / elapsed
            messages_per_second.set(rate)
            self._msg_count = 0
            self._last_rate_check = now

    def produce_tick(self, symbol: str):
        """Generate and produce a single tick to Kafka."""
        tick = self.generator.generate_tick(symbol)

        # Serialize with orjson for speed
        value = orjson.dumps(tick)
        key = symbol.encode("utf-8")

        start = time.perf_counter()
        self.producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=value,
            callback=self._delivery_callback,
            partition=-1,  # Let Kafka partition by key
        )
        elapsed = time.perf_counter() - start

        produce_latency.observe(elapsed)
        last_price.labels(symbol=symbol).set(tick["price"])

        self._msg_count += 1
        self._update_rate_metrics()

    def run(self):
        """Main production loop - generates ticks at millisecond intervals."""
        self._running = True
        producer_active.set(1)
        symbols_count.set(len(self.symbols_config))

        symbol_names = [s["symbol"] for s in self.symbols_config]
        logger.info(f"Starting production loop | {len(symbol_names)} symbols | {PRODUCE_INTERVAL_MS}ms interval")
        logger.info(f"Symbols: {', '.join(symbol_names)}")

        interval_sec = PRODUCE_INTERVAL_MS / 1000.0
        cycle = 0

        try:
            while self._running:
                cycle_start = time.perf_counter()

                # Round-robin through symbols each cycle
                symbol = symbol_names[cycle % len(symbol_names)]
                self.produce_tick(symbol)

                # Poll for delivery reports (non-blocking)
                self.producer.poll(0)

                cycle += 1

                # Precise sleep to maintain interval
                elapsed = time.perf_counter() - cycle_start
                sleep_time = interval_sec - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

                # Log stats every 10 seconds
                if cycle % (10000 // max(PRODUCE_INTERVAL_MS, 1)) == 0:
                    logger.info(
                        f"Produced {cycle} ticks | "
                        f"Rate: {messages_per_second._value.get():.0f} msg/s | "
                        f"Last: {symbol} @ ${self.generator.symbols[symbol]['price']:.2f}"
                    )

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            self.shutdown()

    def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down producer...")
        self._running = False
        producer_active.set(0)

        # Flush remaining messages
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")
        else:
            logger.info("All messages flushed successfully")


def main():
    # Start Prometheus metrics server
    start_metrics_server(METRICS_PORT)

    # Create and run producer
    producer = StockProducer()

    # Handle signals for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"Signal {sig} received, stopping...")
        producer._running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    producer.run()


if __name__ == "__main__":
    main()
