"""
Synthetic Data Generator for Testing
Generates deterministic test data for environment parity validation.
"""

import json
import time
from datetime import datetime, timezone
from typing import List, Dict


def generate_synthetic_ticks(count: int = 100, seed: int = 42) -> List[Dict]:
    """
    Generate deterministic synthetic tick data for testing.
    Uses a fixed seed for reproducibility across environments.
    """
    import numpy as np
    np.random.seed(seed)

    symbols = ["MSFT", "AAPL", "NVDA", "GOOGL", "AMZN"]
    base_prices = {"MSFT": 420.0, "AAPL": 195.0, "NVDA": 880.0, "GOOGL": 175.0, "AMZN": 185.0}

    ticks = []
    prices = dict(base_prices)
    base_time_ms = int(datetime(2026, 1, 1, 9, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)

    for i in range(count):
        symbol = symbols[i % len(symbols)]
        price = prices[symbol]

        # Deterministic price movement
        change = np.random.normal(0, price * 0.001)
        new_price = max(price + change, 0.01)
        prices[symbol] = new_price

        spread = new_price * 0.0002
        bid = new_price - spread / 2
        ask = new_price + spread / 2

        tick = {
            "symbol": symbol,
            "timestamp": datetime.fromtimestamp((base_time_ms + i) / 1000, tz=timezone.utc).isoformat(),
            "timestamp_ms": base_time_ms + i,
            "sequence": i + 1,
            "price": round(new_price, 4),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "spread": round(ask - bid, 4),
            "volume": int(np.random.randint(10, 500)),
            "volume_24h": 1000000 + i * 100,
            "change_pct": round(((new_price - base_prices[symbol]) / base_prices[symbol]) * 100, 4),
            "volatility": 0.015,
        }
        ticks.append(tick)

    return ticks


def generate_invalid_ticks() -> List[Dict]:
    """Generate known-invalid ticks for testing error handling."""
    base_time_ms = int(time.time() * 1000)

    return [
        # Missing required field
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timestamp_ms": base_time_ms,
            "sequence": 1,
            "price": 420.0,
            "bid": 419.95,
            "ask": 420.05,
            "spread": 0.10,
            "volume": 100,
            "volume_24h": 1000,
        },
        # Negative price
        {
            "symbol": "MSFT",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timestamp_ms": base_time_ms + 1,
            "sequence": 2,
            "price": -5.0,
            "bid": -5.05,
            "ask": -4.95,
            "spread": 0.10,
            "volume": 100,
            "volume_24h": 1000,
        },
        # Crossed market (bid > ask)
        {
            "symbol": "AAPL",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timestamp_ms": base_time_ms + 2,
            "sequence": 3,
            "price": 195.0,
            "bid": 200.0,
            "ask": 190.0,
            "spread": -10.0,
            "volume": 100,
            "volume_24h": 1000,
        },
        # Invalid symbol format
        {
            "symbol": "invalid!!",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timestamp_ms": base_time_ms + 3,
            "sequence": 4,
            "price": 100.0,
            "bid": 99.95,
            "ask": 100.05,
            "spread": 0.10,
            "volume": 100,
            "volume_24h": 1000,
        },
        # Extreme change_pct
        {
            "symbol": "TSLA",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timestamp_ms": base_time_ms + 4,
            "sequence": 5,
            "price": 245.0,
            "bid": 244.95,
            "ask": 245.05,
            "spread": 0.10,
            "volume": 100,
            "volume_24h": 1000,
            "change_pct": -99.0,
            "volatility": 0.035,
        },
    ]


if __name__ == "__main__":
    # Generate and save test fixtures
    valid_ticks = generate_synthetic_ticks(1000)
    invalid_ticks = generate_invalid_ticks()

    with open("fixtures/valid_ticks.json", "w") as f:
        json.dump(valid_ticks, f, indent=2)

    with open("fixtures/invalid_ticks.json", "w") as f:
        json.dump(invalid_ticks, f, indent=2)

    print(f"Generated {len(valid_ticks)} valid ticks and {len(invalid_ticks)} invalid ticks")
