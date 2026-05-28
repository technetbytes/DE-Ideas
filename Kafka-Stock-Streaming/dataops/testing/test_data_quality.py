"""
Data Pipeline Unit & Integration Tests
Run in CI to validate data transformations and contract compliance.
"""

import time
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

import sys
sys.path.insert(0, '../dataops/data-quality')
sys.path.insert(0, '../producer')
sys.path.insert(0, '../consumer')


class TestDataContract:
    """Unit tests for data contract validator."""

    @pytest.fixture
    def validator(self):
        from contract_validator import DataContractValidator
        return DataContractValidator()

    @pytest.fixture
    def valid_tick(self):
        return {
            "symbol": "MSFT",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timestamp_ms": int(time.time() * 1000),
            "sequence": 1,
            "price": 420.50,
            "bid": 420.45,
            "ask": 420.55,
            "spread": 0.10,
            "volume": 100,
            "volume_24h": 5000000,
            "change_pct": 0.5,
            "volatility": 0.015,
        }

    def test_valid_tick_passes(self, validator, valid_tick):
        is_valid, violations = validator.validate(valid_tick)
        assert is_valid is True
        assert len(violations) == 0

    def test_missing_required_field_fails(self, validator, valid_tick):
        del valid_tick['symbol']
        is_valid, violations = validator.validate(valid_tick)
        assert is_valid is False
        assert any(v['rule'] == 'required_field' for v in violations)

    def test_null_price_fails(self, validator, valid_tick):
        valid_tick['price'] = None
        is_valid, violations = validator.validate(valid_tick)
        assert is_valid is False

    def test_negative_price_fails(self, validator, valid_tick):
        valid_tick['price'] = -10.0
        is_valid, violations = validator.validate(valid_tick)
        assert is_valid is False
        assert any(v['rule'] == 'price_range' for v in violations)

    def test_bid_above_ask_fails(self, validator, valid_tick):
        valid_tick['bid'] = 425.00
        valid_tick['ask'] = 420.00
        is_valid, violations = validator.validate(valid_tick)
        assert is_valid is False
        assert any(v['rule'] == 'bid_ask_crossing' for v in violations)

    def test_invalid_symbol_format(self, validator, valid_tick):
        valid_tick['symbol'] = 'invalid_symbol!'
        is_valid, violations = validator.validate(valid_tick)
        assert is_valid is False
        assert any(v['rule'] == 'symbol_format' for v in violations)

    def test_future_timestamp_fails(self, validator, valid_tick):
        valid_tick['timestamp_ms'] = int(time.time() * 1000) + 60000  # 1 min in future
        is_valid, violations = validator.validate(valid_tick)
        assert any(v['rule'] == 'timestamp_future' for v in violations)

    def test_stale_timestamp_warns(self, validator, valid_tick):
        valid_tick['timestamp_ms'] = int(time.time() * 1000) - 10000  # 10s old
        is_valid, violations = validator.validate(valid_tick)
        assert any(v['rule'] == 'timestamp_freshness' for v in violations)

    def test_volume_zero_fails(self, validator, valid_tick):
        valid_tick['volume'] = 0
        is_valid, violations = validator.validate(valid_tick)
        assert any(v['rule'] == 'volume_range' for v in violations)

    def test_sequence_monotonicity(self, validator, valid_tick):
        # First tick is fine
        valid_tick['sequence'] = 10
        validator.validate(valid_tick)

        # Second tick with lower sequence should warn
        valid_tick['sequence'] = 5
        is_valid, violations = validator.validate(valid_tick)
        assert any(v['rule'] == 'sequence_monotonic' for v in violations)

    def test_volume_24h_monotonicity(self, validator, valid_tick):
        valid_tick['volume_24h'] = 5000000
        valid_tick['sequence'] = 1
        validator.validate(valid_tick)

        valid_tick['volume_24h'] = 4000000  # Decreased
        valid_tick['sequence'] = 2
        is_valid, violations = validator.validate(valid_tick)
        assert any(v['rule'] == 'volume_24h_monotonic' for v in violations)

    def test_extreme_change_pct_fails(self, validator, valid_tick):
        valid_tick['change_pct'] = -75.0
        is_valid, violations = validator.validate(valid_tick)
        assert is_valid is False
        assert any(v['rule'] == 'change_pct_bounds' for v in violations)

    def test_spread_exceeds_max_pct(self, validator, valid_tick):
        valid_tick['spread'] = 10.0  # Way too high for a $420 stock
        valid_tick['ask'] = 430.0
        valid_tick['bid'] = 420.0
        is_valid, violations = validator.validate(valid_tick)
        assert any(v['rule'] == 'spread_max_pct' for v in violations)

    def test_compliance_tracking(self, validator, valid_tick):
        # Run several valid ticks
        for i in range(10):
            valid_tick['sequence'] = i + 1
            valid_tick['volume_24h'] = 5000000 + (i * 100)
            validator.validate(valid_tick)

        stats = validator.get_stats()
        assert stats['total_checks'] == 10
        assert stats['compliance_pct'] == 100.0


class TestProducerTickGeneration:
    """Unit tests for the stock tick generator."""

    @pytest.fixture
    def generator(self):
        sys.path.insert(0, '../producer')
        from producer import StockTickGenerator
        symbols = [
            {"symbol": "MSFT", "name": "Microsoft", "base_price": 420.0, "volatility": 0.015},
            {"symbol": "AAPL", "name": "Apple", "base_price": 195.0, "volatility": 0.012},
        ]
        return StockTickGenerator(symbols)

    def test_tick_has_all_fields(self, generator):
        tick = generator.generate_tick("MSFT")
        required = ['symbol', 'timestamp', 'timestamp_ms', 'sequence',
                    'price', 'bid', 'ask', 'spread', 'volume', 'volume_24h',
                    'change_pct', 'volatility']
        for field in required:
            assert field in tick, f"Missing field: {field}"

    def test_price_stays_positive(self, generator):
        for _ in range(1000):
            tick = generator.generate_tick("MSFT")
            assert tick['price'] > 0

    def test_bid_less_than_ask(self, generator):
        for _ in range(1000):
            tick = generator.generate_tick("AAPL")
            assert tick['bid'] <= tick['ask']

    def test_spread_equals_ask_minus_bid(self, generator):
        for _ in range(100):
            tick = generator.generate_tick("MSFT")
            expected = round(tick['ask'] - tick['bid'], 4)
            assert abs(tick['spread'] - expected) < 0.001

    def test_volume_positive(self, generator):
        for _ in range(100):
            tick = generator.generate_tick("MSFT")
            assert tick['volume'] >= 1

    def test_sequence_increments(self, generator):
        tick1 = generator.generate_tick("MSFT")
        tick2 = generator.generate_tick("MSFT")
        assert tick2['sequence'] > tick1['sequence']

    def test_mean_reversion_prevents_runaway(self, generator):
        """Price should not deviate too far from base over many ticks."""
        for _ in range(10000):
            generator.generate_tick("MSFT")
        final_price = generator.symbols["MSFT"]["price"]
        base_price = 420.0
        # After 10k ticks, price should stay within 50% of base
        assert final_price > base_price * 0.5
        assert final_price < base_price * 1.5


class TestEnvironmentParity:
    """Tests that ensure dev/test environments produce consistent results."""

    def test_config_loading(self):
        """Verify symbols.json can be loaded and parsed."""
        import json
        with open('../config/symbols.json', 'r') as f:
            config = json.load(f)

        assert 'symbols' in config
        assert len(config['symbols']) >= 20

        for s in config['symbols']:
            assert 'symbol' in s
            assert 'base_price' in s
            assert 'volatility' in s
            assert s['base_price'] > 0
            assert 0 < s['volatility'] < 1

    def test_all_symbols_have_valid_format(self):
        """All configured symbols match the contract pattern."""
        import json
        import re
        pattern = re.compile(r'^[A-Z.]{1,10}$')

        with open('../config/symbols.json', 'r') as f:
            config = json.load(f)

        for s in config['symbols']:
            assert pattern.match(s['symbol']), f"Invalid symbol: {s['symbol']}"
