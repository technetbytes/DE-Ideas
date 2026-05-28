"""
Data Contract Validator
Validates incoming Kafka messages against the data contract specification.
Runs inline in the consumer or as a standalone validation service.
"""

import os
import re
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("contract-validator")

# Metrics
contract_violations = Counter(
    'dataops_contract_violations_total',
    'Total data contract violations detected',
    ['rule_name', 'severity', 'symbol']
)

contract_checks_total = Counter(
    'dataops_contract_checks_total',
    'Total contract validation checks performed',
    ['status']
)

contract_compliance_pct = Gauge(
    'dataops_contract_compliance_percent',
    'Current contract compliance percentage'
)


class DataContractValidator:
    """Validates stock tick messages against the data contract."""

    VALID_SYMBOL_PATTERN = re.compile(r'^[A-Z.]{1,10}$')

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.total_checks = 0
        self.total_violations = 0
        self._symbol_last_sequence: Dict[str, int] = {}
        self._symbol_last_volume_24h: Dict[str, int] = {}

    def validate(self, tick: Dict) -> Tuple[bool, List[Dict]]:
        """
        Validate a tick against all contract rules.
        Returns (is_valid, list_of_violations).
        """
        violations = []

        # Required fields check
        required_fields = ['symbol', 'timestamp', 'timestamp_ms', 'sequence',
                           'price', 'bid', 'ask', 'spread', 'volume', 'volume_24h']
        for field in required_fields:
            if field not in tick or tick[field] is None:
                violations.append({
                    'rule': 'required_field',
                    'severity': 'critical',
                    'message': f"Required field '{field}' is missing or null",
                    'field': field,
                })

        if violations:
            self._record_violations(tick, violations)
            return False, violations

        # Symbol format
        if not self.VALID_SYMBOL_PATTERN.match(tick['symbol']):
            violations.append({
                'rule': 'symbol_format',
                'severity': 'critical',
                'message': f"Symbol '{tick['symbol']}' does not match pattern ^[A-Z.]{{1,10}}$",
            })

        # Price sanity
        price = tick['price']
        if price <= 0 or price >= 100000:
            violations.append({
                'rule': 'price_range',
                'severity': 'critical',
                'message': f"Price {price} outside valid range (0, 100000)",
            })

        # Bid/Ask validity
        bid, ask = tick['bid'], tick['ask']
        if bid > ask:
            violations.append({
                'rule': 'bid_ask_crossing',
                'severity': 'critical',
                'message': f"Bid ({bid}) > Ask ({ask}) — crossed market",
            })

        if bid > price * 1.001:  # Allow tiny float tolerance
            violations.append({
                'rule': 'bid_above_price',
                'severity': 'warning',
                'message': f"Bid ({bid}) significantly above price ({price})",
            })

        if ask < price * 0.999:
            violations.append({
                'rule': 'ask_below_price',
                'severity': 'warning',
                'message': f"Ask ({ask}) significantly below price ({price})",
            })

        # Spread validation
        expected_spread = round(ask - bid, 4)
        actual_spread = tick['spread']
        if abs(actual_spread - expected_spread) > 0.01:
            violations.append({
                'rule': 'spread_calculation',
                'severity': 'warning',
                'message': f"Spread {actual_spread} != ask-bid ({expected_spread})",
            })

        # Spread as percentage of price
        if price > 0 and (actual_spread / price) > 0.01:
            violations.append({
                'rule': 'spread_max_pct',
                'severity': 'warning',
                'message': f"Spread is {(actual_spread/price)*100:.2f}% of price (max 1%)",
            })

        # Volume check
        if tick['volume'] < 1 or tick['volume'] > 1000000:
            violations.append({
                'rule': 'volume_range',
                'severity': 'warning',
                'message': f"Volume {tick['volume']} outside expected range [1, 1000000]",
            })

        # Timestamp freshness (within 5 seconds)
        now_ms = int(time.time() * 1000)
        age_ms = now_ms - tick['timestamp_ms']
        if age_ms > 5000:
            violations.append({
                'rule': 'timestamp_freshness',
                'severity': 'warning',
                'message': f"Message is {age_ms}ms old (max 5000ms)",
            })

        if age_ms < -1000:  # Future timestamp (1s tolerance for clock drift)
            violations.append({
                'rule': 'timestamp_future',
                'severity': 'critical',
                'message': f"Message timestamp is {-age_ms}ms in the future",
            })

        # Sequence monotonicity per symbol
        symbol = tick['symbol']
        seq = tick['sequence']
        if symbol in self._symbol_last_sequence:
            if seq <= self._symbol_last_sequence[symbol]:
                violations.append({
                    'rule': 'sequence_monotonic',
                    'severity': 'warning',
                    'message': f"Sequence {seq} <= previous {self._symbol_last_sequence[symbol]}",
                })
        self._symbol_last_sequence[symbol] = seq

        # Volume 24h monotonicity
        vol_24h = tick['volume_24h']
        if symbol in self._symbol_last_volume_24h:
            if vol_24h < self._symbol_last_volume_24h[symbol]:
                violations.append({
                    'rule': 'volume_24h_monotonic',
                    'severity': 'warning',
                    'message': f"volume_24h decreased: {vol_24h} < {self._symbol_last_volume_24h[symbol]}",
                })
        self._symbol_last_volume_24h[symbol] = vol_24h

        # Change percentage bounds
        if tick.get('change_pct') is not None:
            if tick['change_pct'] < -50 or tick['change_pct'] > 100:
                violations.append({
                    'rule': 'change_pct_bounds',
                    'severity': 'critical',
                    'message': f"change_pct {tick['change_pct']} outside [-50, 100]",
                })

        # Record metrics
        self._record_violations(tick, violations)

        is_valid = not any(v['severity'] == 'critical' for v in violations)
        return is_valid, violations

    def _record_violations(self, tick: Dict, violations: List[Dict]):
        """Record violations in Prometheus metrics."""
        self.total_checks += 1
        symbol = tick.get('symbol', 'unknown')

        if violations:
            self.total_violations += len(violations)
            for v in violations:
                contract_violations.labels(
                    rule_name=v['rule'],
                    severity=v['severity'],
                    symbol=symbol
                ).inc()
            contract_checks_total.labels(status='violation').inc()
        else:
            contract_checks_total.labels(status='pass').inc()

        # Update compliance gauge
        if self.total_checks > 0:
            compliance = ((self.total_checks - self.total_violations) / self.total_checks) * 100
            contract_compliance_pct.set(max(compliance, 0))

    def get_stats(self) -> Dict:
        """Return validation statistics."""
        compliance = 0
        if self.total_checks > 0:
            compliance = ((self.total_checks - self.total_violations) / self.total_checks) * 100

        return {
            'total_checks': self.total_checks,
            'total_violations': self.total_violations,
            'compliance_pct': round(compliance, 2),
            'symbols_tracked': len(self._symbol_last_sequence),
        }


if __name__ == "__main__":
    # Standalone test mode
    start_http_server(8002)
    validator = DataContractValidator()

    # Example valid tick
    test_tick = {
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

    is_valid, violations = validator.validate(test_tick)
    print(f"Valid: {is_valid}, Violations: {len(violations)}")
    for v in violations:
        print(f"  [{v['severity']}] {v['rule']}: {v['message']}")

    print(f"\nStats: {validator.get_stats()}")
