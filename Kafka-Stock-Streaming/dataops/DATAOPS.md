# DataOps Framework

This document describes the complete DataOps implementation for the Stock Streaming Pipeline.

## DataOps Pillars Implemented

### 1. Schema Registry & Evolution

**Location:** `dataops/schema-registry/`

- Avro schema (`stock-tick-v1.avsc`) defines the canonical tick event format
- Compatibility config enforces BACKWARD compatibility on value schemas
- Schema Registry runs as a service (port 8081) and validates all schema changes
- Breaking changes require a new topic version + migration plan

**Usage:**
```bash
# Register a schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "..."}' \
  http://localhost:8081/subjects/stock-ticks-value/versions

# Check compatibility
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "..."}' \
  http://localhost:8081/compatibility/subjects/stock-ticks-value/versions/latest
```

### 2. Data Contracts

**Location:** `dataops/data-contracts/stock-tick-contract.yml`

Formal agreement between producer and consumers covering:
- Schema definition with field-level constraints
- SLA targets (availability, freshness, throughput, latency, completeness)
- Quality rules with severity levels and actions
- Change management process (RFC → PR → review → deploy)
- Versioning strategy (SemVer)

### 3. Data Quality (Soda Core)

**Location:** `dataops/data-quality/`

- `soda-checks.yml` — 20+ quality checks covering freshness, completeness, validity, uniqueness, anomaly detection
- `contract-validator.py` — Inline validation at the consumer level with Prometheus metrics
- `run-scan.sh` — Continuous scanning loop (every 5 minutes)
- Runs as Docker service (`data-quality-scanner`)

**Checks include:**
- Freshness < 30s
- No nulls in required fields
- Price range validation
- Bid < Ask invariant
- 25+ symbols producing data
- No duplicates
- Anomaly detection on row counts

### 4. Pipeline Orchestration (Airflow)

**Location:** `dataops/orchestration/`

Four DAGs manage data operations:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `stock_ohlcv_refresh` | Every 1 min | Refresh materialized views + update summary |
| `stock_data_quality` | Every 5 min | Run Soda scans, alert on failures |
| `stock_data_maintenance` | Daily 2 AM | Retention cleanup, VACUUM ANALYZE |
| `stock_sla_monitor` | Every 1 min | Check freshness, throughput, coverage SLAs |

**Start Airflow:**
```bash
docker compose -f docker-compose.yml -f dataops/orchestration/docker-compose.airflow.yml up -d
# Airflow UI: http://localhost:8081 (admin/admin)
```

### 5. SLA Monitoring & Alerting

**Location:** `dataops/alerting/`

- `prometheus-alerts.yml` — 14 alert rules organized by category:
  - Freshness SLA (warning at 10s, critical at 30s)
  - Throughput SLA (warning < 500 msg/s, critical < 100 msg/s)
  - Latency SLA (P99 > 100ms warning, > 500ms critical)
  - Consumer health (lag, service down)
  - Producer health (down, error rate)
  - Database health (slow writes, failures, connection loss)
  - Data quality (contract violations, compliance drop)

- `alertmanager.yml` — Routes alerts by severity to Slack channels and PagerDuty

### 6. Data Lineage

**Location:** `dataops/lineage/data-lineage.yml`

Documents the complete data flow:
```
GBM Generator → Kafka Producer → stock-ticks topic → Consumer → stock_ticks table
                                                                  ├→ ohlcv_1s view
                                                                  ├→ ohlcv_1m view
                                                                  └→ stock_summary
```

Quality checkpoints are documented at each stage with validators and actions.

### 7. Data Testing

**Location:** `dataops/testing/`

- `test_data_quality.py` — Unit tests for contract validator, tick generator, config loading
- `test_synthetic_data.py` — Deterministic test data generator (seeded for reproducibility)
- Tests run in CI (GitHub Actions) before every merge

### 8. Environment Parity

**Location:** `dataops/environments/`

Three environment configs with progressively stricter SLAs:

| Setting | Dev | Staging | Prod |
|---------|-----|---------|------|
| Interval | 10ms | 1ms | 1ms |
| Partitions | 3 | 6 | 12 |
| Freshness SLA | 60s | 30s | 5s |
| Throughput SLA | 50 msg/s | 500 msg/s | 1000 msg/s |
| Retention | 1 day | 3 days | 7 days |
| Scan interval | 10 min | 5 min | 1 min |
| Log level | DEBUG | INFO | WARNING |

## Quick Reference

```bash
# Start core pipeline + DataOps
make up

# Start with Airflow orchestration
docker compose -f docker-compose.yml -f dataops/orchestration/docker-compose.airflow.yml up -d

# Run data quality scan manually
docker exec data-quality-scanner soda scan -d stockdata -c /app/soda-configuration.yml /app/soda-checks.yml

# Check SLA alerts
curl http://localhost:9090/api/v1/alerts | jq

# View Schema Registry subjects
curl http://localhost:8081/subjects

# Run data tests
cd dataops/testing && pytest -v
```
