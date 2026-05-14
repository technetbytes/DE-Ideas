# Inventory Analytics — Data Engineering Project

> **Stack**: Apache Airflow 2.9 · PostgreSQL 15 · Docker Compose · Metabase 0.49

---

## Project structure

```
inventory-analytics/
│
├── airflow/                        # Airflow application
│   ├── dags/
│   │   ├── inventory_etl_pipeline.py       # Daily ETL (02:00 UTC)
│   │   ├── inventory_weekly_analytics.py   # Weekly analytics (Mon 03:00 UTC)
│   │   └── utils/
│   │       ├── __init__.py
│   │       └── db.py                       # Shared DB helpers
│   ├── plugins/
│   │   └── __init__.py                     # Custom operators go here
│   ├── config/
│   │   └── airflow.cfg                     # Airflow config overrides
│   └── logs/                               # Volume-mounted at runtime
│
├── database/                       # PostgreSQL
│   ├── ddl/
│   │   ├── 01_schemas.sql                  # raw / staging / analytics schemas
│   │   ├── 02_raw_tables.sql               # Landing zone tables
│   │   ├── 03_staging_tables.sql           # Cleaned tables + generated columns
│   │   └── 04_analytics_tables.sql         # KPI & reporting tables
│   ├── seeds/
│   │   ├── 01_sample_dimensions.sql        # Products, suppliers, warehouses
│   │   ├── 02_sample_transactions.sql      # 90 days inventory + sales
│   │   └── 03_seed_analytics.sql           # Pre-populate analytics layer
│   └── migrations/
│       └── README.md                       # Future schema changes
│
├── docker/
│   └── init_postgres.sh                    # Docker entrypoint: create DBs + run DDL
│
├── metabase/
│   └── dashboards/
│       └── README.md                       # Dashboard setup guide
│
├── tests/
│   ├── dags/
│   │   └── test_dag_integrity.py           # DAG import + structure tests
│   └── sql/
│       └── test_schema_queries.sql         # Smoke-test queries
│
├── docs/
│   └── architecture.md                     # Architecture overview
│
├── .github/
│   └── workflows/
│       └── ci.yml                          # GitHub Actions: lint + DAG tests
│
├── docker-compose.yml
├── .env.example                            # Copy to .env and fill in secrets
├── .gitignore
└── README.md
```

---

## Quick start

### Prerequisites
- Docker Desktop ≥ 4.x (Compose V2)
- 4 GB RAM available

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env if you want to change passwords (optional for local dev)
```

### 2. Start all services

```bash
chmod +x docker/init_postgres.sh
docker compose up -d
```

First boot takes ~2-3 minutes. PostgreSQL runs all DDL and seed scripts automatically.

### 3. Verify services are healthy

```bash
docker compose ps
```

All five containers should show `healthy` or `running`.

### 4. Access services

| Service    | URL                    | Credentials       |
|------------|------------------------|-------------------|
| Airflow UI | http://localhost:8080  | admin / admin     |
| Metabase   | http://localhost:3000  | setup on first visit |
| PostgreSQL | localhost:5432         | inventory / inventory |

### 5. Connect Metabase to PostgreSQL

On first visit to http://localhost:3000, use the setup wizard:
- Database type: **PostgreSQL**
- Host: `postgres` · Port: `5432` · Database: `inventory`
- Username: `inventory` · Password: `inventory`

---

## Running tests

```bash
# DAG integrity tests (requires airflow installed locally)
pip install apache-airflow==2.9.1 psycopg2-binary pytest
pytest tests/dags/ -v

# SQL smoke tests (requires running postgres container)
docker exec -i inventory_postgres \
  psql -U inventory -d inventory \
  < tests/sql/test_schema_queries.sql
```

---

## Common commands

```bash
# View logs for a service
docker compose logs -f airflow-scheduler

# Trigger a DAG run manually
docker exec inventory_airflow_scheduler \
  airflow dags trigger inventory_etl_pipeline

# Connect to the inventory database
docker exec -it inventory_postgres \
  psql -U inventory -d inventory

# Stop everything
docker compose down

# Full reset (deletes all data volumes)
docker compose down -v
```

---

## Key SQL queries

```sql
-- Current reorder alerts
SELECT product_name, warehouse_name, quantity_on_hand, reorder_point, urgency
FROM analytics.reorder_alerts
WHERE alert_date = CURRENT_DATE AND is_resolved = FALSE
ORDER BY urgency, shortage_quantity DESC;

-- Top 10 products by revenue (last 30 days)
SELECT product_name, SUM(total_revenue) AS revenue
FROM analytics.product_sales_metrics
WHERE metric_date >= CURRENT_DATE - 30
GROUP BY product_name ORDER BY revenue DESC LIMIT 10;

-- ABC class summary
SELECT abc_class, COUNT(*) AS products, ROUND(SUM(total_revenue_12m), 2) AS revenue
FROM analytics.abc_classification
WHERE classified_at = CURRENT_DATE
GROUP BY abc_class ORDER BY abc_class;

-- Supplier on-time rate
SELECT supplier_name, on_time_pct, fulfillment_rate
FROM analytics.supplier_performance
ORDER BY on_time_pct DESC;
```

---

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full data-flow diagram.

---

## Production notes

- Swap the `generate_series` seed data for real ERP/POS/WMS connectors in the DAG extract step
- Use Airflow Connections UI to manage credentials (not env vars)
- Enable Airflow email/Slack alerting on DAG failure
- Add Flyway or Liquibase for managed migrations
- Enable Metabase SSO/LDAP for multi-user access
