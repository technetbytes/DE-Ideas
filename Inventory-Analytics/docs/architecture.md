# Architecture

## Data flow

```
Source Systems
  ├── ERP          → products, purchase orders
  ├── POS / Sales  → sales transactions
  ├── Supplier API → pricing, lead times
  └── WMS          → inventory snapshots

         ↓  (Airflow DAG: inventory_etl_pipeline, daily 02:00 UTC)

raw schema          ← raw landing zone, append-only
         ↓  extract_and_validate → transform_staging
staging schema      ← cleaned, FK-validated, computed columns
         ↓  refresh_analytics (parallel tasks)
analytics schema    ← aggregated KPIs, Metabase-ready

         ↓  (Metabase reads analytics.*)
Dashboards & Alerts
```

## Schema layers

| Schema    | Role             | Key rules                                      |
|-----------|------------------|------------------------------------------------|
| `raw`     | Landing zone     | No deletions. Source data as-is.               |
| `staging` | Clean layer      | FK constraints, CHECK constraints, generated cols |
| `analytics` | Reporting     | Pre-aggregated, indexed for BI queries         |

## Airflow DAGs

| DAG                          | Schedule        | Tasks                                    |
|------------------------------|-----------------|------------------------------------------|
| `inventory_etl_pipeline`     | Daily 02:00 UTC | extract → transform → 5× analytics       |
| `inventory_weekly_analytics` | Monday 03:00 UTC | DQ check → turnover + category summary  |

## Networking

All services share the `inventory_net` Docker bridge network.
Service discovery uses Docker DNS (`postgres`, `metabase`, etc.).

## Ports

| Service         | Host port | Container port |
|-----------------|-----------|----------------|
| PostgreSQL      | 5432      | 5432           |
| Airflow         | 8080      | 8080           |
| Metabase        | 3000      | 3000           |
