"""
airflow/dags/inventory_weekly_analytics.py
───────────────────────────────────────────
Weekly analytics refresh + data quality checks.
Schedule: 03:00 UTC every Monday
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from utils.db import get_inventory_conn

log = logging.getLogger(__name__)


# ── Task functions ────────────────────────────────────────────

def data_quality_check(**ctx):
    """
    Run basic data-quality assertions.
    Raises ValueError on failure so Airflow marks the task red.
    """
    conn = get_inventory_conn()
    failures = []

    presence_checks = {
        "staging.products row count":   "SELECT COUNT(*) FROM staging.products",
        "staging.suppliers row count":  "SELECT COUNT(*) FROM staging.suppliers",
        "analytics summary has today":  "SELECT COUNT(*) FROM analytics.daily_inventory_summary WHERE summary_date = CURRENT_DATE",
    }
    zero_checks = {
        "negative inventory rows":   "SELECT COUNT(*) FROM staging.inventory_snapshots WHERE quantity_on_hand < 0",
        "zero/negative price sales": "SELECT COUNT(*) FROM staging.sales_transactions WHERE unit_price <= 0",
        "orphan sales (no product)": """
            SELECT COUNT(*) FROM staging.sales_transactions t
            WHERE NOT EXISTS (SELECT 1 FROM staging.products p WHERE p.product_id = t.product_id)
        """,
    }

    with conn.cursor() as cur:
        for name, query in presence_checks.items():
            cur.execute(query)
            result = cur.fetchone()[0]
            log.info("DQ [presence] %-45s = %d", name, result)
            if result == 0:
                failures.append(f"FAIL: {name} returned 0 rows")

        for name, query in zero_checks.items():
            cur.execute(query)
            result = cur.fetchone()[0]
            log.info("DQ [zero]     %-45s = %d", name, result)
            if result > 0:
                failures.append(f"FAIL: {name} found {result} bad rows")

    conn.close()

    if failures:
        raise ValueError("Data quality failures detected:\n" + "\n".join(failures))
    log.info("All data quality checks passed.")


def refresh_inventory_turnover(**ctx):
    conn = get_inventory_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO analytics.inventory_turnover
                (year_month, product_id, product_name, category,
                 cogs, avg_inventory_value, turnover_ratio, days_inventory_outstanding)
            SELECT
                TO_CHAR(t.transaction_date, 'YYYY-MM'),
                t.product_id, p.product_name, p.category,
                ROUND(SUM(t.quantity_sold * p.unit_cost), 2),
                ROUND(AVG(s.inventory_value), 2),
                CASE WHEN AVG(s.inventory_value) > 0
                     THEN ROUND(SUM(t.quantity_sold * p.unit_cost) / AVG(s.inventory_value), 2)
                     ELSE NULL END,
                CASE WHEN SUM(t.quantity_sold * p.unit_cost) > 0 AND AVG(s.inventory_value) > 0
                     THEN ROUND(30 / (SUM(t.quantity_sold * p.unit_cost) / AVG(s.inventory_value)), 1)
                     ELSE NULL END
            FROM staging.sales_transactions t
            JOIN staging.products p ON p.product_id = t.product_id
            LEFT JOIN staging.inventory_snapshots s
                   ON s.product_id   = t.product_id
                  AND s.snapshot_date = t.transaction_date
            WHERE TO_CHAR(t.transaction_date, 'YYYY-MM')
                  = TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYY-MM')
            GROUP BY TO_CHAR(t.transaction_date, 'YYYY-MM'), t.product_id, p.product_name, p.category
            ON CONFLICT (year_month, product_id) DO UPDATE SET
                cogs                       = EXCLUDED.cogs,
                avg_inventory_value        = EXCLUDED.avg_inventory_value,
                turnover_ratio             = EXCLUDED.turnover_ratio,
                days_inventory_outstanding = EXCLUDED.days_inventory_outstanding,
                updated_at                 = NOW()
        """)
        conn.commit()
    conn.close()
    log.info("Inventory turnover refreshed.")


def refresh_category_summary(**ctx):
    conn = get_inventory_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO analytics.category_summary
                (week_start, category, total_inventory_value,
                 total_quantity_on_hand, total_revenue,
                 product_count, items_below_reorder)
            SELECT
                DATE_TRUNC('week', s.snapshot_date)::DATE,
                p.category,
                ROUND(SUM(s.inventory_value), 2),
                SUM(s.quantity_on_hand),
                ROUND(COALESCE(SUM(t.revenue), 0), 2),
                COUNT(DISTINCT s.product_id),
                SUM(CASE WHEN s.quantity_on_hand < p.reorder_point THEN 1 ELSE 0 END)
            FROM staging.inventory_snapshots s
            JOIN staging.products p ON p.product_id = s.product_id
            LEFT JOIN staging.sales_transactions t
                   ON t.product_id      = s.product_id
                  AND t.transaction_date = s.snapshot_date
            WHERE s.snapshot_date >= CURRENT_DATE - INTERVAL '28 days'
            GROUP BY DATE_TRUNC('week', s.snapshot_date)::DATE, p.category
            ON CONFLICT (week_start, category) DO UPDATE SET
                total_inventory_value  = EXCLUDED.total_inventory_value,
                total_quantity_on_hand = EXCLUDED.total_quantity_on_hand,
                total_revenue          = EXCLUDED.total_revenue,
                items_below_reorder    = EXCLUDED.items_below_reorder,
                updated_at             = NOW()
        """)
        conn.commit()
    conn.close()
    log.info("Category summary refreshed.")


# ── DAG definition ────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="inventory_weekly_analytics",
    description="Weekly analytics: turnover, category summary, DQ checks",
    schedule_interval="0 3 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["inventory", "analytics", "weekly"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_dq       = PythonOperator(task_id="data_quality_check",       python_callable=data_quality_check)
    t_turnover = PythonOperator(task_id="refresh_inventory_turnover", python_callable=refresh_inventory_turnover)
    t_category = PythonOperator(task_id="refresh_category_summary",  python_callable=refresh_category_summary)

    start >> t_dq >> [t_turnover, t_category] >> end
