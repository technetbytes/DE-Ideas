"""
airflow/dags/inventory_etl_pipeline.py
───────────────────────────────────────
Daily ETL pipeline:  raw → staging → analytics refresh
Schedule: 02:00 UTC every day
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from utils.db import get_inventory_conn, log_pipeline_run

log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────

def _run_upsert(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
        return cur.rowcount


# ── Task functions ────────────────────────────────────────────

def extract_and_validate(**ctx):
    """
    Verify source data row counts and log the pipeline run.
    In production: replace with HTTP / file / CDC ingestion calls.
    """
    conn = get_inventory_conn()
    checks = {
        "raw.products":            "SELECT COUNT(*) FROM raw.products",
        "raw.suppliers":           "SELECT COUNT(*) FROM raw.suppliers",
        "raw.warehouses":          "SELECT COUNT(*) FROM raw.warehouses",
        "raw.inventory_snapshots": "SELECT COUNT(*) FROM raw.inventory_snapshots WHERE snapshot_date = CURRENT_DATE",
        "raw.sales_transactions":  "SELECT COUNT(*) FROM raw.sales_transactions  WHERE transaction_date = CURRENT_DATE",
    }
    total = 0
    with conn.cursor() as cur:
        for name, query in checks.items():
            cur.execute(query)
            count = cur.fetchone()[0]
            total += count
            log.info("Row count  %-40s : %d", name, count)
    log_pipeline_run(conn, ctx["dag"].dag_id, ctx["run_id"], ctx["task"].task_id, "SUCCESS", total)
    conn.close()


def transform_staging(**ctx):
    """Upsert cleaned records from raw → staging with business-rule validation."""
    conn  = get_inventory_conn()
    total = 0

    total += _run_upsert(conn, """
        INSERT INTO staging.products
            (product_id, product_name, category, sub_category, sku,
             unit_cost, unit_price, reorder_point, reorder_quantity,
             lead_time_days, supplier_id, is_active)
        SELECT product_id, product_name, category, sub_category, sku,
               unit_cost, unit_price, reorder_point, reorder_quantity,
               lead_time_days, supplier_id, is_active
        FROM raw.products
        WHERE unit_cost >= 0 AND unit_price >= 0
        ON CONFLICT (product_id) DO UPDATE SET
            product_name     = EXCLUDED.product_name,
            unit_cost        = EXCLUDED.unit_cost,
            unit_price       = EXCLUDED.unit_price,
            reorder_point    = EXCLUDED.reorder_point,
            reorder_quantity = EXCLUDED.reorder_quantity,
            updated_at       = NOW()
    """)

    total += _run_upsert(conn, """
        INSERT INTO staging.suppliers
            (supplier_id, supplier_name, country, city, contact_email, payment_terms, rating, is_active)
        SELECT supplier_id, supplier_name, country, city, contact_email, payment_terms, rating, is_active
        FROM raw.suppliers
        ON CONFLICT (supplier_id) DO UPDATE SET
            supplier_name = EXCLUDED.supplier_name,
            rating        = EXCLUDED.rating,
            updated_at    = NOW()
    """)

    total += _run_upsert(conn, """
        INSERT INTO staging.inventory_snapshots
            (snapshot_date, product_id, warehouse_id,
             quantity_on_hand, quantity_reserved, quantity_in_transit, unit_cost)
        SELECT s.snapshot_date, s.product_id, s.warehouse_id,
               GREATEST(0, s.quantity_on_hand),
               GREATEST(0, s.quantity_reserved),
               GREATEST(0, s.quantity_in_transit),
               s.unit_cost
        FROM raw.inventory_snapshots s
        WHERE s.snapshot_date = CURRENT_DATE
          AND EXISTS (SELECT 1 FROM staging.products  p WHERE p.product_id  = s.product_id)
          AND EXISTS (SELECT 1 FROM staging.warehouses w WHERE w.warehouse_id = s.warehouse_id)
        ON CONFLICT (snapshot_date, product_id, warehouse_id) DO UPDATE SET
            quantity_on_hand    = EXCLUDED.quantity_on_hand,
            quantity_reserved   = EXCLUDED.quantity_reserved,
            quantity_in_transit = EXCLUDED.quantity_in_transit,
            unit_cost           = EXCLUDED.unit_cost,
            updated_at          = NOW()
    """)

    total += _run_upsert(conn, """
        INSERT INTO staging.sales_transactions
            (transaction_id, transaction_date, product_id, warehouse_id,
             customer_id, quantity_sold, unit_price, discount_pct, channel)
        SELECT t.transaction_id, t.transaction_date, t.product_id, t.warehouse_id,
               t.customer_id, t.quantity_sold, t.unit_price, t.discount_pct, t.channel
        FROM raw.sales_transactions t
        WHERE t.transaction_date = CURRENT_DATE
          AND t.quantity_sold > 0
          AND EXISTS (SELECT 1 FROM staging.products p WHERE p.product_id = t.product_id)
        ON CONFLICT (transaction_id) DO NOTHING
    """)

    total += _run_upsert(conn, """
        INSERT INTO staging.purchase_orders
            (po_number, order_date, expected_date, received_date,
             product_id, supplier_id, warehouse_id,
             quantity_ordered, quantity_received, unit_cost, status)
        SELECT po.po_number, po.order_date, po.expected_date, po.received_date,
               po.product_id, po.supplier_id, po.warehouse_id,
               po.quantity_ordered, po.quantity_received, po.unit_cost, po.status
        FROM raw.purchase_orders po
        WHERE EXISTS (SELECT 1 FROM staging.products  p WHERE p.product_id  = po.product_id)
          AND EXISTS (SELECT 1 FROM staging.suppliers s WHERE s.supplier_id  = po.supplier_id)
        ON CONFLICT (po_number) DO UPDATE SET
            received_date     = EXCLUDED.received_date,
            quantity_received = EXCLUDED.quantity_received,
            status            = EXCLUDED.status,
            updated_at        = NOW()
    """)

    log_pipeline_run(conn, ctx["dag"].dag_id, ctx["run_id"], ctx["task"].task_id, "SUCCESS", total)
    conn.close()
    log.info("Staging transform complete — rows upserted: %d", total)


def refresh_daily_inventory_summary(**ctx):
    conn = get_inventory_conn()
    _run_upsert(conn, """
        INSERT INTO analytics.daily_inventory_summary
            (summary_date, product_id, product_name, category,
             warehouse_id, warehouse_name,
             quantity_on_hand, quantity_available, quantity_in_transit,
             inventory_value, reorder_point, reorder_quantity,
             is_below_reorder, days_of_supply)
        SELECT
            s.snapshot_date, s.product_id, p.product_name, p.category,
            s.warehouse_id, w.warehouse_name,
            s.quantity_on_hand, s.quantity_available, s.quantity_in_transit,
            s.inventory_value, p.reorder_point, p.reorder_quantity,
            s.quantity_on_hand < p.reorder_point,
            CASE WHEN COALESCE(av.avg_qty, 0) > 0
                 THEN ROUND(s.quantity_on_hand::NUMERIC / av.avg_qty, 1)
                 ELSE NULL END
        FROM staging.inventory_snapshots s
        JOIN staging.products   p  ON p.product_id   = s.product_id
        JOIN staging.warehouses w  ON w.warehouse_id = s.warehouse_id
        LEFT JOIN LATERAL (
            SELECT ROUND(AVG(quantity_sold)::NUMERIC, 2) AS avg_qty
            FROM staging.sales_transactions st
            WHERE st.product_id = s.product_id
              AND st.transaction_date BETWEEN s.snapshot_date - 30 AND s.snapshot_date
        ) av ON TRUE
        WHERE s.snapshot_date = CURRENT_DATE
        ON CONFLICT (summary_date, product_id, warehouse_id) DO UPDATE SET
            quantity_on_hand   = EXCLUDED.quantity_on_hand,
            quantity_available = EXCLUDED.quantity_available,
            inventory_value    = EXCLUDED.inventory_value,
            is_below_reorder   = EXCLUDED.is_below_reorder,
            days_of_supply     = EXCLUDED.days_of_supply,
            updated_at         = NOW()
    """)
    conn.close()
    log.info("daily_inventory_summary refreshed.")


def refresh_sales_metrics(**ctx):
    conn = get_inventory_conn()
    _run_upsert(conn, """
        INSERT INTO analytics.product_sales_metrics
            (metric_date, product_id, product_name, category,
             total_quantity_sold, total_revenue, avg_selling_price,
             transaction_count, avg_daily_sales_30d, avg_daily_sales_7d)
        SELECT
            t.transaction_date, t.product_id, p.product_name, p.category,
            SUM(t.quantity_sold), SUM(t.revenue), ROUND(AVG(t.unit_price), 2), COUNT(*),
            ROUND((SELECT AVG(dq) FROM (
                SELECT SUM(quantity_sold) AS dq
                FROM staging.sales_transactions t2
                WHERE t2.product_id = t.product_id
                  AND t2.transaction_date BETWEEN t.transaction_date - 30 AND t.transaction_date
                GROUP BY transaction_date) d30)::NUMERIC, 2),
            ROUND((SELECT AVG(dq) FROM (
                SELECT SUM(quantity_sold) AS dq
                FROM staging.sales_transactions t2
                WHERE t2.product_id = t.product_id
                  AND t2.transaction_date BETWEEN t.transaction_date - 7 AND t.transaction_date
                GROUP BY transaction_date) d7)::NUMERIC, 2)
        FROM staging.sales_transactions t
        JOIN staging.products p ON p.product_id = t.product_id
        WHERE t.transaction_date = CURRENT_DATE
        GROUP BY t.transaction_date, t.product_id, p.product_name, p.category
        ON CONFLICT (metric_date, product_id) DO UPDATE SET
            total_quantity_sold = EXCLUDED.total_quantity_sold,
            total_revenue       = EXCLUDED.total_revenue,
            avg_daily_sales_30d = EXCLUDED.avg_daily_sales_30d,
            updated_at          = NOW()
    """)
    conn.close()
    log.info("product_sales_metrics refreshed.")


def refresh_reorder_alerts(**ctx):
    conn = get_inventory_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE analytics.reorder_alerts SET is_resolved = TRUE WHERE alert_date < CURRENT_DATE AND is_resolved = FALSE")
        conn.commit()
    _run_upsert(conn, """
        INSERT INTO analytics.reorder_alerts
            (alert_date, product_id, product_name, category,
             warehouse_id, warehouse_name,
             quantity_on_hand, reorder_point, shortage_quantity, recommended_order_qty,
             supplier_id, supplier_name, lead_time_days, urgency)
        SELECT CURRENT_DATE, s.product_id, p.product_name, p.category,
               s.warehouse_id, w.warehouse_name,
               s.quantity_on_hand, p.reorder_point,
               p.reorder_point - s.quantity_on_hand, p.reorder_quantity,
               p.supplier_id, sup.supplier_name, p.lead_time_days,
               CASE
                   WHEN s.quantity_on_hand = 0                      THEN 'CRITICAL'
                   WHEN s.quantity_on_hand < p.reorder_point * 0.5  THEN 'HIGH'
                   ELSE 'MEDIUM'
               END
        FROM staging.inventory_snapshots s
        JOIN staging.products   p   ON p.product_id   = s.product_id
        JOIN staging.warehouses w   ON w.warehouse_id = s.warehouse_id
        LEFT JOIN staging.suppliers sup ON sup.supplier_id = p.supplier_id
        WHERE s.snapshot_date = CURRENT_DATE
          AND s.quantity_on_hand < p.reorder_point
    """)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.reorder_alerts WHERE alert_date = CURRENT_DATE")
        log.info("Reorder alerts today: %d", cur.fetchone()[0])
    conn.close()


def refresh_abc_classification(**ctx):
    conn = get_inventory_conn()
    _run_upsert(conn, """
        INSERT INTO analytics.abc_classification
            (classified_at, product_id, product_name, category,
             total_revenue_12m, revenue_pct, cumulative_revenue_pct, abc_class)
        WITH rev AS (
            SELECT t.product_id, p.product_name, p.category, SUM(t.revenue) AS total_rev
            FROM staging.sales_transactions t
            JOIN staging.products p ON p.product_id = t.product_id
            WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY t.product_id, p.product_name, p.category
        ),
        pcts AS (
            SELECT *,
                ROUND(total_rev / SUM(total_rev) OVER () * 100, 2) AS rev_pct,
                ROUND(SUM(total_rev) OVER (ORDER BY total_rev DESC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                      / SUM(total_rev) OVER () * 100, 2) AS cum_pct
            FROM rev
        )
        SELECT CURRENT_DATE, product_id, product_name, category,
               ROUND(total_rev, 2), rev_pct, cum_pct,
               CASE WHEN cum_pct <= 70 THEN 'A' WHEN cum_pct <= 90 THEN 'B' ELSE 'C' END
        FROM pcts
        ON CONFLICT (classified_at, product_id) DO NOTHING
    """)
    conn.close()
    log.info("ABC classification refreshed.")


def refresh_supplier_performance(**ctx):
    conn = get_inventory_conn()
    _run_upsert(conn, """
        INSERT INTO analytics.supplier_performance
            (year_month, supplier_id, supplier_name,
             total_orders, on_time_orders, late_orders, on_time_pct,
             avg_days_late, total_order_value, fulfillment_rate, supplier_rating)
        SELECT
            TO_CHAR(po.order_date, 'YYYY-MM'), po.supplier_id, sup.supplier_name,
            COUNT(*),
            SUM(CASE WHEN COALESCE(po.days_late, 0) <= 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN COALESCE(po.days_late, 0) > 0  THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN COALESCE(po.days_late, 0) <= 0 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2),
            ROUND(AVG(CASE WHEN po.days_late > 0 THEN po.days_late END)::NUMERIC, 1),
            ROUND(SUM(po.total_cost), 2),
            ROUND(SUM(po.quantity_received)::NUMERIC / NULLIF(SUM(po.quantity_ordered), 0) * 100, 2),
            sup.rating
        FROM staging.purchase_orders po
        JOIN staging.suppliers sup ON sup.supplier_id = po.supplier_id
        WHERE po.status = 'COMPLETE'
          AND TO_CHAR(po.order_date, 'YYYY-MM') = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
        GROUP BY TO_CHAR(po.order_date, 'YYYY-MM'), po.supplier_id, sup.supplier_name, sup.rating
        ON CONFLICT (year_month, supplier_id) DO UPDATE SET
            total_orders      = EXCLUDED.total_orders,
            on_time_pct       = EXCLUDED.on_time_pct,
            total_order_value = EXCLUDED.total_order_value,
            fulfillment_rate  = EXCLUDED.fulfillment_rate,
            updated_at        = NOW()
    """)
    conn.close()
    log.info("Supplier performance refreshed.")


# ── DAG definition ────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="inventory_etl_pipeline",
    description="Daily inventory ETL: raw → staging → analytics",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["inventory", "etl", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_extract   = PythonOperator(task_id="extract_and_validate", python_callable=extract_and_validate)
    t_transform = PythonOperator(task_id="transform_staging",    python_callable=transform_staging)

    with TaskGroup("refresh_analytics") as analytics_group:
        PythonOperator(task_id="daily_inventory_summary", python_callable=refresh_daily_inventory_summary)
        PythonOperator(task_id="product_sales_metrics",   python_callable=refresh_sales_metrics)
        PythonOperator(task_id="reorder_alerts",          python_callable=refresh_reorder_alerts)
        PythonOperator(task_id="abc_classification",      python_callable=refresh_abc_classification)
        PythonOperator(task_id="supplier_performance",    python_callable=refresh_supplier_performance)

    start >> t_extract >> t_transform >> analytics_group >> end
