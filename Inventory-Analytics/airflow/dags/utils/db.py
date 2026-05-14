"""
airflow/dags/utils/db.py
────────────────────────
Shared database utilities for all inventory DAGs.
"""

from __future__ import annotations

import os
import logging
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def get_inventory_conn() -> psycopg2.extensions.connection:
    """Return a psycopg2 connection to the inventory data warehouse."""
    return psycopg2.connect(
        host=os.getenv("INVENTORY_DB_HOST", "postgres"),
        port=int(os.getenv("INVENTORY_DB_PORT", 5432)),
        dbname=os.getenv("INVENTORY_DB_NAME", "inventory"),
        user=os.getenv("INVENTORY_DB_USER", "inventory"),
        password=os.getenv("INVENTORY_DB_PASSWORD", "inventory"),
    )


def log_pipeline_run(
    conn,
    dag_id: str,
    run_id: str,
    task_id: str,
    status: str,
    rows_processed: int = 0,
    error_message: str | None = None,
) -> None:
    """Insert a row into raw.pipeline_runs for observability."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.pipeline_runs
                (dag_id, run_id, task_id, status, rows_processed, error_message,
                 started_at, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            """,
            (dag_id, run_id, task_id, status, rows_processed, error_message),
        )
    conn.commit()
    log.info("Pipeline run logged: %s / %s  status=%s  rows=%d", dag_id, task_id, status, rows_processed)


def execute_sql(conn, sql: str, params=None) -> int:
    """Execute a DML statement and return rowcount."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()
        return cur.rowcount
