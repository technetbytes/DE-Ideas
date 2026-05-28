"""
Stock Pipeline Orchestration DAG
Manages periodic data operations: view refresh, quality checks, retention, SLA monitoring.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}


# ═══════════════════════════════════════════════════════════════════════════════
# DAG 1: Materialized View Refresh (Every minute)
# ═══════════════════════════════════════════════════════════════════════════════
with DAG(
    'stock_ohlcv_refresh',
    default_args=default_args,
    description='Refresh OHLCV materialized views for real-time dashboards',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['stock', 'dataops', 'views'],
    max_active_runs=1,
) as dag_refresh:

    refresh_1s_view = PostgresOperator(
        task_id='refresh_ohlcv_1s',
        postgres_conn_id='stockdata',
        sql='REFRESH MATERIALIZED VIEW CONCURRENTLY stock_ohlcv_1s;',
    )

    refresh_1m_view = PostgresOperator(
        task_id='refresh_ohlcv_1m',
        postgres_conn_id='stockdata',
        sql='REFRESH MATERIALIZED VIEW CONCURRENTLY stock_ohlcv_1m;',
    )

    update_summary = PostgresOperator(
        task_id='update_stock_summary',
        postgres_conn_id='stockdata',
        sql="""
            INSERT INTO stock_summary (symbol, last_price, last_updated, high_24h, low_24h, volume_24h, tick_count_24h, avg_spread)
            SELECT
                symbol,
                (array_agg(price ORDER BY timestamp DESC))[1] as last_price,
                MAX(timestamp) as last_updated,
                MAX(price) as high_24h,
                MIN(price) as low_24h,
                SUM(volume) as volume_24h,
                COUNT(*) as tick_count_24h,
                AVG(spread) as avg_spread
            FROM stock_ticks
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY symbol
            ON CONFLICT (symbol) DO UPDATE SET
                last_price = EXCLUDED.last_price,
                last_updated = EXCLUDED.last_updated,
                high_24h = EXCLUDED.high_24h,
                low_24h = EXCLUDED.low_24h,
                volume_24h = EXCLUDED.volume_24h,
                tick_count_24h = EXCLUDED.tick_count_24h,
                avg_spread = EXCLUDED.avg_spread;
        """,
    )

    refresh_1s_view >> refresh_1m_view >> update_summary


# ═══════════════════════════════════════════════════════════════════════════════
# DAG 2: Data Quality Checks (Every 5 minutes)
# ═══════════════════════════════════════════════════════════════════════════════
with DAG(
    'stock_data_quality',
    default_args=default_args,
    description='Run Soda Core data quality checks against stock_ticks',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['stock', 'dataops', 'quality'],
    max_active_runs=1,
) as dag_quality:

    run_soda_scan = BashOperator(
        task_id='run_soda_checks',
        bash_command="""
            cd /opt/airflow/dags/dataops/data-quality && \
            soda scan -d stockdata \
                -c soda-configuration.yml \
                soda-checks.yml \
                --verbose
        """,
    )

    def check_soda_results(**context):
        """Evaluate Soda scan results and decide on alerting."""
        ti = context['ti']
        return_code = ti.xcom_pull(task_ids='run_soda_checks', key='return_value')
        if return_code and return_code > 1:
            raise Exception(f"Critical data quality failures detected (exit code: {return_code})")

    evaluate_results = PythonOperator(
        task_id='evaluate_quality_results',
        python_callable=check_soda_results,
    )

    alert_on_failure = EmailOperator(
        task_id='alert_quality_failure',
        to='data-eng@company.com',
        subject='[ALERT] Stock Pipeline Data Quality Failure',
        html_content="""
            <h3>Data Quality Check Failed</h3>
            <p>Soda Core detected critical violations in the stock_ticks table.</p>
            <p>Check Grafana dashboard for details: <a href="http://localhost:3000">Dashboard</a></p>
            <p>Execution: {{ ds }}</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    run_soda_scan >> evaluate_results >> alert_on_failure


# ═══════════════════════════════════════════════════════════════════════════════
# DAG 3: Data Retention & Maintenance (Daily at 2 AM)
# ═══════════════════════════════════════════════════════════════════════════════
with DAG(
    'stock_data_maintenance',
    default_args=default_args,
    description='Data retention, cleanup, and database maintenance',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['stock', 'dataops', 'maintenance'],
) as dag_maintenance:

    cleanup_old_ticks = PostgresOperator(
        task_id='cleanup_old_ticks',
        postgres_conn_id='stockdata',
        sql="""
            DELETE FROM stock_ticks
            WHERE timestamp < NOW() - INTERVAL '7 days';
        """,
    )

    vacuum_analyze = PostgresOperator(
        task_id='vacuum_analyze',
        postgres_conn_id='stockdata',
        sql="""
            VACUUM ANALYZE stock_ticks;
            VACUUM ANALYZE stock_summary;
        """,
        autocommit=True,
    )

    def log_retention_stats(**context):
        """Log data retention statistics."""
        hook = PostgresHook(postgres_conn_id='stockdata')
        result = hook.get_records("""
            SELECT
                COUNT(*) as total_rows,
                MIN(timestamp) as oldest,
                MAX(timestamp) as newest,
                pg_size_pretty(pg_total_relation_size('stock_ticks')) as table_size
            FROM stock_ticks;
        """)
        if result:
            row = result[0]
            print(f"Retention stats: {row[0]} rows | Oldest: {row[1]} | Newest: {row[2]} | Size: {row[3]}")

    report_stats = PythonOperator(
        task_id='report_retention_stats',
        python_callable=log_retention_stats,
    )

    cleanup_old_ticks >> vacuum_analyze >> report_stats


# ═══════════════════════════════════════════════════════════════════════════════
# DAG 4: SLA Monitoring (Every minute)
# ═══════════════════════════════════════════════════════════════════════════════
with DAG(
    'stock_sla_monitor',
    default_args=default_args,
    description='Monitor pipeline SLAs: freshness, throughput, latency',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['stock', 'dataops', 'sla'],
    max_active_runs=1,
) as dag_sla:

    def check_freshness_sla(**context):
        """Check if data is arriving within SLA (30s freshness)."""
        hook = PostgresHook(postgres_conn_id='stockdata')
        result = hook.get_records("""
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) as staleness_seconds
            FROM stock_ticks;
        """)
        if result and result[0][0]:
            staleness = float(result[0][0])
            if staleness > 30:
                raise Exception(
                    f"FRESHNESS SLA BREACH: Data is {staleness:.1f}s stale (SLA: 30s)"
                )
            print(f"Freshness OK: {staleness:.1f}s")

    def check_throughput_sla(**context):
        """Check minimum throughput SLA (500 msg/s)."""
        hook = PostgresHook(postgres_conn_id='stockdata')
        result = hook.get_records("""
            SELECT COUNT(*) as tick_count
            FROM stock_ticks
            WHERE timestamp > NOW() - INTERVAL '1 minute';
        """)
        if result and result[0][0]:
            count = int(result[0][0])
            rate = count / 60.0
            if rate < 500:
                raise Exception(
                    f"THROUGHPUT SLA BREACH: {rate:.0f} msg/s (SLA: 500 msg/s)"
                )
            print(f"Throughput OK: {rate:.0f} msg/s ({count} msgs in last 60s)")

    def check_symbol_coverage(**context):
        """Check all symbols are producing data."""
        hook = PostgresHook(postgres_conn_id='stockdata')
        result = hook.get_records("""
            SELECT COUNT(DISTINCT symbol) as active_symbols
            FROM stock_ticks
            WHERE timestamp > NOW() - INTERVAL '1 minute';
        """)
        if result and result[0][0]:
            active = int(result[0][0])
            if active < 20:
                raise Exception(
                    f"COVERAGE SLA BREACH: Only {active} symbols active (SLA: 20+)"
                )
            print(f"Coverage OK: {active} symbols active")

    freshness_check = PythonOperator(
        task_id='check_freshness_sla',
        python_callable=check_freshness_sla,
    )

    throughput_check = PythonOperator(
        task_id='check_throughput_sla',
        python_callable=check_throughput_sla,
    )

    coverage_check = PythonOperator(
        task_id='check_symbol_coverage',
        python_callable=check_symbol_coverage,
    )

    sla_alert = EmailOperator(
        task_id='sla_breach_alert',
        to='data-eng@company.com',
        subject='[CRITICAL] Stock Pipeline SLA Breach',
        html_content="""
            <h3>Pipeline SLA Breach Detected</h3>
            <p>One or more SLA checks have failed. Immediate investigation required.</p>
            <p>Dashboard: <a href="http://localhost:3000">Grafana</a></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    [freshness_check, throughput_check, coverage_check] >> sla_alert
