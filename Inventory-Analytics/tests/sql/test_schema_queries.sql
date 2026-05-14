-- tests/sql/test_schema_queries.sql
-- Smoke-test queries to run after init to verify data is loaded correctly.
-- Run with:  psql -U inventory -d inventory -f tests/sql/test_schema_queries.sql

\echo '=== Schema counts ==='
SELECT 'raw.products'            AS table_name, COUNT(*) AS rows FROM raw.products
UNION ALL
SELECT 'raw.suppliers',                          COUNT(*) FROM raw.suppliers
UNION ALL
SELECT 'raw.warehouses',                         COUNT(*) FROM raw.warehouses
UNION ALL
SELECT 'raw.inventory_snapshots',                COUNT(*) FROM raw.inventory_snapshots
UNION ALL
SELECT 'raw.sales_transactions',                 COUNT(*) FROM raw.sales_transactions
UNION ALL
SELECT 'raw.purchase_orders',                    COUNT(*) FROM raw.purchase_orders
UNION ALL
SELECT 'staging.products',                       COUNT(*) FROM staging.products
UNION ALL
SELECT 'staging.suppliers',                      COUNT(*) FROM staging.suppliers
UNION ALL
SELECT 'staging.warehouses',                     COUNT(*) FROM staging.warehouses
UNION ALL
SELECT 'staging.inventory_snapshots',            COUNT(*) FROM staging.inventory_snapshots
UNION ALL
SELECT 'staging.sales_transactions',             COUNT(*) FROM staging.sales_transactions
UNION ALL
SELECT 'analytics.daily_inventory_summary',      COUNT(*) FROM analytics.daily_inventory_summary
UNION ALL
SELECT 'analytics.product_sales_metrics',        COUNT(*) FROM analytics.product_sales_metrics
UNION ALL
SELECT 'analytics.reorder_alerts',               COUNT(*) FROM analytics.reorder_alerts
UNION ALL
SELECT 'analytics.abc_classification',           COUNT(*) FROM analytics.abc_classification
UNION ALL
SELECT 'analytics.supplier_performance',         COUNT(*) FROM analytics.supplier_performance
ORDER BY table_name;

\echo ''
\echo '=== Reorder alerts by urgency ==='
SELECT urgency, COUNT(*) AS alerts
FROM analytics.reorder_alerts
GROUP BY urgency ORDER BY urgency;

\echo ''
\echo '=== ABC classification breakdown ==='
SELECT abc_class, COUNT(*) AS products, ROUND(SUM(total_revenue_12m),2) AS total_revenue
FROM analytics.abc_classification
GROUP BY abc_class ORDER BY abc_class;

\echo ''
\echo '=== Supplier on-time performance ==='
SELECT supplier_name, on_time_pct, avg_days_late, fulfillment_rate
FROM analytics.supplier_performance
ORDER BY on_time_pct DESC;

\echo ''
\echo '=== Top 5 products by revenue (all time) ==='
SELECT product_name, ROUND(SUM(total_revenue),2) AS total_revenue
FROM analytics.product_sales_metrics
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 5;
