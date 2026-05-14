-- ============================================================
-- 03_seed_analytics.sql  –  Pre-populate analytics tables
-- ============================================================

-- ── Daily inventory summary ─────────────────────────────────
INSERT INTO analytics.daily_inventory_summary
    (summary_date, product_id, product_name, category,
     warehouse_id, warehouse_name,
     quantity_on_hand, quantity_available, quantity_in_transit,
     inventory_value, reorder_point, reorder_quantity,
     is_below_reorder, days_of_supply)
SELECT
    s.snapshot_date,
    s.product_id,
    p.product_name,
    p.category,
    s.warehouse_id,
    w.warehouse_name,
    s.quantity_on_hand,
    s.quantity_available,
    s.quantity_in_transit,
    s.inventory_value,
    p.reorder_point,
    p.reorder_quantity,
    s.quantity_on_hand < p.reorder_point,
    CASE
        WHEN COALESCE(avg_sales.avg_qty, 0) > 0
        THEN ROUND(s.quantity_on_hand::NUMERIC / avg_sales.avg_qty, 1)
        ELSE NULL
    END
FROM staging.inventory_snapshots s
JOIN staging.products   p ON p.product_id   = s.product_id
JOIN staging.warehouses w ON w.warehouse_id = s.warehouse_id
LEFT JOIN LATERAL (
    SELECT ROUND(AVG(quantity_sold)::NUMERIC, 2) AS avg_qty
    FROM staging.sales_transactions st
    WHERE st.product_id = s.product_id
      AND st.transaction_date BETWEEN s.snapshot_date - 30 AND s.snapshot_date
) avg_sales ON TRUE
ON CONFLICT (summary_date, product_id, warehouse_id) DO NOTHING;

-- ── Product sales metrics ───────────────────────────────────
INSERT INTO analytics.product_sales_metrics
    (metric_date, product_id, product_name, category,
     total_quantity_sold, total_revenue, avg_selling_price,
     transaction_count, avg_daily_sales_30d, avg_daily_sales_7d)
SELECT
    t.transaction_date,
    t.product_id,
    p.product_name,
    p.category,
    SUM(t.quantity_sold),
    SUM(t.revenue),
    ROUND(AVG(t.unit_price), 2),
    COUNT(*),
    ROUND(
        (SELECT AVG(daily_qty)
         FROM (
             SELECT transaction_date, SUM(quantity_sold) AS daily_qty
             FROM staging.sales_transactions t2
             WHERE t2.product_id = t.product_id
               AND t2.transaction_date BETWEEN t.transaction_date - 30 AND t.transaction_date
             GROUP BY transaction_date
         ) d30
        )::NUMERIC, 2),
    ROUND(
        (SELECT AVG(daily_qty)
         FROM (
             SELECT transaction_date, SUM(quantity_sold) AS daily_qty
             FROM staging.sales_transactions t2
             WHERE t2.product_id = t.product_id
               AND t2.transaction_date BETWEEN t.transaction_date - 7 AND t.transaction_date
             GROUP BY transaction_date
         ) d7
        )::NUMERIC, 2)
FROM staging.sales_transactions t
JOIN staging.products p ON p.product_id = t.product_id
GROUP BY t.transaction_date, t.product_id, p.product_name, p.category
ON CONFLICT (metric_date, product_id) DO NOTHING;

-- ── Inventory turnover (monthly) ────────────────────────────
INSERT INTO analytics.inventory_turnover
    (year_month, product_id, product_name, category,
     cogs, avg_inventory_value, turnover_ratio, days_inventory_outstanding)
SELECT
    TO_CHAR(t.transaction_date, 'YYYY-MM') AS year_month,
    t.product_id,
    p.product_name,
    p.category,
    ROUND(SUM(t.quantity_sold * p.unit_cost), 2)            AS cogs,
    ROUND(AVG(s.inventory_value), 2)                         AS avg_inventory_value,
    CASE
        WHEN AVG(s.inventory_value) > 0
        THEN ROUND(SUM(t.quantity_sold * p.unit_cost) / AVG(s.inventory_value), 2)
        ELSE NULL
    END                                                       AS turnover_ratio,
    CASE
        WHEN SUM(t.quantity_sold * p.unit_cost) > 0 AND AVG(s.inventory_value) > 0
        THEN ROUND(30 / (SUM(t.quantity_sold * p.unit_cost) / AVG(s.inventory_value)), 1)
        ELSE NULL
    END                                                       AS days_inventory_outstanding
FROM staging.sales_transactions t
JOIN staging.products p ON p.product_id = t.product_id
LEFT JOIN staging.inventory_snapshots s
       ON s.product_id = t.product_id
      AND s.snapshot_date = t.transaction_date
GROUP BY TO_CHAR(t.transaction_date, 'YYYY-MM'), t.product_id, p.product_name, p.category
ON CONFLICT (year_month, product_id) DO NOTHING;

-- ── Reorder alerts (current snapshot only) ──────────────────
INSERT INTO analytics.reorder_alerts
    (alert_date, product_id, product_name, category,
     warehouse_id, warehouse_name,
     quantity_on_hand, reorder_point, shortage_quantity, recommended_order_qty,
     supplier_id, supplier_name, lead_time_days, urgency)
SELECT
    CURRENT_DATE,
    s.product_id,
    p.product_name,
    p.category,
    s.warehouse_id,
    w.warehouse_name,
    s.quantity_on_hand,
    p.reorder_point,
    p.reorder_point - s.quantity_on_hand,
    p.reorder_quantity,
    p.supplier_id,
    sup.supplier_name,
    p.lead_time_days,
    CASE
        WHEN s.quantity_on_hand = 0             THEN 'CRITICAL'
        WHEN s.quantity_on_hand < p.reorder_point * 0.5 THEN 'HIGH'
        ELSE 'MEDIUM'
    END
FROM staging.inventory_snapshots s
JOIN staging.products   p   ON p.product_id   = s.product_id
JOIN staging.warehouses w   ON w.warehouse_id = s.warehouse_id
LEFT JOIN staging.suppliers sup ON sup.supplier_id = p.supplier_id
WHERE s.snapshot_date = CURRENT_DATE
  AND s.quantity_on_hand < p.reorder_point;

-- ── Supplier performance (monthly) ──────────────────────────
INSERT INTO analytics.supplier_performance
    (year_month, supplier_id, supplier_name,
     total_orders, on_time_orders, late_orders, on_time_pct,
     avg_days_late, total_order_value, fulfillment_rate, supplier_rating)
SELECT
    TO_CHAR(po.order_date, 'YYYY-MM')   AS year_month,
    po.supplier_id,
    sup.supplier_name,
    COUNT(*)                             AS total_orders,
    SUM(CASE WHEN COALESCE(po.days_late, 0) <= 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN COALESCE(po.days_late, 0) > 0  THEN 1 ELSE 0 END),
    ROUND(
        SUM(CASE WHEN COALESCE(po.days_late, 0) <= 0 THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*) * 100, 2),
    ROUND(AVG(CASE WHEN po.days_late > 0 THEN po.days_late ELSE NULL END)::NUMERIC, 1),
    ROUND(SUM(po.total_cost), 2),
    ROUND(SUM(po.quantity_received)::NUMERIC / NULLIF(SUM(po.quantity_ordered), 0) * 100, 2),
    sup.rating
FROM staging.purchase_orders po
JOIN staging.suppliers sup ON sup.supplier_id = po.supplier_id
WHERE po.status = 'COMPLETE'
GROUP BY TO_CHAR(po.order_date, 'YYYY-MM'), po.supplier_id, sup.supplier_name, sup.rating
ON CONFLICT (year_month, supplier_id) DO NOTHING;

-- ── ABC classification ───────────────────────────────────────
INSERT INTO analytics.abc_classification
    (classified_at, product_id, product_name, category,
     total_revenue_12m, revenue_pct, cumulative_revenue_pct, abc_class)
WITH revenue_ranked AS (
    SELECT
        t.product_id,
        p.product_name,
        p.category,
        SUM(t.revenue) AS total_revenue
    FROM staging.sales_transactions t
    JOIN staging.products p ON p.product_id = t.product_id
    WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY t.product_id, p.product_name, p.category
),
with_pcts AS (
    SELECT *,
        ROUND(total_revenue / SUM(total_revenue) OVER () * 100, 2) AS revenue_pct,
        ROUND(SUM(total_revenue) OVER (ORDER BY total_revenue DESC
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
              / SUM(total_revenue) OVER () * 100, 2) AS cumulative_pct
    FROM revenue_ranked
)
SELECT
    CURRENT_DATE,
    product_id,
    product_name,
    category,
    ROUND(total_revenue, 2),
    revenue_pct,
    cumulative_pct,
    CASE
        WHEN cumulative_pct <= 70 THEN 'A'
        WHEN cumulative_pct <= 90 THEN 'B'
        ELSE 'C'
    END
FROM with_pcts
ON CONFLICT (classified_at, product_id) DO NOTHING;

-- ── Category weekly summary ──────────────────────────────────
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
       ON t.product_id = s.product_id
      AND t.transaction_date = s.snapshot_date
GROUP BY DATE_TRUNC('week', s.snapshot_date)::DATE, p.category
ON CONFLICT (week_start, category) DO NOTHING;
