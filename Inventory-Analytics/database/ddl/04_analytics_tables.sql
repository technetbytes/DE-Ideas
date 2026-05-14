-- ============================================================
-- 04_analytics_tables.sql  –  KPI and reporting tables
-- ============================================================

-- ── Daily inventory summary ─────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.daily_inventory_summary (
    summary_date            DATE            NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    product_name            TEXT,
    category                VARCHAR(100),
    warehouse_id            VARCHAR(50)     NOT NULL,
    warehouse_name          TEXT,
    quantity_on_hand        INTEGER,
    quantity_available      INTEGER,
    quantity_in_transit     INTEGER,
    inventory_value         NUMERIC(14, 2),
    reorder_point           INTEGER,
    reorder_quantity        INTEGER,
    is_below_reorder        BOOLEAN,
    days_of_supply          NUMERIC(8, 1),  -- inventory / avg_daily_sales
    updated_at              TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (summary_date, product_id, warehouse_id)
);

-- ── Product sales metrics (daily) ───────────────────────────
CREATE TABLE IF NOT EXISTS analytics.product_sales_metrics (
    metric_date             DATE            NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    product_name            TEXT,
    category                VARCHAR(100),
    total_quantity_sold     INTEGER,
    total_revenue           NUMERIC(14, 2),
    avg_selling_price       NUMERIC(12, 2),
    transaction_count       INTEGER,
    avg_daily_sales_30d     NUMERIC(10, 2), -- rolling 30-day avg
    avg_daily_sales_7d      NUMERIC(10, 2), -- rolling 7-day avg
    updated_at              TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (metric_date, product_id)
);

-- ── Inventory turnover (monthly) ────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.inventory_turnover (
    year_month              CHAR(7)         NOT NULL,  -- YYYY-MM
    product_id              VARCHAR(50)     NOT NULL,
    product_name            TEXT,
    category                VARCHAR(100),
    cogs                    NUMERIC(14, 2), -- cost of goods sold
    avg_inventory_value     NUMERIC(14, 2),
    turnover_ratio          NUMERIC(8, 2),  -- COGS / avg_inventory
    days_inventory_outstanding NUMERIC(8, 1), -- 30 / turnover_ratio
    updated_at              TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (year_month, product_id)
);

-- ── Reorder alerts ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.reorder_alerts (
    alert_id                SERIAL          PRIMARY KEY,
    alert_date              DATE            NOT NULL DEFAULT CURRENT_DATE,
    product_id              VARCHAR(50)     NOT NULL,
    product_name            TEXT,
    category                VARCHAR(100),
    warehouse_id            VARCHAR(50)     NOT NULL,
    warehouse_name          TEXT,
    quantity_on_hand        INTEGER,
    reorder_point           INTEGER,
    shortage_quantity       INTEGER,        -- reorder_point - quantity_on_hand
    recommended_order_qty   INTEGER,
    supplier_id             VARCHAR(50),
    supplier_name           TEXT,
    lead_time_days          INTEGER,
    urgency                 VARCHAR(20),    -- CRITICAL, HIGH, MEDIUM
    is_resolved             BOOLEAN         DEFAULT FALSE,
    created_at              TIMESTAMPTZ     DEFAULT NOW()
);

-- ── Supplier performance (monthly) ──────────────────────────
CREATE TABLE IF NOT EXISTS analytics.supplier_performance (
    year_month              CHAR(7)         NOT NULL,
    supplier_id             VARCHAR(50)     NOT NULL,
    supplier_name           TEXT,
    total_orders            INTEGER,
    on_time_orders          INTEGER,
    late_orders             INTEGER,
    on_time_pct             NUMERIC(5, 2),
    avg_days_late           NUMERIC(6, 1),
    total_order_value       NUMERIC(14, 2),
    fulfillment_rate        NUMERIC(5, 2),  -- qty_received / qty_ordered
    supplier_rating         NUMERIC(3, 1),
    updated_at              TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (year_month, supplier_id)
);

-- ── ABC classification ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.abc_classification (
    classified_at           DATE            NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    product_name            TEXT,
    category                VARCHAR(100),
    total_revenue_12m       NUMERIC(14, 2),
    revenue_pct             NUMERIC(6, 2),
    cumulative_revenue_pct  NUMERIC(6, 2),
    abc_class               CHAR(1),        -- A, B, C
    updated_at              TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (classified_at, product_id)
);

-- ── Category summary (weekly) ───────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.category_summary (
    week_start              DATE            NOT NULL,
    category                VARCHAR(100)    NOT NULL,
    total_inventory_value   NUMERIC(14, 2),
    total_quantity_on_hand  INTEGER,
    total_revenue           NUMERIC(14, 2),
    product_count           INTEGER,
    items_below_reorder     INTEGER,
    updated_at              TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (week_start, category)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ana_inv_sum_date    ON analytics.daily_inventory_summary (summary_date);
CREATE INDEX IF NOT EXISTS idx_ana_inv_sum_product ON analytics.daily_inventory_summary (product_id);
CREATE INDEX IF NOT EXISTS idx_ana_sales_date      ON analytics.product_sales_metrics   (metric_date);
CREATE INDEX IF NOT EXISTS idx_ana_reorder_date    ON analytics.reorder_alerts           (alert_date);
CREATE INDEX IF NOT EXISTS idx_ana_reorder_product ON analytics.reorder_alerts           (product_id);
CREATE INDEX IF NOT EXISTS idx_ana_abc_class       ON analytics.abc_classification       (abc_class);
