-- ============================================================
-- 02_raw_tables.sql  –  Raw landing zone tables
-- ============================================================

-- ── Products ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.products (
    id                  SERIAL PRIMARY KEY,
    product_id          VARCHAR(50)     NOT NULL,
    product_name        TEXT            NOT NULL,
    category            VARCHAR(100),
    sub_category        VARCHAR(100),
    sku                 VARCHAR(100)    UNIQUE,
    unit_cost           NUMERIC(12, 2),
    unit_price          NUMERIC(12, 2),
    reorder_point       INTEGER,
    reorder_quantity    INTEGER,
    lead_time_days      INTEGER,
    supplier_id         VARCHAR(50),
    is_active           BOOLEAN         DEFAULT TRUE,
    source_system       VARCHAR(50),
    ingested_at         TIMESTAMPTZ     DEFAULT NOW()
);

-- ── Suppliers ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.suppliers (
    id              SERIAL PRIMARY KEY,
    supplier_id     VARCHAR(50)  NOT NULL,
    supplier_name   TEXT         NOT NULL,
    country         VARCHAR(100),
    city            VARCHAR(100),
    contact_email   VARCHAR(200),
    payment_terms   VARCHAR(50),
    rating          NUMERIC(3, 1),
    is_active       BOOLEAN      DEFAULT TRUE,
    source_system   VARCHAR(50),
    ingested_at     TIMESTAMPTZ  DEFAULT NOW()
);

-- ── Warehouses ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.warehouses (
    id              SERIAL PRIMARY KEY,
    warehouse_id    VARCHAR(50)  NOT NULL,
    warehouse_name  TEXT         NOT NULL,
    city            VARCHAR(100),
    country         VARCHAR(100),
    capacity_units  INTEGER,
    source_system   VARCHAR(50),
    ingested_at     TIMESTAMPTZ  DEFAULT NOW()
);

-- ── Inventory snapshots ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.inventory_snapshots (
    id              SERIAL PRIMARY KEY,
    snapshot_date   DATE         NOT NULL,
    product_id      VARCHAR(50)  NOT NULL,
    warehouse_id    VARCHAR(50)  NOT NULL,
    quantity_on_hand    INTEGER  NOT NULL,
    quantity_reserved   INTEGER  DEFAULT 0,
    quantity_in_transit INTEGER  DEFAULT 0,
    unit_cost       NUMERIC(12, 2),
    source_system   VARCHAR(50),
    ingested_at     TIMESTAMPTZ  DEFAULT NOW()
);

-- ── Sales transactions ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.sales_transactions (
    id                  SERIAL PRIMARY KEY,
    transaction_id      VARCHAR(100)    NOT NULL,
    transaction_date    DATE            NOT NULL,
    product_id          VARCHAR(50)     NOT NULL,
    warehouse_id        VARCHAR(50),
    customer_id         VARCHAR(50),
    quantity_sold       INTEGER         NOT NULL,
    unit_price          NUMERIC(12, 2),
    discount_pct        NUMERIC(5, 2)   DEFAULT 0,
    channel             VARCHAR(50),
    source_system       VARCHAR(50),
    ingested_at         TIMESTAMPTZ     DEFAULT NOW()
);

-- ── Purchase orders ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.purchase_orders (
    id                  SERIAL PRIMARY KEY,
    po_number           VARCHAR(100)    NOT NULL,
    order_date          DATE            NOT NULL,
    expected_date       DATE,
    received_date       DATE,
    product_id          VARCHAR(50)     NOT NULL,
    supplier_id         VARCHAR(50)     NOT NULL,
    warehouse_id        VARCHAR(50),
    quantity_ordered    INTEGER         NOT NULL,
    quantity_received   INTEGER         DEFAULT 0,
    unit_cost           NUMERIC(12, 2),
    status              VARCHAR(30),    -- PENDING, PARTIAL, COMPLETE, CANCELLED
    source_system       VARCHAR(50),
    ingested_at         TIMESTAMPTZ     DEFAULT NOW()
);

-- ── Pipeline run log ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(200)    NOT NULL,
    run_id          VARCHAR(200)    NOT NULL,
    task_id         VARCHAR(200)    NOT NULL,
    status          VARCHAR(30)     NOT NULL,
    rows_processed  INTEGER         DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ     DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

-- Indexes on commonly filtered columns
CREATE INDEX IF NOT EXISTS idx_raw_inv_snap_date    ON raw.inventory_snapshots (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_raw_inv_snap_product ON raw.inventory_snapshots (product_id);
CREATE INDEX IF NOT EXISTS idx_raw_sales_date       ON raw.sales_transactions  (transaction_date);
CREATE INDEX IF NOT EXISTS idx_raw_sales_product    ON raw.sales_transactions  (product_id);
CREATE INDEX IF NOT EXISTS idx_raw_po_date          ON raw.purchase_orders     (order_date);
