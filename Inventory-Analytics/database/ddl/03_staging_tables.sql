-- ============================================================
-- 03_staging_tables.sql  –  Cleaned and validated tables
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.products (
    product_id          VARCHAR(50)     PRIMARY KEY,
    product_name        TEXT            NOT NULL,
    category            VARCHAR(100)    NOT NULL,
    sub_category        VARCHAR(100),
    sku                 VARCHAR(100)    UNIQUE,
    unit_cost           NUMERIC(12, 2)  NOT NULL CHECK (unit_cost >= 0),
    unit_price          NUMERIC(12, 2)  NOT NULL CHECK (unit_price >= 0),
    margin_pct          NUMERIC(6, 2)   GENERATED ALWAYS AS
                            (ROUND(((unit_price - unit_cost) / NULLIF(unit_price, 0)) * 100, 2))
                            STORED,
    reorder_point       INTEGER         NOT NULL DEFAULT 0,
    reorder_quantity    INTEGER         NOT NULL DEFAULT 0,
    lead_time_days      INTEGER         NOT NULL DEFAULT 7,
    supplier_id         VARCHAR(50),
    is_active           BOOLEAN         DEFAULT TRUE,
    updated_at          TIMESTAMPTZ     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.suppliers (
    supplier_id     VARCHAR(50)     PRIMARY KEY,
    supplier_name   TEXT            NOT NULL,
    country         VARCHAR(100),
    city            VARCHAR(100),
    contact_email   VARCHAR(200),
    payment_terms   VARCHAR(50),
    rating          NUMERIC(3, 1)   CHECK (rating BETWEEN 0 AND 5),
    is_active       BOOLEAN         DEFAULT TRUE,
    updated_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.warehouses (
    warehouse_id    VARCHAR(50)     PRIMARY KEY,
    warehouse_name  TEXT            NOT NULL,
    city            VARCHAR(100),
    country         VARCHAR(100),
    capacity_units  INTEGER,
    updated_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.inventory_snapshots (
    snapshot_date       DATE            NOT NULL,
    product_id          VARCHAR(50)     NOT NULL REFERENCES staging.products(product_id),
    warehouse_id        VARCHAR(50)     NOT NULL REFERENCES staging.warehouses(warehouse_id),
    quantity_on_hand    INTEGER         NOT NULL CHECK (quantity_on_hand >= 0),
    quantity_reserved   INTEGER         NOT NULL DEFAULT 0 CHECK (quantity_reserved >= 0),
    quantity_in_transit INTEGER         NOT NULL DEFAULT 0 CHECK (quantity_in_transit >= 0),
    quantity_available  INTEGER         GENERATED ALWAYS AS
                            (quantity_on_hand - quantity_reserved)
                            STORED,
    unit_cost           NUMERIC(12, 2),
    inventory_value     NUMERIC(14, 2)  GENERATED ALWAYS AS
                            (quantity_on_hand * unit_cost)
                            STORED,
    updated_at          TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, product_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS staging.sales_transactions (
    transaction_id      VARCHAR(100)    PRIMARY KEY,
    transaction_date    DATE            NOT NULL,
    product_id          VARCHAR(50)     NOT NULL REFERENCES staging.products(product_id),
    warehouse_id        VARCHAR(50)     REFERENCES staging.warehouses(warehouse_id),
    customer_id         VARCHAR(50),
    quantity_sold       INTEGER         NOT NULL CHECK (quantity_sold > 0),
    unit_price          NUMERIC(12, 2)  NOT NULL,
    discount_pct        NUMERIC(5, 2)   DEFAULT 0,
    revenue             NUMERIC(14, 2)  GENERATED ALWAYS AS
                            (quantity_sold * unit_price * (1 - discount_pct / 100))
                            STORED,
    channel             VARCHAR(50),
    updated_at          TIMESTAMPTZ     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.purchase_orders (
    po_number           VARCHAR(100)    PRIMARY KEY,
    order_date          DATE            NOT NULL,
    expected_date       DATE,
    received_date       DATE,
    product_id          VARCHAR(50)     NOT NULL REFERENCES staging.products(product_id),
    supplier_id         VARCHAR(50)     NOT NULL REFERENCES staging.suppliers(supplier_id),
    warehouse_id        VARCHAR(50)     REFERENCES staging.warehouses(warehouse_id),
    quantity_ordered    INTEGER         NOT NULL CHECK (quantity_ordered > 0),
    quantity_received   INTEGER         DEFAULT 0,
    unit_cost           NUMERIC(12, 2),
    total_cost          NUMERIC(14, 2)  GENERATED ALWAYS AS
                            (quantity_ordered * unit_cost)
                            STORED,
    status              VARCHAR(30),
    days_late           INTEGER         GENERATED ALWAYS AS
                            (CASE
                                WHEN received_date IS NOT NULL AND expected_date IS NOT NULL
                                THEN received_date - expected_date
                                ELSE NULL
                             END)
                            STORED,
    updated_at          TIMESTAMPTZ     DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_stg_inv_snap_date    ON staging.inventory_snapshots (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_stg_inv_snap_product ON staging.inventory_snapshots (product_id);
CREATE INDEX IF NOT EXISTS idx_stg_sales_date       ON staging.sales_transactions  (transaction_date);
CREATE INDEX IF NOT EXISTS idx_stg_sales_product    ON staging.sales_transactions  (product_id);
CREATE INDEX IF NOT EXISTS idx_stg_po_status        ON staging.purchase_orders     (status);
