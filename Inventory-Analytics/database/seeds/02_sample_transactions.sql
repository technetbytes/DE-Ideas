-- ============================================================
-- 02_sample_transactions.sql  –  90 days of inventory and sales
-- ============================================================

-- ── Inventory snapshots (last 90 days, all products × all warehouses) ──
-- We use a PL/pgSQL block so we can loop over dates and products cleanly.

DO $$
DECLARE
    v_date          DATE;
    v_base_qty      INTEGER;
    v_noise         INTEGER;
    v_product       RECORD;
    v_warehouse     RECORD;
BEGIN
    FOR v_date IN
        SELECT generate_series(
            CURRENT_DATE - INTERVAL '89 days',
            CURRENT_DATE,
            INTERVAL '1 day'
        )::DATE
    LOOP
        FOR v_product IN SELECT product_id, reorder_point FROM raw.products LOOP
            FOR v_warehouse IN SELECT warehouse_id FROM raw.warehouses LOOP
                -- Base stock: random between reorder_point and 3×reorder_point
                v_base_qty := (v_product.reorder_point * (1 + random() * 2))::INTEGER;
                -- Add small daily noise ± 5%
                v_noise := (v_base_qty * (random() * 0.1 - 0.05))::INTEGER;

                INSERT INTO raw.inventory_snapshots
                    (snapshot_date, product_id, warehouse_id,
                     quantity_on_hand, quantity_reserved, quantity_in_transit, unit_cost, source_system)
                SELECT
                    v_date,
                    v_product.product_id,
                    v_warehouse.warehouse_id,
                    GREATEST(0, v_base_qty + v_noise),
                    GREATEST(0, (v_base_qty * random() * 0.1)::INTEGER),
                    GREATEST(0, (v_base_qty * random() * 0.05)::INTEGER),
                    rp.unit_cost,
                    'WMS'
                FROM raw.products rp
                WHERE rp.product_id = v_product.product_id
                ON CONFLICT DO NOTHING;
            END LOOP;
        END LOOP;
    END LOOP;
END
$$;

-- ── Sales transactions (last 90 days) ───────────────────────
DO $$
DECLARE
    v_date          DATE;
    v_product       RECORD;
    v_warehouse     RECORD;
    v_num_txns      INTEGER;
    v_txn_qty       INTEGER;
    v_txn_id        TEXT;
    i               INTEGER;
    v_channels      TEXT[] := ARRAY['online','retail','wholesale','marketplace'];
BEGIN
    FOR v_date IN
        SELECT generate_series(
            CURRENT_DATE - INTERVAL '89 days',
            CURRENT_DATE,
            INTERVAL '1 day'
        )::DATE
    LOOP
        FOR v_product IN
            SELECT product_id, unit_price FROM raw.products
        LOOP
            -- 2-6 transactions per product per day across warehouses
            v_num_txns := (2 + random() * 4)::INTEGER;
            FOR i IN 1 .. v_num_txns LOOP
                -- Pick a random warehouse
                SELECT warehouse_id INTO v_warehouse
                FROM raw.warehouses
                ORDER BY random()
                LIMIT 1;

                v_txn_qty := (1 + random() * 9)::INTEGER;
                v_txn_id  := 'TXN-' || TO_CHAR(v_date, 'YYYYMMDD')
                             || '-' || v_product.product_id
                             || '-' || i::TEXT
                             || '-' || floor(random()*9000+1000)::TEXT;

                INSERT INTO raw.sales_transactions
                    (transaction_id, transaction_date, product_id, warehouse_id,
                     customer_id, quantity_sold, unit_price, discount_pct, channel, source_system)
                VALUES (
                    v_txn_id,
                    v_date,
                    v_product.product_id,
                    v_warehouse.warehouse_id,
                    'CUST-' || floor(random() * 5000 + 1)::TEXT,
                    v_txn_qty,
                    v_product.unit_price * (0.95 + random() * 0.1),  -- slight price variation
                    round((random() * 15)::NUMERIC, 2),               -- 0-15% discount
                    v_channels[1 + floor(random() * 4)::INTEGER],
                    'POS'
                )
                ON CONFLICT DO NOTHING;
            END LOOP;
        END LOOP;
    END LOOP;
END
$$;

-- ── Purchase orders (mix of completed and in-flight) ─────────
DO $$
DECLARE
    v_product       RECORD;
    v_supplier      RECORD;
    v_warehouse     RECORD;
    v_order_date    DATE;
    v_expected_date DATE;
    v_received_date DATE;
    v_status        TEXT;
    v_qty           INTEGER;
    v_po_num        TEXT;
    i               INTEGER;
BEGIN
    FOR v_product IN
        SELECT p.product_id, p.reorder_quantity, p.lead_time_days,
               p.unit_cost, p.supplier_id
        FROM raw.products p
    LOOP
        -- 4 historical POs per product
        FOR i IN 1 .. 4 LOOP
            v_order_date    := CURRENT_DATE - (10 + i * 20 + floor(random()*10))::INTEGER;
            v_expected_date := v_order_date + v_product.lead_time_days;

            -- 70% received on time, 20% late, 10% still pending
            IF random() < 0.70 THEN
                v_status        := 'COMPLETE';
                v_received_date := v_expected_date - floor(random() * 3)::INTEGER;
            ELSIF random() < 0.89 THEN
                v_status        := 'COMPLETE';
                v_received_date := v_expected_date + (1 + floor(random() * 7))::INTEGER;
            ELSE
                v_status        := 'PENDING';
                v_received_date := NULL;
            END IF;

            v_qty    := v_product.reorder_quantity + floor(random() * 50 - 25)::INTEGER;
            v_po_num := 'PO-' || TO_CHAR(v_order_date, 'YYYYMMDD')
                        || '-' || v_product.product_id
                        || '-' || i::TEXT;

            SELECT warehouse_id INTO v_warehouse
            FROM raw.warehouses ORDER BY random() LIMIT 1;

            INSERT INTO raw.purchase_orders
                (po_number, order_date, expected_date, received_date,
                 product_id, supplier_id, warehouse_id,
                 quantity_ordered, quantity_received, unit_cost, status, source_system)
            VALUES (
                v_po_num,
                v_order_date,
                v_expected_date,
                v_received_date,
                v_product.product_id,
                v_product.supplier_id,
                v_warehouse.warehouse_id,
                GREATEST(1, v_qty),
                CASE WHEN v_status = 'COMPLETE' THEN GREATEST(1, v_qty) ELSE 0 END,
                v_product.unit_cost * (0.98 + random() * 0.04),
                v_status,
                'ERP'
            )
            ON CONFLICT DO NOTHING;
        END LOOP;
    END LOOP;
END
$$;

-- ── Propagate snapshots and transactions to staging ──────────
INSERT INTO staging.inventory_snapshots
    (snapshot_date, product_id, warehouse_id,
     quantity_on_hand, quantity_reserved, quantity_in_transit, unit_cost)
SELECT
    s.snapshot_date,
    s.product_id,
    s.warehouse_id,
    s.quantity_on_hand,
    s.quantity_reserved,
    s.quantity_in_transit,
    s.unit_cost
FROM raw.inventory_snapshots s
WHERE EXISTS (SELECT 1 FROM staging.products  p WHERE p.product_id  = s.product_id)
  AND EXISTS (SELECT 1 FROM staging.warehouses w WHERE w.warehouse_id = s.warehouse_id)
ON CONFLICT (snapshot_date, product_id, warehouse_id) DO NOTHING;

INSERT INTO staging.sales_transactions
    (transaction_id, transaction_date, product_id, warehouse_id,
     customer_id, quantity_sold, unit_price, discount_pct, channel)
SELECT
    t.transaction_id,
    t.transaction_date,
    t.product_id,
    t.warehouse_id,
    t.customer_id,
    t.quantity_sold,
    t.unit_price,
    t.discount_pct,
    t.channel
FROM raw.sales_transactions t
WHERE EXISTS (SELECT 1 FROM staging.products  p WHERE p.product_id  = t.product_id)
  AND (t.warehouse_id IS NULL OR EXISTS (SELECT 1 FROM staging.warehouses w WHERE w.warehouse_id = t.warehouse_id))
ON CONFLICT (transaction_id) DO NOTHING;

INSERT INTO staging.purchase_orders
    (po_number, order_date, expected_date, received_date,
     product_id, supplier_id, warehouse_id,
     quantity_ordered, quantity_received, unit_cost, status)
SELECT
    po.po_number,
    po.order_date,
    po.expected_date,
    po.received_date,
    po.product_id,
    po.supplier_id,
    po.warehouse_id,
    po.quantity_ordered,
    po.quantity_received,
    po.unit_cost,
    po.status
FROM raw.purchase_orders po
WHERE EXISTS (SELECT 1 FROM staging.products  p WHERE p.product_id  = po.product_id)
  AND EXISTS (SELECT 1 FROM staging.suppliers s WHERE s.supplier_id  = po.supplier_id)
ON CONFLICT (po_number) DO NOTHING;
