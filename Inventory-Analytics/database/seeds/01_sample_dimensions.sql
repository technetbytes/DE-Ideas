-- ============================================================
-- 01_sample_dimensions.sql  –  Reference / dimension data
-- ============================================================

-- ── Suppliers ───────────────────────────────────────────────
INSERT INTO raw.suppliers
    (supplier_id, supplier_name, country, city, contact_email, payment_terms, rating, source_system)
VALUES
    ('SUP-001', 'GlobalParts Co.',        'USA',        'Chicago',       'orders@globalparts.com',    'NET-30', 4.5, 'ERP'),
    ('SUP-002', 'FastShip Electronics',   'China',      'Shenzhen',      'supply@fastship.cn',        'NET-45', 4.2, 'ERP'),
    ('SUP-003', 'Euro Components GmbH',   'Germany',    'Frankfurt',     'procurement@eurocomp.de',   'NET-30', 4.7, 'ERP'),
    ('SUP-004', 'Pacific Goods Ltd.',     'Japan',      'Osaka',         'orders@pacificgoods.jp',    'NET-60', 3.9, 'ERP'),
    ('SUP-005', 'Local Supplies Inc.',    'USA',        'Dallas',        'orders@localsupplies.com',  'NET-15', 4.8, 'ERP'),
    ('SUP-006', 'Indian Textiles Corp.',  'India',      'Mumbai',        'supply@indiantextiles.in',  'NET-45', 4.1, 'ERP'),
    ('SUP-007', 'Nordic Raw Materials',   'Sweden',     'Stockholm',     'buy@nordicraw.se',          'NET-30', 4.6, 'ERP'),
    ('SUP-008', 'South Asia Logistics',   'Singapore',  'Singapore',     'ops@salogistics.sg',        'NET-30', 4.3, 'ERP');

-- ── Warehouses ──────────────────────────────────────────────
INSERT INTO raw.warehouses
    (warehouse_id, warehouse_name, city, country, capacity_units, source_system)
VALUES
    ('WH-001', 'East Coast DC',     'New York',      'USA',       50000, 'WMS'),
    ('WH-002', 'West Coast DC',     'Los Angeles',   'USA',       75000, 'WMS'),
    ('WH-003', 'Central Hub',       'Dallas',        'USA',       60000, 'WMS'),
    ('WH-004', 'European DC',       'Amsterdam',     'Netherlands', 40000, 'WMS'),
    ('WH-005', 'Asia Pacific DC',   'Singapore',     'Singapore', 35000, 'WMS');

-- ── Products ────────────────────────────────────────────────
INSERT INTO raw.products
    (product_id, product_name, category, sub_category, sku, unit_cost, unit_price,
     reorder_point, reorder_quantity, lead_time_days, supplier_id, source_system)
VALUES
    -- Electronics
    ('PRD-001', 'Wireless Headphones Pro',      'Electronics', 'Audio',        'SKU-EL-001', 45.00,  129.99, 100, 300,  14, 'SUP-002', 'ERP'),
    ('PRD-002', 'Bluetooth Speaker Mini',       'Electronics', 'Audio',        'SKU-EL-002', 18.00,   49.99,  80, 250,  14, 'SUP-002', 'ERP'),
    ('PRD-003', 'USB-C Hub 7-Port',             'Electronics', 'Accessories',  'SKU-EL-003', 12.00,   34.99, 150, 400,  10, 'SUP-001', 'ERP'),
    ('PRD-004', 'Mechanical Keyboard TKL',      'Electronics', 'Peripherals',  'SKU-EL-004', 38.00,   89.99,  60, 200,  21, 'SUP-002', 'ERP'),
    ('PRD-005', 'Gaming Mouse 16000 DPI',       'Electronics', 'Peripherals',  'SKU-EL-005', 22.00,   59.99,  90, 250,  14, 'SUP-002', 'ERP'),
    ('PRD-006', 'Laptop Stand Aluminium',       'Electronics', 'Accessories',  'SKU-EL-006',  8.50,   29.99, 200, 500,   7, 'SUP-001', 'ERP'),
    ('PRD-007', '4K Webcam 60fps',              'Electronics', 'Cameras',      'SKU-EL-007', 55.00,  149.99,  40, 120,  21, 'SUP-004', 'ERP'),
    ('PRD-008', 'Portable SSD 1TB',             'Electronics', 'Storage',      'SKU-EL-008', 60.00,  109.99,  70, 200,  14, 'SUP-002', 'ERP'),
    -- Office Supplies
    ('PRD-011', 'Ergonomic Office Chair',       'Furniture',   'Seating',      'SKU-FN-001', 120.00, 349.99,  20,  50,  30, 'SUP-003', 'ERP'),
    ('PRD-012', 'Standing Desk Converter',      'Furniture',   'Desks',        'SKU-FN-002',  85.00, 199.99,  15,  40,  30, 'SUP-003', 'ERP'),
    ('PRD-013', 'Monitor Arm Dual',             'Furniture',   'Accessories',  'SKU-FN-003',  25.00,  69.99,  50, 150,  14, 'SUP-001', 'ERP'),
    -- Apparel
    ('PRD-021', 'Cotton T-Shirt (Pack 3)',       'Apparel',     'Tops',         'SKU-AP-001',   5.50,  19.99, 300, 800,  21, 'SUP-006', 'ERP'),
    ('PRD-022', 'Fleece Hoodie',                'Apparel',     'Outerwear',    'SKU-AP-002',  14.00,  44.99, 150, 400,  28, 'SUP-006', 'ERP'),
    ('PRD-023', 'Running Shorts',               'Apparel',     'Bottoms',      'SKU-AP-003',   7.00,  24.99, 200, 500,  21, 'SUP-006', 'ERP'),
    -- Health & Beauty
    ('PRD-031', 'Vitamin C Serum 30ml',         'Health',      'Skincare',     'SKU-HB-001',   6.00,  24.99, 400, 1000,  7, 'SUP-005', 'ERP'),
    ('PRD-032', 'Protein Powder Vanilla 1kg',   'Health',      'Supplements',  'SKU-HB-002',  12.00,  39.99, 200,  600,  7, 'SUP-005', 'ERP'),
    ('PRD-033', 'Yoga Mat 6mm',                 'Health',      'Fitness',      'SKU-HB-003',   8.00,  29.99, 120,  350,  7, 'SUP-005', 'ERP'),
    -- Home & Garden
    ('PRD-041', 'Stainless Steel Water Bottle', 'Home',        'Kitchen',      'SKU-HG-001',   4.50,  18.99, 300,  800,  7, 'SUP-007', 'ERP'),
    ('PRD-042', 'Bamboo Cutting Board Set',     'Home',        'Kitchen',      'SKU-HG-002',   6.00,  22.99, 150,  400, 14, 'SUP-007', 'ERP'),
    ('PRD-043', 'LED Desk Lamp Dimmable',       'Home',        'Lighting',     'SKU-HG-003',   9.00,  34.99, 100,  300, 14, 'SUP-003', 'ERP');

-- Propagate dimension data to staging immediately so FKs resolve
INSERT INTO staging.suppliers (supplier_id, supplier_name, country, city, contact_email, payment_terms, rating, is_active)
SELECT supplier_id, supplier_name, country, city, contact_email, payment_terms, rating, is_active
FROM raw.suppliers
ON CONFLICT (supplier_id) DO UPDATE
    SET supplier_name = EXCLUDED.supplier_name,
        rating        = EXCLUDED.rating,
        updated_at    = NOW();

INSERT INTO staging.warehouses (warehouse_id, warehouse_name, city, country, capacity_units)
SELECT warehouse_id, warehouse_name, city, country, capacity_units
FROM raw.warehouses
ON CONFLICT (warehouse_id) DO UPDATE
    SET warehouse_name = EXCLUDED.warehouse_name,
        updated_at     = NOW();

INSERT INTO staging.products
    (product_id, product_name, category, sub_category, sku, unit_cost, unit_price,
     reorder_point, reorder_quantity, lead_time_days, supplier_id, is_active)
SELECT product_id, product_name, category, sub_category, sku, unit_cost, unit_price,
       reorder_point, reorder_quantity, lead_time_days, supplier_id, is_active
FROM raw.products
ON CONFLICT (product_id) DO UPDATE
    SET product_name   = EXCLUDED.product_name,
        unit_cost      = EXCLUDED.unit_cost,
        unit_price     = EXCLUDED.unit_price,
        updated_at     = NOW();
