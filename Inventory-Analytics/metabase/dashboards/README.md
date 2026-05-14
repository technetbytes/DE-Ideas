# Metabase Dashboard Setup

## Connection details (first-time setup)

When Metabase opens at http://localhost:3000, complete the wizard:

| Field    | Value       |
|----------|-------------|
| Type     | PostgreSQL  |
| Host     | `postgres`  |
| Port     | `5432`      |
| Database | `inventory` |
| Username | `inventory` |
| Password | `inventory` |

---

## Recommended dashboards

### 1. Stock Overview
- **Table**: `analytics.daily_inventory_summary`
- **Charts**: Total inventory value (big number), stock by category (bar), items below reorder point (table)
- **Filters**: Date, category, warehouse

### 2. Reorder Alerts
- **Table**: `analytics.reorder_alerts`
- **Charts**: Alert count by urgency (donut), alert list sorted by shortage quantity (table)
- **Filters**: `is_resolved = false`, urgency level

### 3. Sales Performance
- **Table**: `analytics.product_sales_metrics`
- **Charts**: Revenue over time (line), top products by revenue (horizontal bar), 7-day vs 30-day avg sales (combo)

### 4. Inventory Turnover
- **Table**: `analytics.inventory_turnover`
- **Charts**: Turnover ratio by product (bar), days-inventory-outstanding trend (line)

### 5. Supplier Scorecard
- **Table**: `analytics.supplier_performance`
- **Charts**: On-time % gauge, late orders trend, fulfillment rate table

### 6. ABC Classification
- **Table**: `analytics.abc_classification`
- **Charts**: Revenue distribution A/B/C (pie), product list with class filter

### 7. Category Weekly Trends
- **Table**: `analytics.category_summary`
- **Charts**: Inventory value by category over time (stacked area), revenue by category (bar)

---

## Useful saved questions

```sql
-- Current stock health (Metabase question)
SELECT
    category,
    COUNT(*) AS total_skus,
    SUM(CASE WHEN is_below_reorder THEN 1 ELSE 0 END) AS below_reorder,
    ROUND(AVG(days_of_supply), 1) AS avg_days_supply,
    SUM(inventory_value) AS total_value
FROM analytics.daily_inventory_summary
WHERE summary_date = CURRENT_DATE
GROUP BY category
ORDER BY total_value DESC;
```
