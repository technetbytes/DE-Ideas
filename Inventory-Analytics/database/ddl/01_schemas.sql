-- ============================================================
-- 01_schemas.sql  –  Create schema layers
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA raw      IS 'Landing zone: source data loaded as-is';
COMMENT ON SCHEMA staging  IS 'Cleaned and validated records';
COMMENT ON SCHEMA analytics IS 'Aggregated KPIs and reporting tables';
