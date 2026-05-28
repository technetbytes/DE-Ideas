-- Stock Ticks Database Schema
-- Optimized for high-throughput writes and time-series queries

-- Main tick data table with partitioning-ready structure
CREATE TABLE IF NOT EXISTS stock_ticks (
    id BIGSERIAL,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    sequence BIGINT NOT NULL,
    price NUMERIC(12, 4) NOT NULL,
    bid NUMERIC(12, 4) NOT NULL,
    ask NUMERIC(12, 4) NOT NULL,
    spread NUMERIC(8, 4) NOT NULL,
    volume INTEGER NOT NULL,
    volume_24h BIGINT NOT NULL,
    change_pct NUMERIC(8, 4),
    volatility NUMERIC(8, 6),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT stock_ticks_unique UNIQUE (symbol, timestamp_ms, sequence)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_stock_ticks_symbol_time
    ON stock_ticks (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_stock_ticks_timestamp
    ON stock_ticks (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_stock_ticks_symbol
    ON stock_ticks (symbol);

-- Aggregated OHLCV view (1-second bars)
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_ohlcv_1s AS
SELECT
    symbol,
    date_trunc('second', timestamp) AS bar_time,
    (array_agg(price ORDER BY timestamp ASC))[1] AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    (array_agg(price ORDER BY timestamp DESC))[1] AS close,
    SUM(volume) AS volume,
    COUNT(*) AS tick_count,
    AVG(spread) AS avg_spread
FROM stock_ticks
GROUP BY symbol, date_trunc('second', timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_1s_symbol_time
    ON stock_ohlcv_1s (symbol, bar_time);

-- Aggregated OHLCV view (1-minute bars)
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_ohlcv_1m AS
SELECT
    symbol,
    date_trunc('minute', timestamp) AS bar_time,
    (array_agg(price ORDER BY timestamp ASC))[1] AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    (array_agg(price ORDER BY timestamp DESC))[1] AS close,
    SUM(volume) AS volume,
    COUNT(*) AS tick_count,
    AVG(spread) AS avg_spread,
    AVG(volatility) AS avg_volatility
FROM stock_ticks
GROUP BY symbol, date_trunc('minute', timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_1m_symbol_time
    ON stock_ohlcv_1m (symbol, bar_time);

-- Summary statistics table (updated periodically)
CREATE TABLE IF NOT EXISTS stock_summary (
    symbol VARCHAR(10) PRIMARY KEY,
    last_price NUMERIC(12, 4),
    last_updated TIMESTAMPTZ,
    high_24h NUMERIC(12, 4),
    low_24h NUMERIC(12, 4),
    volume_24h BIGINT,
    tick_count_24h BIGINT,
    avg_spread NUMERIC(8, 4),
    change_pct_24h NUMERIC(8, 4)
);

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_ohlcv_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY stock_ohlcv_1s;
    REFRESH MATERIALIZED VIEW CONCURRENTLY stock_ohlcv_1m;
END;
$$ LANGUAGE plpgsql;

-- Data retention: auto-delete ticks older than 7 days
CREATE OR REPLACE FUNCTION cleanup_old_ticks()
RETURNS void AS $$
BEGIN
    DELETE FROM stock_ticks WHERE timestamp < NOW() - INTERVAL '7 days';
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO stockuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO stockuser;
GRANT USAGE ON SCHEMA public TO stockuser;
