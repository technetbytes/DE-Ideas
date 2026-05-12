CREATE TABLE IF NOT EXISTS fact_prices (

    id SERIAL PRIMARY KEY,

    store VARCHAR(100),

    product_name VARCHAR(255),

    brand VARCHAR(100),

    category VARCHAR(100),

    price NUMERIC(10,2),

    scraped_at TIMESTAMP
);