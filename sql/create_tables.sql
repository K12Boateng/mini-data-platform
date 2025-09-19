-- sql/create_tables.sql

CREATE TABLE IF NOT EXISTS sales (
    sale_id TEXT PRIMARY KEY,
    sale_date TIMESTAMP,
    customer_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    amount NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sales_sale_date ON sales (sale_date);

-- tracking table for ingestion
CREATE TABLE IF NOT EXISTS file_ingestion_log (
    file_key TEXT PRIMARY KEY,
    bucket TEXT NOT NULL,
    detected_at TIMESTAMP DEFAULT now(),
    status TEXT NOT NULL, -- pending/validated/validation_failed/processing_failed/loaded/failed
    rows_processed INTEGER,
    error TEXT,
    updated_at TIMESTAMP DEFAULT now()
);
