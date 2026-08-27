-- DDL for the Star Schema Data Warehouse
-- Run this in your PostgreSQL database before triggering the pipeline!

-- 1. Create the Dimension tables first (since the Fact table depends on them)
CREATE TABLE dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(255),
    email VARCHAR(255)
);

CREATE TABLE dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100)
);

-- 2. Create the Fact table
-- Includes foreign keys for integrity and a batch_id so we can easily track/rollback specific pipeline runs
CREATE TABLE fact_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES dim_customer(customer_id),
    product_id VARCHAR(50) REFERENCES dim_product(product_id),
    order_date TIMESTAMP,
    amount NUMERIC(10, 2),
    batch_id VARCHAR(255)
);

-- 3. The Quarantine Table
-- JSONB is a lifesaver here. We dump the raw failing row directly into JSON 
-- so it doesn't break schema rules, making it super easy to debug later.
CREATE TABLE quarantine_sales (
    raw_record JSONB,
    error_reason TEXT,
    batch_id VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
