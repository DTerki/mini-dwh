#!/usr/bin/env python
"""
Initialize the mini DWH database schema.
This script creates all required schemas and tables based on database.md.
"""

import sys
from src.common.db import get_connection

# SQL commands to initialize the database schema
INIT_SQL = """
-- Create schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl')
    CREATE SCHEMA etl;
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    CREATE SCHEMA bronze;
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    CREATE SCHEMA silver;
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    CREATE SCHEMA gold;

-- Create ETL tables
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('etl') AND name = 'batch_run')
CREATE TABLE etl.batch_run (
    batch_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name NVARCHAR(100) NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    start_ts DATETIME2 NOT NULL,
    end_ts DATETIME2 NULL,
    status NVARCHAR(20) NOT NULL,
    rows_extracted INT NULL,
    rows_loaded INT NULL,
    rows_skipped INT NULL,
    error_message NVARCHAR(MAX) NULL
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('etl') AND name = 'source_delivery')
CREATE TABLE etl.source_delivery (
    delivery_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_name NVARCHAR(100) NOT NULL,
    delivery_type NVARCHAR(20) NOT NULL,
    source_object_name NVARCHAR(255) NOT NULL,
    snapshot_date DATE NULL,
    received_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    content_hash NVARCHAR(64) NULL,
    batch_id BIGINT NOT NULL,
    status NVARCHAR(50) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('etl') AND name = 'silver_delivery_log')
CREATE TABLE etl.silver_delivery_log (
    silver_table_name NVARCHAR(100) NOT NULL,
    delivery_id BIGINT NOT NULL,
    processed_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    status NVARCHAR(20) NOT NULL,
    PRIMARY KEY (silver_table_name, delivery_id)
);

-- Create Bronze tables
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('bronze') AND name = 'olist_orders_raw')
CREATE TABLE bronze.olist_orders_raw (
    bronze_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    delivery_id BIGINT NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    payload_json NVARCHAR(MAX) NOT NULL,
    order_id NVARCHAR(100) NULL,
    customer_id NVARCHAR(100) NULL,
    order_status NVARCHAR(50) NULL,
    order_purchase_timestamp DATETIME2 NULL,
    extracted_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('bronze') AND name = 'olist_customers_raw')
CREATE TABLE bronze.olist_customers_raw (
    bronze_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    delivery_id BIGINT NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    payload_json NVARCHAR(MAX) NOT NULL,
    customer_id NVARCHAR(100) NULL,
    customer_unique_id NVARCHAR(100) NULL,
    customer_zip_code_prefix NVARCHAR(20) NULL,
    customer_city NVARCHAR(100) NULL,
    customer_state NVARCHAR(10) NULL,
    extracted_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Bronze indexes
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.olist_orders_raw') AND name = 'IX_olist_orders_raw_delivery_id')
CREATE INDEX IX_olist_orders_raw_delivery_id ON bronze.olist_orders_raw(delivery_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.olist_orders_raw') AND name = 'IX_olist_orders_raw_source_record')
CREATE INDEX IX_olist_orders_raw_source_record ON bronze.olist_orders_raw(source_name, source_record_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.olist_orders_raw') AND name = 'IX_olist_orders_raw_order_delivery')
CREATE INDEX IX_olist_orders_raw_order_delivery ON bronze.olist_orders_raw(order_id, delivery_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.olist_customers_raw') AND name = 'IX_olist_customers_raw_delivery_id')
CREATE INDEX IX_olist_customers_raw_delivery_id ON bronze.olist_customers_raw(delivery_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.olist_customers_raw') AND name = 'IX_olist_customers_raw_source_record')
CREATE INDEX IX_olist_customers_raw_source_record ON bronze.olist_customers_raw(source_name, source_record_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.olist_customers_raw') AND name = 'IX_olist_customers_raw_customer_delivery')
CREATE INDEX IX_olist_customers_raw_customer_delivery ON bronze.olist_customers_raw(customer_id, delivery_id);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('bronze') AND name = 'github_issues_raw')
CREATE TABLE bronze.github_issues_raw (
    bronze_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    delivery_id BIGINT NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    payload_json NVARCHAR(MAX) NOT NULL,
    issue_number INT NULL,
    title NVARCHAR(500) NULL,
    state NVARCHAR(20) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    closed_at DATETIME2 NULL,
    user_login NVARCHAR(100) NULL,
    labels_json NVARCHAR(MAX) NULL,
    extracted_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.github_issues_raw') AND name = 'IX_github_issues_raw_delivery_id')
CREATE INDEX IX_github_issues_raw_delivery_id ON bronze.github_issues_raw(delivery_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.github_issues_raw') AND name = 'IX_github_issues_raw_source_record')
CREATE INDEX IX_github_issues_raw_source_record ON bronze.github_issues_raw(source_name, source_record_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('bronze.github_issues_raw') AND name = 'IX_github_issues_raw_issue_delivery')
CREATE INDEX IX_github_issues_raw_issue_delivery ON bronze.github_issues_raw(issue_number, delivery_id);

-- Create Silver tables
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('silver') AND name = 'orders_current')
CREATE TABLE silver.orders_current (
    order_id NVARCHAR(100) NOT NULL PRIMARY KEY,
    customer_id NVARCHAR(100) NULL,
    order_status NVARCHAR(50) NULL,
    order_purchase_timestamp DATETIME2 NULL,
    row_hash NVARCHAR(64) NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    delivery_id BIGINT NOT NULL,
    bronze_batch_id BIGINT NOT NULL,
    silver_updated_at_utc DATETIME2 NOT NULL
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('silver') AND name = 'orders_history')
CREATE TABLE silver.orders_history (
    order_history_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_id NVARCHAR(100) NOT NULL,
    customer_id NVARCHAR(100) NULL,
    order_status NVARCHAR(50) NULL,
    order_purchase_timestamp DATETIME2 NULL,
    row_hash NVARCHAR(64) NOT NULL,
    valid_from_utc DATETIME2 NOT NULL,
    valid_to_utc DATETIME2 NULL,
    is_current BIT NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    delivery_id BIGINT NOT NULL,
    bronze_batch_id BIGINT NOT NULL,
    created_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('silver') AND name = 'customers_current')
CREATE TABLE silver.customers_current (
    customer_id NVARCHAR(100) NOT NULL PRIMARY KEY,
    customer_unique_id NVARCHAR(100) NULL,
    customer_zip_code_prefix NVARCHAR(20) NULL,
    customer_city NVARCHAR(100) NULL,
    customer_state NVARCHAR(10) NULL,
    row_hash NVARCHAR(64) NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    delivery_id BIGINT NOT NULL,
    bronze_batch_id BIGINT NOT NULL,
    silver_updated_at_utc DATETIME2 NOT NULL
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('silver') AND name = 'customers_history')
CREATE TABLE silver.customers_history (
    customer_history_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id NVARCHAR(100) NOT NULL,
    customer_unique_id NVARCHAR(100) NULL,
    customer_zip_code_prefix NVARCHAR(20) NULL,
    customer_city NVARCHAR(100) NULL,
    customer_state NVARCHAR(10) NULL,
    row_hash NVARCHAR(64) NOT NULL,
    valid_from_utc DATETIME2 NOT NULL,
    valid_to_utc DATETIME2 NULL,
    is_current BIT NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    delivery_id BIGINT NOT NULL,
    bronze_batch_id BIGINT NOT NULL,
    created_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);

-- Create Silver indexes
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('silver.orders_history') AND name = 'IX_orders_history_order_id')
CREATE INDEX IX_orders_history_order_id ON silver.orders_history(order_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('silver.orders_history') AND name = 'IX_orders_history_current')
CREATE INDEX IX_orders_history_current ON silver.orders_history(order_id, is_current);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('silver.customers_history') AND name = 'IX_customers_history_customer_id')
CREATE INDEX IX_customers_history_customer_id ON silver.customers_history(customer_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('silver.customers_history') AND name = 'IX_customers_history_current')
CREATE INDEX IX_customers_history_current ON silver.customers_history(customer_id, is_current);
"""

def init_database():
    """Initialize the database schema."""
    try:
        with get_connection() as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            
            print("[INIT] Starting database initialization...")
            
            # Split SQL into individual statements and execute them
            statements = [s.strip() for s in INIT_SQL.split(';') if s.strip()]
            
            for i, statement in enumerate(statements, 1):
                try:
                    cursor.execute(statement)
                    print(f"[INIT] Executed statement {i}/{len(statements)}")
                except Exception as stmt_err:
                    print(f"[INIT] Warning on statement {i}: {stmt_err}")
                    # Continue with next statement
            
            print("[INIT] Database schema initialized successfully!")
            return 0
    except Exception as e:
        print(f"[INIT ERROR] Failed to initialize database: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(init_database())
