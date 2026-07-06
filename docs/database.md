# Database Documentation

Target database:

```sql
USE mini_dwh;
```

## Schemas

```sql
CREATE SCHEMA etl;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

---

# ETL Schema

## etl.batch_run

Purpose: tracks each technical pipeline execution.

```sql
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
```

## etl.source_delivery

Purpose: tracks each source artifact or API pull.

```sql
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
```

Statuses:

- `RECEIVED`
- `LOADED`
- `SKIPPED_DUPLICATE`
- `SUPERSEDED`
- `FAILED`

## etl.silver_delivery_log

Purpose: prevents reprocessing the same delivery into Silver.

```sql
CREATE TABLE etl.silver_delivery_log (
    silver_table_name NVARCHAR(100) NOT NULL,
    delivery_id BIGINT NOT NULL,
    processed_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    status NVARCHAR(20) NOT NULL,
    PRIMARY KEY (silver_table_name, delivery_id)
);
```

---

# Bronze Schema

## bronze.olist_orders_raw

Purpose: raw + parsed delivery-level Olist orders.

```sql
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

    extracted_at_utc DATETIME2 NOT NULL
);
```

Recommended indexes:

```sql
CREATE INDEX IX_olist_orders_raw_delivery_id
ON bronze.olist_orders_raw(delivery_id);

CREATE INDEX IX_olist_orders_raw_source_record
ON bronze.olist_orders_raw(source_name, source_record_id);

CREATE INDEX IX_olist_orders_raw_order_delivery
ON bronze.olist_orders_raw(order_id, delivery_id);
```

Principles:

- Bronze is append-only by delivery.
- `RELOAD` creates a new delivery.
- Old Bronze rows are not deleted.
- Duplicate files are detected by `content_hash`.

## bronze.olist_customers_raw

Purpose: raw + parsed delivery-level Olist customers.

```sql
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

    extracted_at_utc DATETIME2 NOT NULL
);
```

Recommended indexes:

```sql
CREATE INDEX IX_olist_customers_raw_delivery_id
ON bronze.olist_customers_raw(delivery_id);

CREATE INDEX IX_olist_customers_raw_source_record
ON bronze.olist_customers_raw(source_name, source_record_id);

CREATE INDEX IX_olist_customers_raw_customer_delivery
ON bronze.olist_customers_raw(customer_id, delivery_id);
```

---

# Silver Schema

## silver.orders_current

Purpose: current-state order table. SCD1-style.

Grain: one row per `order_id`.

```sql
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
```

## silver.orders_history

Purpose: SCD2 observed history of order state.

Grain: one row per observed version of `order_id`.

```sql
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
```

Indexes:

```sql
CREATE INDEX IX_orders_history_order_id
ON silver.orders_history(order_id);

CREATE INDEX IX_orders_history_current
ON silver.orders_history(order_id, is_current);
```

SCD2 behavior:

- If hash is unchanged: do nothing.
- If hash changed:
  - close old row with `valid_to_utc`
  - set old row `is_current = 0`
  - insert new row with `is_current = 1`

Tracked fields:

- `order_id`
- `customer_id`
- `order_status`
- `order_purchase_timestamp`

Technical fields like `batch_id` and `delivery_id` are lineage fields, not change drivers.

## silver.customers_current

Purpose: current-state customer table. SCD1-style.

Grain: one row per `customer_id`.

```sql
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
```

## silver.customers_history

Purpose: SCD2 observed history of customer attributes.

Grain: one row per observed version of `customer_id`.

```sql
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
```

Indexes:

```sql
CREATE INDEX IX_customers_history_customer_id
ON silver.customers_history(customer_id);

CREATE INDEX IX_customers_history_current
ON silver.customers_history(customer_id, is_current);
```

SCD2 behavior matches `silver.orders_history`.

Tracked fields:

- `customer_id`
- `customer_unique_id`
- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

---

# Gold Schema

## gold.daily_orders

Purpose: daily analytical mart based on `silver.orders_current`.

Grain: one row per `order_date`.

```sql
CREATE TABLE gold.daily_orders (
    order_date DATE NOT NULL PRIMARY KEY,
    orders_count INT NOT NULL,
    delivered_count INT NOT NULL,
    canceled_count INT NOT NULL,
    shipped_count INT NOT NULL,
    approved_count INT NOT NULL,
    invoiced_count INT NOT NULL,
    unavailable_count INT NOT NULL,
    processing_count INT NOT NULL,
    gold_updated_at_utc DATETIME2 NOT NULL
);
```

Source:

```sql
silver.orders_current
```

Validation:

```sql
SELECT SUM(orders_count) AS gold_total
FROM gold.daily_orders;

SELECT COUNT(*) AS silver_total
FROM silver.orders_current
WHERE order_purchase_timestamp IS NOT NULL;
```

These numbers must match.

## gold.customers_current

Purpose: BI-facing current customer dimension; full refresh from `silver.customers_current`.

Grain: one row per `customer_id`.

```sql
CREATE TABLE gold.customers_current (
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

    gold_updated_at_utc DATETIME2 NOT NULL
);
```

Source:

```sql
silver.customers_current
```

## gold.customers_history

Purpose: BI-facing SCD2 customer history; full refresh from `silver.customers_history`. Surrogate keys match Silver.

Grain: one row per observed version of `customer_id`.

```sql
CREATE TABLE gold.customers_history (
    customer_history_sk BIGINT NOT NULL PRIMARY KEY,
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

    created_at_utc DATETIME2 NOT NULL,
    gold_updated_at_utc DATETIME2 NOT NULL
);
```

Indexes (recommended):

```sql
CREATE INDEX IX_gold_customers_history_customer_id
ON gold.customers_history(customer_id);

CREATE INDEX IX_gold_customers_history_current
ON gold.customers_history(customer_id, is_current);
```

Source:

```sql
silver.customers_history
```

Validation (row counts must match after each Gold build):

```sql
SELECT COUNT(*) AS gold_current
FROM gold.customers_current;

SELECT COUNT(*) AS silver_current
FROM silver.customers_current;

SELECT COUNT(*) AS gold_history
FROM gold.customers_history;

SELECT COUNT(*) AS silver_history
FROM silver.customers_history;
```

---

# GitHub Issues Tables

## bronze.github_issues_raw

Purpose: raw + parsed delivery-level GitHub issues from REST API.

```sql
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

    extracted_at_utc DATETIME2 NOT NULL
);
```

Recommended indexes:

```sql
CREATE INDEX IX_github_issues_raw_delivery_id
ON bronze.github_issues_raw(delivery_id);

CREATE INDEX IX_github_issues_raw_source_record
ON bronze.github_issues_raw(source_name, source_record_id);

CREATE INDEX IX_github_issues_raw_issue_delivery
ON bronze.github_issues_raw(issue_number, delivery_id);
```

## silver.github_issues_current

Purpose: current-state issue table. SCD1-style.

Grain: one row per `issue_number`.

```sql
CREATE TABLE silver.github_issues_current (
    issue_number INT NOT NULL PRIMARY KEY,
    title NVARCHAR(500) NULL,
    state NVARCHAR(20) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    closed_at DATETIME2 NULL,
    user_login NVARCHAR(100) NULL,
    labels_json NVARCHAR(MAX) NULL,

    row_hash NVARCHAR(64) NOT NULL,

    source_name NVARCHAR(100) NOT NULL,
    source_record_id NVARCHAR(100) NOT NULL,
    delivery_id BIGINT NOT NULL,
    bronze_batch_id BIGINT NOT NULL,

    silver_updated_at_utc DATETIME2 NOT NULL
);
```

## silver.github_issues_history

Purpose: SCD2 observed history of issue attributes.

```sql
CREATE TABLE silver.github_issues_history (
    issue_history_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    issue_number INT NOT NULL,
    title NVARCHAR(500) NULL,
    state NVARCHAR(20) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    closed_at DATETIME2 NULL,
    user_login NVARCHAR(100) NULL,
    labels_json NVARCHAR(MAX) NULL,

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
```

Tracked fields for `row_hash`:

- `title`, `state`, `labels_json`, `user_login`, `closed_at`

## gold.daily_open_issues

Purpose: daily count of open issues (from SCD2 history).

Grain: one row per `snapshot_date`.

```sql
CREATE TABLE gold.daily_open_issues (
    snapshot_date DATE NOT NULL PRIMARY KEY,
    open_issues_count INT NOT NULL,
    gold_updated_at_utc DATETIME2 NOT NULL
);
```

## gold.issue_resolution_time

Purpose: resolution time for closed issues.

Grain: one row per closed `issue_number`.

```sql
CREATE TABLE gold.issue_resolution_time (
    issue_number INT NOT NULL PRIMARY KEY,
    title NVARCHAR(500) NULL,
    user_login NVARCHAR(100) NULL,
    created_at DATETIME2 NOT NULL,
    closed_at DATETIME2 NOT NULL,
    resolution_days INT NOT NULL,
    gold_updated_at_utc DATETIME2 NOT NULL
);
```

Validation:

```sql
SELECT COUNT(*) AS silver_closed
FROM silver.github_issues_current
WHERE state = 'closed' AND closed_at IS NOT NULL;

SELECT COUNT(*) AS gold_resolution
FROM gold.issue_resolution_time;
```

---

# Pipeline Order

Orders (through Gold):

```text
ingest_olist_orders
        ↓
build_silver_orders
        ↓
build_gold_daily_orders
```

Customers (through Gold):

```text
ingest_olist_customers
        ↓
build_silver_customers
        ↓
build_gold_customers
```

Commands:

```bash
cd /workspaces/mini-dwh
export PYTHONPATH=$(pwd)

python -m src.pipelines.ingest_olist_orders --mode RELOAD
python -m src.pipelines.build_silver_orders
python -m src.pipelines.build_gold_daily_orders

python -m src.pipelines.ingest_olist_customers --mode RELOAD
python -m src.pipelines.build_silver_customers
python -m src.pipelines.build_gold_customers
```

GitHub Issues (through Gold):

```text
ingest_github_issues
        ↓
build_silver_github_issues
        ↓
build_gold_github_issue_metrics
```

Commands:

```bash
python -m src.pipelines.ingest_github_issues
python -m src.pipelines.build_silver_github_issues
python -m src.pipelines.build_gold_github_issue_metrics
```