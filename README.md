# Mini DWH

Local mini data warehouse project using Python, SQL Server, and a Bronze/Silver/Gold architecture.

## Environment

- OS: Windows
- IDE: VS Code Dev Container
- Runtime: Python 3.11
- Database: SQL Server 2022 in Docker

## Critical connection detail

From inside the Dev Container, SQL Server is accessed through:

```text
host.docker.internal,1433

Layers
Bronze

Raw delivery-aware ingestion layer.

Main table:

bronze.olist_orders_raw

Key concepts:

batch_id = technical pipeline run
delivery_id = file/API delivery
source_record_id = business key
Bronze is append-only by delivery
duplicate deliveries are skipped or marked
reload creates a new delivery, does not delete old Bronze rows
Silver

Business-grain cleaned layer.

Tables:

silver.orders_current
silver.orders_history

orders_current is SCD1-style current state.

orders_history is SCD2-style observed history with:

valid_from_utc
valid_to_utc
is_current
row_hash
Gold

Analytics marts.

Tables:

gold.daily_orders

Built from silver.orders_current.

Pipelines
Ingest Olist orders
python -m src.pipelines.ingest_olist_orders

Reload mode:

python -m src.pipelines.ingest_olist_orders --mode RELOAD
Build Silver orders
python -m src.pipelines.build_silver_orders
Build Gold daily orders
python -m src.pipelines.build_gold_daily_orders
Current validated behavior
Bronze loads Olist orders
Reload appends a new delivery
Silver current keeps one row per order
Silver history creates a new SCD2 version when tracked fields change
Silver delivery processing is idempotent
Gold daily orders aggregates match Silver totals

Validation queries
USE mini_dwh;

SELECT COUNT(*) FROM silver.orders_current;

SELECT COUNT(*) FROM silver.orders_history;

SELECT
    SUM(orders_count) AS gold_total
FROM gold.daily_orders;

SELECT
    COUNT(*) AS silver_total
FROM silver.orders_current
WHERE order_purchase_timestamp IS NOT NULL;