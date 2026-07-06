# Mini DWH

Local mini data warehouse project using Python, SQL Server, and a Bronze/Silver/Gold architecture.

## Environment

- OS: Windows / Linux
- IDE: VS Code Dev Container
- Runtime: Python 3.11
- Database: SQL Server 2022

## Critical connection detail

SQL Server is accessed through:

```text
host.docker.internal,1433
```

Database credentials (from `.env`):
- Server: `host.docker.internal,1433`
- Database: `mini_dwh`
- User: `sa`
- Password: `YourStrong!Passw0rd123`

## Setup & Installation

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Initialize Database Schema

Before running any pipelines, initialize the database with all required schemas and tables:

```bash
python init_database.py
```

This creates:
- Schemas: `etl`, `bronze`, `silver`, `gold`
- ETL tracking tables: `etl.batch_run`, `etl.source_delivery`, `etl.silver_delivery_log`
- Bronze raw tables: `bronze.olist_orders_raw`, `bronze.olist_customers_raw`
- Silver current & history tables for orders and customers
- All necessary indexes for performance
## Layers

### Bronze

Raw delivery-aware ingestion layer.

**Main tables:**
- `bronze.olist_orders_raw` — raw orders from CSV
- `bronze.olist_customers_raw` — raw customers from CSV

**Key concepts:**
- `batch_id` = technical pipeline run ID
- `delivery_id` = source artifact ID (file/API pull)
- `source_record_id` = business key
- Bronze is **append-only by delivery**
- Duplicate deliveries are detected and skipped
- Reload creates a new delivery, does not delete old Bronze rows

### Silver

Business-grain cleaned layer with SCD (Slowly Changing Dimension) support.

**Tables:**
- `silver.orders_current` — SCD1-style current state (one row per order)
- `silver.orders_history` — SCD2-style history with valid_from/valid_to dates
- `silver.customers_current` — SCD1-style current state (one row per customer)
- `silver.customers_history` — SCD2-style history with valid_from/valid_to dates

**History tracking includes:**
- `valid_from_utc` — when this version became active
- `valid_to_utc` — when this version became inactive
- `is_current` — flag for current version
- `row_hash` — hash of business fields to detect changes

### Gold

Analytics-ready business marts.

**Tables:**
- `gold.daily_orders` — daily order metrics (built from `silver.orders_current`)
- `gold.customers_current` — current customer dimension
- `gold.customers_history` — historical customer records

---

## Pipelines

### Bronze Ingestion Pipelines

These pipelines load CSV data into Bronze tables. Run these first.

**Ingest Olist Orders:**
```bash
python -m src.pipelines.ingest_olist_orders
```

Reload mode (clears and reloads all data):
```bash
python -m src.pipelines.ingest_olist_orders --mode RELOAD
```

**Ingest Olist Customers:**
```bash
python -m src.pipelines.ingest_olist_customers
```

Reload mode:
```bash
python -m src.pipelines.ingest_olist_customers --mode RELOAD
```

### Silver Transformation Pipelines

These pipelines transform Bronze data into business-grain Silver tables.

**Build Silver Orders:**
```bash
python -m src.pipelines.build_silver_orders
```

**Build Silver Customers:**
```bash
python -m src.pipelines.build_silver_customers
```

### Gold Analytics Pipelines

These pipelines build analytics-ready marts from Silver tables.

**Build Gold Daily Orders:**
```bash
python -m src.pipelines.build_gold_daily_orders
```

**Build Gold Customers:**
```bash
python -m src.pipelines.build_gold_customers
```

---

## Recommended Execution Order

1. **Initialize database** (one-time):
   ```bash
   python init_database.py
   ```

2. **Load Bronze layer:**
   ```bash
   python -m src.pipelines.ingest_olist_orders
   python -m src.pipelines.ingest_olist_customers
   ```

3. **Transform to Silver:**
   ```bash
   python -m src.pipelines.build_silver_orders
   python -m src.pipelines.build_silver_customers
   ```

4. **Build Gold analytics marts:**
   ```bash
   python -m src.pipelines.build_gold_daily_orders
   python -m src.pipelines.build_gold_customers
   ```

---

## Current Data Flows

### Orders Flow
```
data/olist/olist_orders_dataset.csv
    ↓
bronze.olist_orders_raw (raw ingestion)
    ↓
silver.orders_current (latest state - SCD1)
silver.orders_history (full history - SCD2)
    ↓
gold.daily_orders (analytics mart)
```

### Customers Flow
```
data/olist/olist_customers_dataset.csv
    ↓
bronze.olist_customers_raw (raw ingestion)
    ↓
silver.customers_current (latest state - SCD1)
silver.customers_history (full history - SCD2)
    ↓
gold.customers_current
gold.customers_history (analytics marts)
```

---

## Validation

After running the full pipeline, verify data integrity with these queries:

```sql
USE mini_dwh;

-- Check Bronze loads
SELECT COUNT(*) as bronze_orders FROM bronze.olist_orders_raw;
SELECT COUNT(*) as bronze_customers FROM bronze.olist_customers_raw;

-- Check Silver current tables
SELECT COUNT(*) as silver_orders_current FROM silver.orders_current;
SELECT COUNT(*) as silver_customers_current FROM silver.customers_current;

-- Check Silver history tables
SELECT COUNT(*) as silver_orders_history FROM silver.orders_history;
SELECT COUNT(*) as silver_customers_history FROM silver.customers_history;

-- Check Gold totals
SELECT SUM(orders_count) AS gold_orders_total FROM gold.daily_orders;
SELECT COUNT(*) as gold_customers_current FROM gold.customers_current;
SELECT COUNT(*) as gold_customers_history FROM gold.customers_history;

-- Verify all current records exist in history
SELECT COUNT(*) FROM silver.orders_current c
WHERE NOT EXISTS (
    SELECT 1 FROM silver.orders_history h 
    WHERE h.order_id = c.order_id AND h.is_current = 1
);
```

---

## AI-Assisted Workflow

This project uses three Cursor subagents in [`.cursor/agents/`](.cursor/agents/):

| Agent | Invoke | Role |
|-------|--------|------|
| Architect | `@pipeline-architect` | Designs tables, data flows, acceptance criteria (docs only) |
| Developer | `@pipeline-developer` | Implements Bronze/Silver/Gold pipelines |
| QA | `@pipeline-qa` | Runs pipelines and validates row counts / integrity |

Workflow: Architect designs → Developer implements → QA validates.

---

## Validated Behavior

- ✅ Bronze loads Olist orders and customers
- ✅ Reload mode appends a new delivery without deleting old Bronze rows
- ✅ Silver current maintains one row per business key
- ✅ Silver history creates new SCD2 versions when tracked fields change
- ✅ Silver delivery processing is idempotent (safe to re-run)
- ✅ Gold daily orders totals match Silver current totals
- ✅ All current records have corresponding history records