# Architecture

## Overview

The project implements a local mini-DWH with three layers:

```
Source files / APIs
    ↓
Bronze - raw delivery-aware ingestion (append-only)
    ↓
Silver - cleaned business-grain tables (SCD1/SCD2)
    ↓
Gold - analytics-ready marts
```

### Core Concepts

| ID               | Meaning                    |
| ---------------- | -------------------------- |
| `batch_id`       | Technical pipeline run ID  |
| `delivery_id`    | Source artifact ID (file/API pull) |
| `source_record_id` | Business key from source |

---

## Layer Principles

### Bronze Layer

**Principles:**
- Append-only by delivery
- Tracks lineage (batch, delivery, source)
- Stores both raw JSON and parsed key fields
- Duplicate files detected by content hash
- Reload creates new delivery, keeps old Bronze rows

**Process:**
1. Read source file/API
2. Parse and validate records
3. Filter out duplicates (same source + source_record_id)
4. Insert into bronze raw table
5. Mark delivery as LOADED

### Silver Layer

**Principles:**
- Business-grain cleaned tables
- For mutable entities, maintain two tables:
  - **Current (SCD1)** — one row per business key, latest state
  - **History (SCD2)** — all observed versions with valid_from/valid_to dates
- Change detection via row_hash on tracked fields
- Delivery-aware processing (idempotent)

**Process:**
1. Read Bronze delivery
2. Join/enrich with existing Silver current
3. Detect changes via row_hash
4. For changes:
   - Close old Silver history row
   - Insert new Silver history row
   - Update Silver current
5. Mark delivery as processed in `etl.silver_delivery_log`

### Gold Layer

**Principles:**
- Business-facing analytics marts
- Source from Silver (not Bronze)
- Can be full-refresh or incremental
- Domain-specific aggregations

**Example: Daily Orders**
- Source: `silver.orders_current`
- Grain: one row per date
- Metrics: order count, revenue, etc.

---

## Current Implemented Flows

### Orders Flow
```
data/olist/olist_orders_dataset.csv
    ↓ (ingest_olist_orders)
bronze.olist_orders_raw
    ↓ (build_silver_orders)
silver.orders_current (SCD1 — latest state)
silver.orders_history (SCD2 — all versions)
    ↓ (build_gold_daily_orders)
gold.daily_orders (daily aggregates)
```

### Customers Flow
```
data/olist/olist_customers_dataset.csv
    ↓ (ingest_olist_customers)
bronze.olist_customers_raw
    ↓ (build_silver_customers)
silver.customers_current (SCD1 — latest state)
silver.customers_history (SCD2 — all versions)
    ↓ (build_gold_customers)
gold.customers_current
gold.customers_history (analytics marts)
```

### GitHub Issues Flow
```
GitHub REST API /repos/dbt-labs/dbt-core/issues (last 90 days by default)
    ↓ (ingest_github_issues)
bronze.github_issues_raw
    ↓ (build_silver_github_issues)
silver.github_issues_current (SCD1 — latest state)
silver.github_issues_history (SCD2 — all versions)
    ↓ (build_gold_github_issue_metrics)
gold.daily_open_issues
gold.issue_resolution_time
```

See [github-issues-pipeline.md](github-issues-pipeline.md) for acceptance criteria and validation.

---

## Deployment & Initialization

### One-Time Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Initialize database schema
python init_database.py
```

This creates:
- All schemas: `etl`, `bronze`, `silver`, `gold`
- ETL tracking infrastructure
- All raw and processed tables
- Performance indexes

### Regular Execution

See [README.md](../README.md) for pipeline execution order and validation steps.