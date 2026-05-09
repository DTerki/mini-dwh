# Architecture

## Overview

The project implements a local mini-DWH with three layers:

```text
Source files / APIs
        ↓
Bronze - raw delivery-aware ingestion
        ↓
Silver - cleaned business-grain tables
        ↓
Gold - analytics-ready marts

Core IDs:
| ID               | Meaning                    |
| ---------------- | -------------------------- |
| batch_id         | technical pipeline run     |
| delivery_id      | source artifact / API pull |
| source_record_id | source business key        |

Bronze principles

Bronze is append-only by delivery.

A reload does not delete old Bronze rows. Instead:

a new delivery is created
the new rows are inserted
previous loaded deliveries can be marked SUPERSEDED
Silver chooses what to process
Silver principles

Silver exposes business-level structures.

For mutable entities, use both:

current table - SCD1-style latest state
history table - SCD2-style observed history
Gold principles

Gold tables are business-facing aggregates.

Gold should use Silver as its source, not Bronze.

Current implemented flow
data/olist/olist_orders_dataset.csv
        ↓
bronze.olist_orders_raw
        ↓
silver.orders_current
silver.orders_history
        ↓
gold.daily_orders