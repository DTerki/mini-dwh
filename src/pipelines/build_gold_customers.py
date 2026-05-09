from src.common.db import get_connection
from src.etl.batch_log import start_batch, finish_batch_success, finish_batch_failure


PIPELINE_NAME = "build_gold_customers"
SOURCE_NAME = "silver.customers_current"


DDL_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;

IF OBJECT_ID('gold.customers_current', 'U') IS NULL
BEGIN
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
END;

IF OBJECT_ID('gold.customers_history', 'U') IS NULL
BEGIN
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

    CREATE INDEX IX_gold_customers_history_customer_id
        ON gold.customers_history(customer_id);

    CREATE INDEX IX_gold_customers_history_current
        ON gold.customers_history(customer_id, is_current);
END;
"""


BUILD_SQL = """
TRUNCATE TABLE gold.customers_history;
TRUNCATE TABLE gold.customers_current;

INSERT INTO gold.customers_current (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    row_hash,
    source_name,
    source_record_id,
    delivery_id,
    bronze_batch_id,
    gold_updated_at_utc
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    row_hash,
    source_name,
    source_record_id,
    delivery_id,
    bronze_batch_id,
    GETUTCDATE() AS gold_updated_at_utc
FROM silver.customers_current;

INSERT INTO gold.customers_history (
    customer_history_sk,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    row_hash,
    valid_from_utc,
    valid_to_utc,
    is_current,
    source_name,
    source_record_id,
    delivery_id,
    bronze_batch_id,
    created_at_utc,
    gold_updated_at_utc
)
SELECT
    customer_history_sk,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    row_hash,
    valid_from_utc,
    valid_to_utc,
    is_current,
    source_name,
    source_record_id,
    delivery_id,
    bronze_batch_id,
    created_at_utc,
    GETUTCDATE() AS gold_updated_at_utc
FROM silver.customers_history;
"""


COUNT_CURRENT_SQL = "SELECT COUNT(*) FROM gold.customers_current;"
COUNT_HISTORY_SQL = "SELECT COUNT(*) FROM gold.customers_history;"


def main():
    conn = get_connection()
    cursor = conn.cursor()
    batch_id = None

    try:
        print(f"Starting pipeline: {PIPELINE_NAME}")

        batch_id = start_batch(cursor, PIPELINE_NAME, SOURCE_NAME)
        conn.commit()
        print(f"Created batch_id={batch_id}")

        cursor.execute(DDL_SQL)
        conn.commit()
        print("Ensured gold.customers_current and gold.customers_history exist")

        cursor.execute(BUILD_SQL)
        conn.commit()
        print("Built gold.customers_current and gold.customers_history")

        cursor.execute(COUNT_CURRENT_SQL)
        current_rows = cursor.fetchone()[0]

        cursor.execute(COUNT_HISTORY_SQL)
        history_rows = cursor.fetchone()[0]

        total_rows = current_rows + history_rows

        finish_batch_success(
            cursor,
            batch_id=batch_id,
            rows_extracted=total_rows,
            rows_loaded=total_rows,
            rows_skipped=0,
        )
        conn.commit()

        print(
            f"Pipeline succeeded: current_rows={current_rows}, "
            f"history_rows={history_rows}"
        )

    except Exception as e:
        conn.rollback()

        if batch_id is not None:
            try:
                finish_batch_failure(cursor, batch_id=batch_id, error_message=str(e))
                conn.commit()
            except Exception:
                pass

        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
