from src.common.db import get_connection
from src.etl.batch_log import start_batch, finish_batch_success, finish_batch_failure


PIPELINE_NAME = "build_gold_daily_orders"
SOURCE_NAME = "silver.orders_current"


DDL_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;

IF OBJECT_ID('gold.daily_orders', 'U') IS NULL
BEGIN
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
END;
"""


BUILD_SQL = """
TRUNCATE TABLE gold.daily_orders;

INSERT INTO gold.daily_orders (
    order_date,
    orders_count,
    delivered_count,
    canceled_count,
    shipped_count,
    approved_count,
    invoiced_count,
    unavailable_count,
    processing_count,
    gold_updated_at_utc
)
SELECT
    CAST(order_purchase_timestamp AS DATE) AS order_date,
    COUNT(*) AS orders_count,
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_count,
    SUM(CASE WHEN order_status = 'shipped' THEN 1 ELSE 0 END) AS shipped_count,
    SUM(CASE WHEN order_status = 'approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN order_status = 'invoiced' THEN 1 ELSE 0 END) AS invoiced_count,
    SUM(CASE WHEN order_status = 'unavailable' THEN 1 ELSE 0 END) AS unavailable_count,
    SUM(CASE WHEN order_status = 'processing' THEN 1 ELSE 0 END) AS processing_count,
    GETUTCDATE() AS gold_updated_at_utc
FROM silver.orders_current
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY CAST(order_purchase_timestamp AS DATE);
"""


COUNT_SQL = "SELECT COUNT(*) FROM gold.daily_orders;"


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
        print("Ensured gold.daily_orders exists")

        cursor.execute(BUILD_SQL)
        conn.commit()
        print("Built gold.daily_orders")

        cursor.execute(COUNT_SQL)
        rows_loaded = cursor.fetchone()[0]

        finish_batch_success(
            cursor,
            batch_id=batch_id,
            rows_extracted=rows_loaded,
            rows_loaded=rows_loaded,
            rows_skipped=0,
        )
        conn.commit()

        print(f"Pipeline succeeded: rows_loaded={rows_loaded}")

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