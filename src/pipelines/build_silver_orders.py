from src.common.db import get_connection
from src.etl.batch_log import start_batch, finish_batch_success, finish_batch_failure


PIPELINE_NAME = "build_silver_orders"
SOURCE_NAME = "olist_orders"


DDL_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
BEGIN
    EXEC('CREATE SCHEMA etl');
END;

IF OBJECT_ID('silver.orders_current', 'U') IS NULL
BEGIN
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
END;

IF OBJECT_ID('silver.orders_history', 'U') IS NULL
BEGIN
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

    CREATE INDEX IX_orders_history_order_id
        ON silver.orders_history(order_id);

    CREATE INDEX IX_orders_history_current
        ON silver.orders_history(order_id, is_current);
END;

IF OBJECT_ID('etl.silver_delivery_log', 'U') IS NULL
BEGIN
    CREATE TABLE etl.silver_delivery_log (
        silver_table_name NVARCHAR(100) NOT NULL,
        delivery_id BIGINT NOT NULL,
        processed_at_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        status NVARCHAR(20) NOT NULL,
        PRIMARY KEY (silver_table_name, delivery_id)
    );
END;
"""


GET_UNPROCESSED_DELIVERIES_SQL = """
SELECT d.delivery_id
FROM etl.source_delivery d
WHERE d.source_name = ?
  AND d.status = 'LOADED'
  AND NOT EXISTS (
      SELECT 1
      FROM etl.silver_delivery_log l
      WHERE l.delivery_id = d.delivery_id
        AND l.silver_table_name = 'silver.orders_history'
        AND l.status = 'SUCCESS'
  )
ORDER BY d.delivery_id;
"""


MERGE_CURRENT_SQL = """
MERGE silver.orders_current AS tgt
USING (
    SELECT
        x.order_id,
        x.customer_id,
        x.order_status,
        x.order_purchase_timestamp,
        x.row_hash,
        x.source_name,
        x.source_record_id,
        x.delivery_id,
        x.bronze_batch_id
    FROM (
        SELECT
            b.order_id,
            b.customer_id,
            b.order_status,
            b.order_purchase_timestamp,
            b.source_name,
            b.source_record_id,
            b.delivery_id,
            b.batch_id AS bronze_batch_id,
            CONVERT(NVARCHAR(64), HASHBYTES(
                'SHA2_256',
                CONCAT(
                    ISNULL(b.order_id, ''), '|',
                    ISNULL(b.customer_id, ''), '|',
                    ISNULL(b.order_status, ''), '|',
                    ISNULL(CONVERT(NVARCHAR(33), b.order_purchase_timestamp, 126), '')
                )
            ), 2) AS row_hash,
            ROW_NUMBER() OVER (
                PARTITION BY b.order_id
                ORDER BY b.bronze_id DESC
            ) AS rn
        FROM bronze.olist_orders_raw b
        WHERE b.delivery_id = ?
          AND b.order_id IS NOT NULL
    ) x
    WHERE x.rn = 1
) AS src
ON tgt.order_id = src.order_id

WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET
        tgt.customer_id = src.customer_id,
        tgt.order_status = src.order_status,
        tgt.order_purchase_timestamp = src.order_purchase_timestamp,
        tgt.row_hash = src.row_hash,
        tgt.source_name = src.source_name,
        tgt.source_record_id = src.source_record_id,
        tgt.delivery_id = src.delivery_id,
        tgt.bronze_batch_id = src.bronze_batch_id,
        tgt.silver_updated_at_utc = GETUTCDATE()

WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        row_hash,
        source_name,
        source_record_id,
        delivery_id,
        bronze_batch_id,
        silver_updated_at_utc
    )
    VALUES (
        src.order_id,
        src.customer_id,
        src.order_status,
        src.order_purchase_timestamp,
        src.row_hash,
        src.source_name,
        src.source_record_id,
        src.delivery_id,
        src.bronze_batch_id,
        GETUTCDATE()
    );
"""


UPDATE_HISTORY_CLOSE_SQL = """
UPDATE h
SET
    h.valid_to_utc = src.observed_at_utc,
    h.is_current = 0
FROM silver.orders_history h
JOIN (
    SELECT
        x.order_id,
        x.row_hash,
        x.observed_at_utc
    FROM (
        SELECT
            b.order_id,
            d.received_at_utc AS observed_at_utc,
            CONVERT(NVARCHAR(64), HASHBYTES(
                'SHA2_256',
                CONCAT(
                    ISNULL(b.order_id, ''), '|',
                    ISNULL(b.customer_id, ''), '|',
                    ISNULL(b.order_status, ''), '|',
                    ISNULL(CONVERT(NVARCHAR(33), b.order_purchase_timestamp, 126), '')
                )
            ), 2) AS row_hash,
            ROW_NUMBER() OVER (
                PARTITION BY b.order_id
                ORDER BY b.bronze_id DESC
            ) AS rn
        FROM bronze.olist_orders_raw b
        JOIN etl.source_delivery d
          ON d.delivery_id = b.delivery_id
        WHERE b.delivery_id = ?
          AND b.order_id IS NOT NULL
    ) x
    WHERE x.rn = 1
) src
  ON h.order_id = src.order_id
WHERE h.is_current = 1
  AND h.row_hash <> src.row_hash;
"""

INSERT_HISTORY_SQL = """
INSERT INTO silver.orders_history (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    row_hash,
    valid_from_utc,
    valid_to_utc,
    is_current,
    source_name,
    source_record_id,
    delivery_id,
    bronze_batch_id
)
SELECT
    src.order_id,
    src.customer_id,
    src.order_status,
    src.order_purchase_timestamp,
    src.row_hash,
    src.observed_at_utc,
    NULL,
    1,
    src.source_name,
    src.source_record_id,
    src.delivery_id,
    src.bronze_batch_id
FROM (
    SELECT
        x.order_id,
        x.customer_id,
        x.order_status,
        x.order_purchase_timestamp,
        x.row_hash,
        x.observed_at_utc,
        x.source_name,
        x.source_record_id,
        x.delivery_id,
        x.bronze_batch_id
    FROM (
        SELECT
            b.order_id,
            b.customer_id,
            b.order_status,
            b.order_purchase_timestamp,
            b.source_name,
            b.source_record_id,
            b.delivery_id,
            b.batch_id AS bronze_batch_id,
            d.received_at_utc AS observed_at_utc,
            CONVERT(NVARCHAR(64), HASHBYTES(
                'SHA2_256',
                CONCAT(
                    ISNULL(b.order_id, ''), '|',
                    ISNULL(b.customer_id, ''), '|',
                    ISNULL(b.order_status, ''), '|',
                    ISNULL(CONVERT(NVARCHAR(33), b.order_purchase_timestamp, 126), '')
                )
            ), 2) AS row_hash,
            ROW_NUMBER() OVER (
                PARTITION BY b.order_id
                ORDER BY b.bronze_id DESC
            ) AS rn
        FROM bronze.olist_orders_raw b
        JOIN etl.source_delivery d
          ON d.delivery_id = b.delivery_id
        WHERE b.delivery_id = ?
          AND b.order_id IS NOT NULL
    ) x
    WHERE x.rn = 1
) src
LEFT JOIN silver.orders_history h
  ON h.order_id = src.order_id
 AND h.is_current = 1
WHERE h.order_id IS NULL
   OR h.row_hash <> src.row_hash;
"""


MARK_DELIVERY_PROCESSED_SQL = """
MERGE etl.silver_delivery_log AS tgt
USING (
    SELECT
        ? AS silver_table_name,
        ? AS delivery_id,
        'SUCCESS' AS status
) AS src
ON tgt.silver_table_name = src.silver_table_name
AND tgt.delivery_id = src.delivery_id
WHEN NOT MATCHED THEN
    INSERT (silver_table_name, delivery_id, processed_at_utc, status)
    VALUES (src.silver_table_name, src.delivery_id, GETUTCDATE(), src.status);
"""


COUNT_CURRENT_SQL = "SELECT COUNT(*) FROM silver.orders_current;"
COUNT_HISTORY_SQL = "SELECT COUNT(*) FROM silver.orders_history;"


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
        print("Ensured Silver objects exist")

        cursor.execute(GET_UNPROCESSED_DELIVERIES_SQL, (SOURCE_NAME,))
        delivery_rows = cursor.fetchall()

        if not delivery_rows:
            print("No unprocessed LOADED deliveries found. Nothing to do.")

        else:
            print(f"Found {len(delivery_rows)} unprocessed delivery/deliveries")

            for delivery_row in delivery_rows:
                delivery_id = delivery_row[0]
                print(f"Processing delivery_id={delivery_id}")

                cursor.execute(MERGE_CURRENT_SQL, delivery_id)
                conn.commit()
                print(f"Updated silver.orders_current for delivery_id={delivery_id}")

                cursor.execute(UPDATE_HISTORY_CLOSE_SQL, delivery_id)
                conn.commit()
                print(f"Closed changed SCD2 rows for delivery_id={delivery_id}")

                cursor.execute(INSERT_HISTORY_SQL, delivery_id)
                conn.commit()
                print(f"Inserted new SCD2 rows for delivery_id={delivery_id}")

                cursor.execute(
                    MARK_DELIVERY_PROCESSED_SQL,
                    ("silver.orders_current", delivery_id)
                )
                cursor.execute(
                    MARK_DELIVERY_PROCESSED_SQL,
                    ("silver.orders_history", delivery_id)
                )
                conn.commit()
                print(f"Marked delivery_id={delivery_id} as processed")

                cursor.execute(COUNT_CURRENT_SQL)
                current_count = cursor.fetchone()[0]

                cursor.execute(COUNT_HISTORY_SQL)
                history_count = cursor.fetchone()[0]

                finish_batch_success(
                    cursor,
                    batch_id=batch_id,
                    rows_extracted=current_count,
                    rows_loaded=current_count,
                    rows_skipped=0,
                )
                conn.commit()

                print(
                    f"Pipeline succeeded: current={current_count}, history={history_count}"
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