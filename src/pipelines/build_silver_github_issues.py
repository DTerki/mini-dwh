from src.common.db import get_connection
from src.etl.batch_log import start_batch, finish_batch_success, finish_batch_failure


PIPELINE_NAME = "build_silver_github_issues"
SOURCE_NAME = "github_issues"

ROW_HASH_EXPR = """
CONVERT(NVARCHAR(64), HASHBYTES(
    'SHA2_256',
    CONCAT(
        ISNULL(CAST(b.issue_number AS NVARCHAR(20)), ''), '|',
        ISNULL(b.title, ''), '|',
        ISNULL(b.state, ''), '|',
        ISNULL(b.labels_json, ''), '|',
        ISNULL(b.user_login, ''), '|',
        ISNULL(CONVERT(NVARCHAR(33), b.closed_at, 126), '')
    )
), 2)
"""

DDL_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
BEGIN
    EXEC('CREATE SCHEMA etl');
END;

IF OBJECT_ID('silver.github_issues_current', 'U') IS NULL
BEGIN
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
END;

IF OBJECT_ID('silver.github_issues_history', 'U') IS NULL
BEGIN
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

    CREATE INDEX IX_github_issues_history_issue_number
        ON silver.github_issues_history(issue_number);

    CREATE INDEX IX_github_issues_history_current
        ON silver.github_issues_history(issue_number, is_current);
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
        AND l.silver_table_name = 'silver.github_issues_history'
        AND l.status = 'SUCCESS'
  )
ORDER BY d.delivery_id;
"""


MERGE_CURRENT_SQL = f"""
MERGE silver.github_issues_current AS tgt
USING (
    SELECT
        x.issue_number,
        x.title,
        x.state,
        x.created_at,
        x.updated_at,
        x.closed_at,
        x.user_login,
        x.labels_json,
        x.row_hash,
        x.source_name,
        x.source_record_id,
        x.delivery_id,
        x.bronze_batch_id
    FROM (
        SELECT
            b.issue_number,
            b.title,
            b.state,
            b.created_at,
            b.updated_at,
            b.closed_at,
            b.user_login,
            b.labels_json,
            b.source_name,
            b.source_record_id,
            b.delivery_id,
            b.batch_id AS bronze_batch_id,
            {ROW_HASH_EXPR} AS row_hash,
            ROW_NUMBER() OVER (
                PARTITION BY b.issue_number
                ORDER BY b.bronze_id DESC
            ) AS rn
        FROM bronze.github_issues_raw b
        WHERE b.delivery_id = ?
          AND b.issue_number IS NOT NULL
    ) x
    WHERE x.rn = 1
) AS src
ON tgt.issue_number = src.issue_number

WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET
        tgt.title = src.title,
        tgt.state = src.state,
        tgt.created_at = src.created_at,
        tgt.updated_at = src.updated_at,
        tgt.closed_at = src.closed_at,
        tgt.user_login = src.user_login,
        tgt.labels_json = src.labels_json,
        tgt.row_hash = src.row_hash,
        tgt.source_name = src.source_name,
        tgt.source_record_id = src.source_record_id,
        tgt.delivery_id = src.delivery_id,
        tgt.bronze_batch_id = src.bronze_batch_id,
        tgt.silver_updated_at_utc = GETUTCDATE()

WHEN NOT MATCHED THEN
    INSERT (
        issue_number,
        title,
        state,
        created_at,
        updated_at,
        closed_at,
        user_login,
        labels_json,
        row_hash,
        source_name,
        source_record_id,
        delivery_id,
        bronze_batch_id,
        silver_updated_at_utc
    )
    VALUES (
        src.issue_number,
        src.title,
        src.state,
        src.created_at,
        src.updated_at,
        src.closed_at,
        src.user_login,
        src.labels_json,
        src.row_hash,
        src.source_name,
        src.source_record_id,
        src.delivery_id,
        src.bronze_batch_id,
        GETUTCDATE()
    );
"""


UPDATE_HISTORY_CLOSE_SQL = f"""
UPDATE h
SET
    h.valid_to_utc = src.observed_at_utc,
    h.is_current = 0
FROM silver.github_issues_history h
JOIN (
    SELECT
        x.issue_number,
        x.row_hash,
        x.observed_at_utc
    FROM (
        SELECT
            b.issue_number,
            d.received_at_utc AS observed_at_utc,
            {ROW_HASH_EXPR} AS row_hash,
            ROW_NUMBER() OVER (
                PARTITION BY b.issue_number
                ORDER BY b.bronze_id DESC
            ) AS rn
        FROM bronze.github_issues_raw b
        JOIN etl.source_delivery d
          ON d.delivery_id = b.delivery_id
        WHERE b.delivery_id = ?
          AND b.issue_number IS NOT NULL
    ) x
    WHERE x.rn = 1
) src
  ON h.issue_number = src.issue_number
WHERE h.is_current = 1
  AND h.row_hash <> src.row_hash;
"""


INSERT_HISTORY_SQL = f"""
INSERT INTO silver.github_issues_history (
    issue_number,
    title,
    state,
    created_at,
    updated_at,
    closed_at,
    user_login,
    labels_json,
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
    src.issue_number,
    src.title,
    src.state,
    src.created_at,
    src.updated_at,
    src.closed_at,
    src.user_login,
    src.labels_json,
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
        x.issue_number,
        x.title,
        x.state,
        x.created_at,
        x.updated_at,
        x.closed_at,
        x.user_login,
        x.labels_json,
        x.row_hash,
        x.observed_at_utc,
        x.source_name,
        x.source_record_id,
        x.delivery_id,
        x.bronze_batch_id
    FROM (
        SELECT
            b.issue_number,
            b.title,
            b.state,
            b.created_at,
            b.updated_at,
            b.closed_at,
            b.user_login,
            b.labels_json,
            b.source_name,
            b.source_record_id,
            b.delivery_id,
            b.batch_id AS bronze_batch_id,
            d.received_at_utc AS observed_at_utc,
            {ROW_HASH_EXPR} AS row_hash,
            ROW_NUMBER() OVER (
                PARTITION BY b.issue_number
                ORDER BY b.bronze_id DESC
            ) AS rn
        FROM bronze.github_issues_raw b
        JOIN etl.source_delivery d
          ON d.delivery_id = b.delivery_id
        WHERE b.delivery_id = ?
          AND b.issue_number IS NOT NULL
    ) x
    WHERE x.rn = 1
) src
LEFT JOIN silver.github_issues_history h
  ON h.issue_number = src.issue_number
 AND h.is_current = 1
WHERE h.issue_number IS NULL
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


COUNT_CURRENT_SQL = "SELECT COUNT(*) FROM silver.github_issues_current;"
COUNT_HISTORY_SQL = "SELECT COUNT(*) FROM silver.github_issues_history;"


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
            finish_batch_success(
                cursor,
                batch_id=batch_id,
                rows_extracted=0,
                rows_loaded=0,
                rows_skipped=0,
            )
            conn.commit()
        else:
            print(f"Found {len(delivery_rows)} unprocessed delivery/deliveries")

            for delivery_row in delivery_rows:
                delivery_id = delivery_row[0]
                print(f"Processing delivery_id={delivery_id}")

                cursor.execute(MERGE_CURRENT_SQL, (delivery_id,))
                conn.commit()
                print(f"Updated silver.github_issues_current for delivery_id={delivery_id}")

                cursor.execute(UPDATE_HISTORY_CLOSE_SQL, (delivery_id,))
                conn.commit()
                print(f"Closed changed SCD2 rows for delivery_id={delivery_id}")

                cursor.execute(INSERT_HISTORY_SQL, (delivery_id,))
                conn.commit()
                print(f"Inserted new SCD2 rows for delivery_id={delivery_id}")

                cursor.execute(
                    MARK_DELIVERY_PROCESSED_SQL,
                    ("silver.github_issues_current", delivery_id),
                )
                cursor.execute(
                    MARK_DELIVERY_PROCESSED_SQL,
                    ("silver.github_issues_history", delivery_id),
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
