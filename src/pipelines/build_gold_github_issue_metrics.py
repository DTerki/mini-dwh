from src.common.db import get_connection
from src.etl.batch_log import start_batch, finish_batch_success, finish_batch_failure


PIPELINE_NAME = "build_gold_github_issue_metrics"
SOURCE_NAME = "silver.github_issues_current"


DDL_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;

IF OBJECT_ID('gold.daily_open_issues', 'U') IS NULL
BEGIN
    CREATE TABLE gold.daily_open_issues (
        snapshot_date DATE NOT NULL PRIMARY KEY,
        open_issues_count INT NOT NULL,
        gold_updated_at_utc DATETIME2 NOT NULL
    );
END;

IF OBJECT_ID('gold.issue_resolution_time', 'U') IS NULL
BEGIN
    CREATE TABLE gold.issue_resolution_time (
        issue_number INT NOT NULL PRIMARY KEY,
        title NVARCHAR(500) NULL,
        user_login NVARCHAR(100) NULL,
        created_at DATETIME2 NOT NULL,
        closed_at DATETIME2 NOT NULL,
        resolution_days INT NOT NULL,
        gold_updated_at_utc DATETIME2 NOT NULL
    );
END;
"""


BUILD_DAILY_OPEN_SQL = """
TRUNCATE TABLE gold.daily_open_issues;

;WITH date_spine AS (
    SELECT CAST(MIN(COALESCE(created_at, valid_from_utc)) AS DATE) AS snapshot_date
    FROM silver.github_issues_history
    WHERE COALESCE(created_at, valid_from_utc) IS NOT NULL

    UNION ALL

    SELECT DATEADD(DAY, 1, snapshot_date)
    FROM date_spine
    WHERE snapshot_date < CAST(GETUTCDATE() AS DATE)
)
INSERT INTO gold.daily_open_issues (
    snapshot_date,
    open_issues_count,
    gold_updated_at_utc
)
SELECT
    d.snapshot_date,
    COUNT(DISTINCT h.issue_number) AS open_issues_count,
    GETUTCDATE() AS gold_updated_at_utc
FROM date_spine d
LEFT JOIN silver.github_issues_history h
  ON h.state = 'open'
 AND CAST(h.valid_from_utc AS DATE) <= d.snapshot_date
 AND (h.valid_to_utc IS NULL OR CAST(h.valid_to_utc AS DATE) > d.snapshot_date)
GROUP BY d.snapshot_date
OPTION (MAXRECURSION 32767);
"""


BUILD_RESOLUTION_SQL = """
TRUNCATE TABLE gold.issue_resolution_time;

INSERT INTO gold.issue_resolution_time (
    issue_number,
    title,
    user_login,
    created_at,
    closed_at,
    resolution_days,
    gold_updated_at_utc
)
SELECT
    issue_number,
    title,
    user_login,
    created_at,
    closed_at,
    DATEDIFF(DAY, created_at, closed_at) AS resolution_days,
    GETUTCDATE() AS gold_updated_at_utc
FROM silver.github_issues_current
WHERE state = 'closed'
  AND closed_at IS NOT NULL
  AND created_at IS NOT NULL;
"""


COUNT_DAILY_SQL = "SELECT COUNT(*) FROM gold.daily_open_issues;"
COUNT_RESOLUTION_SQL = "SELECT COUNT(*) FROM gold.issue_resolution_time;"


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
        print("Ensured Gold objects exist")

        cursor.execute("SELECT COUNT(*) FROM silver.github_issues_history;")
        history_count = cursor.fetchone()[0]

        if history_count == 0:
            print("No Silver history rows found. Skipping Gold build.")
            finish_batch_success(
                cursor,
                batch_id=batch_id,
                rows_extracted=0,
                rows_loaded=0,
                rows_skipped=0,
            )
            conn.commit()
        else:
            cursor.execute(BUILD_DAILY_OPEN_SQL)
            conn.commit()
            print("Built gold.daily_open_issues")

            cursor.execute(BUILD_RESOLUTION_SQL)
            conn.commit()
            print("Built gold.issue_resolution_time")

            cursor.execute(COUNT_DAILY_SQL)
            daily_count = cursor.fetchone()[0]

            cursor.execute(COUNT_RESOLUTION_SQL)
            resolution_count = cursor.fetchone()[0]

            rows_loaded = daily_count + resolution_count

            finish_batch_success(
                cursor,
                batch_id=batch_id,
                rows_extracted=rows_loaded,
                rows_loaded=rows_loaded,
                rows_skipped=0,
            )
            conn.commit()

            print(
                f"Pipeline succeeded: daily_rows={daily_count}, "
                f"resolution_rows={resolution_count}"
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
