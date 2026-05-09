import pyodbc


def start_batch(
    cursor: pyodbc.Cursor,
    pipeline_name: str,
    source_name: str,
) -> int:
    cursor.execute(
        """
        INSERT INTO etl.batch_run (
            pipeline_name,
            source_name,
            start_ts,
            status
        )
        OUTPUT INSERTED.batch_id
        VALUES (?, ?, GETDATE(), ?)
        """,
        (pipeline_name, source_name, "RUNNING"),
    )

    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Failed to create batch_run record")

    return int(row[0])


def finish_batch_success(
    cursor: pyodbc.Cursor,
    batch_id: int,
    rows_extracted: int,
    rows_loaded: int,
    rows_skipped: int,
) -> None:
    cursor.execute(
        """
        UPDATE etl.batch_run
        SET
            end_ts = GETDATE(),
            status = ?,
            rows_extracted = ?,
            rows_loaded = ?,
            rows_skipped = ?,
            error_message = NULL
        WHERE batch_id = ?
        """,
        ("SUCCESS", rows_extracted, rows_loaded, rows_skipped, batch_id),
    )


def finish_batch_failure(
    cursor: pyodbc.Cursor,
    batch_id: int,
    error_message: str,
) -> None:
    cursor.execute(
        """
        UPDATE etl.batch_run
        SET
            end_ts = GETDATE(),
            status = ?,
            error_message = ?
        WHERE batch_id = ?
        """,
        ("FAILED", error_message[:4000], batch_id),
    )