import hashlib


def compute_file_hash(file_path: str) -> str:
    """
    Compute MD5 hash for the source file.

    Why:
    - detect exact same file resent again
    - distinguish corrected file with same name/date but different content
    """
    hash_md5 = hashlib.md5()

    with open(file_path, "rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(8192), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def compute_payload_hash(payload: str) -> str:
    """
    Compute MD5 hash for API response body or canonical JSON payload.

    Used to detect identical API pulls (duplicate delivery).
    """
    hash_md5 = hashlib.md5()
    hash_md5.update(payload.encode("utf-8"))
    return hash_md5.hexdigest()


def create_delivery(
    cursor,
    source_name: str,
    delivery_type: str,
    source_object_name: str,
    snapshot_date,
    content_hash: str,
    batch_id: int,
) -> int:
    """
    Insert one row into etl.source_delivery and return delivery_id.
    """
    cursor.execute(
        """
        INSERT INTO etl.source_delivery (
            source_name,
            delivery_type,
            source_object_name,
            snapshot_date,
            content_hash,
            batch_id,
            status
        )
        OUTPUT INSERTED.delivery_id
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_name,
            delivery_type,
            source_object_name,
            snapshot_date,
            content_hash,
            batch_id,
            "RECEIVED",
        ),
    )

    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Failed to create source_delivery record")

    return int(row[0])


def update_delivery_status(cursor, delivery_id: int, status: str) -> None:
    """
    Update one delivery row with final status.
    """
    cursor.execute(
        """
        UPDATE etl.source_delivery
        SET status = ?
        WHERE delivery_id = ?
        """,
        (status, delivery_id),
    )


def mark_prior_deliveries_superseded(
    cursor,
    source_name: str,
    current_delivery_id: int,
) -> None:
    """
    Mark previous LOADED deliveries for the same source as SUPERSEDED.

    Use this after a successful RELOAD run, where the current delivery
    becomes the new active source slice.
    """
    cursor.execute(
        """
        UPDATE etl.source_delivery
        SET status = 'SUPERSEDED'
        WHERE source_name = ?
          AND delivery_id <> ?
          AND status = 'LOADED'
        """,
        (source_name, current_delivery_id),
    )