from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from src.common.db import get_connection
from src.etl.batch_log import (
    finish_batch_failure,
    finish_batch_success,
    start_batch,
)
from src.etl.source_delivery import update_delivery_status


class BaseBronzeIngestionPipeline(ABC):
    """
    Base class for bronze ingestion pipelines.

    Concepts:
    - batch_id    = technical execution ID
    - delivery_id = source artifact ID (file / API pull)
    - source_record_id = business record key inside the source
    """

    pipeline_name: str
    source_name: str
    delivery_type: str = "FILE"

    def __init__(self, load_mode: str = "SNAPSHOT"):
        # Supported modes:
        # SNAPSHOT    -> load only new business keys, skip existing
        # RELOAD      -> delete existing source slice, then reload everything
        # INCREMENTAL -> later, for APIs like GitHub
        self.load_mode = load_mode.upper()

    @abstractmethod
    def create_delivery(self, cursor, batch_id: int) -> int:
        """
        Create one source_delivery row and return delivery_id.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_data(self) -> list[dict[str, Any]]:
        """Fetch raw records from the source."""
        raise NotImplementedError

    @abstractmethod
    def transform_records(
        self,
        records: list[dict[str, Any]],
        batch_id: int,
        delivery_id: int,
    ) -> list[tuple]:
        """Validate and transform raw source records into bronze rows."""
        raise NotImplementedError

    @abstractmethod
    def filter_new_rows(self, cursor, bronze_rows: list[tuple]) -> tuple[list[tuple], int]:
        """
        Return:
        - rows_to_insert
        - rows_skipped
        """
        raise NotImplementedError

    @abstractmethod
    def load_to_bronze(self, cursor, bronze_rows: list[tuple]) -> int:
        """Insert already-transformed bronze rows and return loaded row count."""
        raise NotImplementedError

    def handle_reload(self, cursor):
        print(
        f"Load mode is RELOAD — append-only mode; "
        f"keeping existing bronze rows for source={self.source_name}"
        )

    def after_successful_reload(self, cursor, delivery_id: int) -> None:
        """
        Optional hook for pipelines that need post-reload actions,
        such as marking previous deliveries as SUPERSEDED.
        """
        return

    def run(self) -> int:
        print(f"[{datetime.now()}] Starting pipeline: {self.pipeline_name}")

        batch_id = None
        delivery_id = None
        rows_extracted = 0
        rows_loaded = 0
        rows_skipped = 0

        try:
            with get_connection() as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                # 1. Create ETL batch row
                batch_id = start_batch(
                    cursor=cursor,
                    pipeline_name=self.pipeline_name,
                    source_name=self.source_name,
                )
                conn.commit()

                print(f"[{datetime.now()}] Created batch_id={batch_id}")

                # 2. Create source delivery row
                delivery_id = self.create_delivery(cursor=cursor, batch_id=batch_id)
                conn.commit()

                print(f"[{datetime.now()}] Created delivery_id={delivery_id}")

                # 3. Fetch source data
                records = self.fetch_data()
                rows_extracted = len(records)

                print(
                    f"[{datetime.now()}] Fetched {rows_extracted} records "
                    f"from source={self.source_name}"
                )

                # 4. Transform source records into bronze rows
                bronze_rows = self.transform_records(
                    records=records,
                    batch_id=batch_id,
                    delivery_id=delivery_id,
                )

                print(
                    f"[{datetime.now()}] Transformed {len(bronze_rows)} records "
                    f"for bronze load"
                )

                # 5. Handle load mode
                if self.load_mode == "RELOAD":
                    print(
                        f"[{datetime.now()}] Load mode is RELOAD — "
                        f"keeping historical bronze rows for source={self.source_name}"
                    )
                    self.handle_reload(cursor)
                    bronze_rows_to_insert, rows_skipped = self.filter_new_rows(
                        cursor=cursor,
                        bronze_rows=bronze_rows,
                    )

                elif self.load_mode in {"SNAPSHOT", "INCREMENTAL"}:
                    bronze_rows_to_insert, rows_skipped = self.filter_new_rows(
                        cursor=cursor,
                        bronze_rows=bronze_rows,
                    )

                else:
                    raise ValueError(
                        f"Unsupported load_mode={self.load_mode}. "
                        "Expected SNAPSHOT, RELOAD, or INCREMENTAL."
                    )

                print(
                    f"[{datetime.now()}] Filtered rows for insert: "
                    f"new={len(bronze_rows_to_insert)}, skipped={rows_skipped}"
                )

                # 6. Insert into bronze
                rows_loaded = self.load_to_bronze(
                    cursor=cursor,
                    bronze_rows=bronze_rows_to_insert,
                )

                # 7. Update delivery status based on actual result
                if self.load_mode == "RELOAD":
                    update_delivery_status(cursor, delivery_id, "LOADED")
                    self.after_successful_reload(cursor, delivery_id)

                elif rows_loaded > 0:
                    update_delivery_status(cursor, delivery_id, "LOADED")

                else:
                    update_delivery_status(cursor, delivery_id, "SKIPPED_DUPLICATE")

                # 8. Mark batch as success
                finish_batch_success(
                    cursor=cursor,
                    batch_id=batch_id,
                    rows_extracted=rows_extracted,
                    rows_loaded=rows_loaded,
                    rows_skipped=rows_skipped,
                )
                conn.commit()

                print(
                    f"[{datetime.now()}] Pipeline succeeded. "
                    f"batch_id={batch_id}, delivery_id={delivery_id}, "
                    f"rows_extracted={rows_extracted}, rows_loaded={rows_loaded}, "
                    f"rows_skipped={rows_skipped}"
                )
                return 0

        except Exception as exc:
            error_message = str(exc)
            print(f"[{datetime.now()}] Pipeline failed: {error_message}", file=sys.stderr)

            # Try to mark both batch and delivery as FAILED in a new connection
            if batch_id is not None:
                try:
                    with get_connection() as conn:
                        conn.autocommit = False
                        cursor = conn.cursor()

                        finish_batch_failure(
                            cursor=cursor,
                            batch_id=batch_id,
                            error_message=error_message,
                        )

                        if delivery_id is not None:
                            update_delivery_status(cursor, delivery_id, "FAILED")

                        conn.commit()
                except Exception as logging_exc:
                    print(
                        f"[{datetime.now()}] Failed to update failure status: "
                        f"{logging_exc}",
                        file=sys.stderr,
                    )

            return 1