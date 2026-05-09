import json
import os
import sys
from datetime import datetime
from typing import Any

import pandas as pd

from src.etl.base_pipeline import BaseBronzeIngestionPipeline
from src.etl.source_delivery import (
    create_delivery,
    compute_file_hash,
    mark_prior_deliveries_superseded,
)


# Fixed local path to the Olist orders file inside the Dev Container
FILE_PATH = "/workspaces/mini-dwh/data/olist/olist_orders_dataset.csv"


def parse_datetime(value: Any) -> datetime | None:
    """
    Parse a datetime string into Python datetime.

    Why not rely on SQL implicit conversion:
    - clearer
    - safer
    - easier to debug
    """
    if value is None:
        return None
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


class IngestOlistOrdersPipeline(BaseBronzeIngestionPipeline):
    pipeline_name = "ingest_olist_orders"
    source_name = "olist_orders"
    delivery_type = "FILE"

    def create_delivery(self, cursor, batch_id: int) -> int:
        """
        Create one delivery row for this physical CSV file.

        Current Olist file does not expose a real business snapshot date,
        so snapshot_date is left NULL for now.
        """
        file_name = os.path.basename(FILE_PATH)
        content_hash = compute_file_hash(FILE_PATH)
        snapshot_date = None

        return create_delivery(
            cursor=cursor,
            source_name=self.source_name,
            delivery_type=self.delivery_type,
            source_object_name=file_name,
            snapshot_date=snapshot_date,
            content_hash=content_hash,
            batch_id=batch_id,
        )

    def fetch_data(self) -> list[dict[str, Any]]:
        """
        Read the CSV file into memory and return list of dict records.

        For ~100k rows this is acceptable.
        For much larger datasets we would move to chunking.
        """
        df = pd.read_csv(FILE_PATH)
        return df.to_dict(orient="records")

    def transform_records(
        self,
        records: list[dict[str, Any]],
        batch_id: int,
        delivery_id: int,
    ) -> list[tuple]:
        """
        Convert raw CSV records into bronze table tuples.

        Bronze table keeps:
        - raw payload_json
        - technical metadata
        - selected parsed fields for convenience
        """
        bronze_rows = []

        for row in records:
            order_id = row.get("order_id")
            customer_id = row.get("customer_id")
            order_status = row.get("order_status")
            order_purchase_timestamp = parse_datetime(row.get("order_purchase_timestamp"))

            # Skip rows without business key
            if order_id is None or pd.isna(order_id):
                continue

            payload_json = json.dumps(row, default=str)

            bronze_rows.append(
                (
                    batch_id,
                    delivery_id,
                    self.source_name,
                    str(order_id),  # source_record_id
                    payload_json,
                    str(order_id),
                    None if pd.isna(customer_id) else str(customer_id),
                    None if pd.isna(order_status) else str(order_status),
                    order_purchase_timestamp,
                )
            )

        return bronze_rows

    def filter_new_rows(self, cursor, bronze_rows: list[tuple]) -> tuple[list[tuple], int]:
        """
        Prevent duplicate business keys in bronze for normal SNAPSHOT mode.

        Current rule:
        if source_name + source_record_id already exists in bronze, skip it.

        This is fine for current scale.
        For very large volumes we would replace this with a more scalable pattern.
        """
        cursor.execute(
            """
            SELECT source_record_id
            FROM bronze.olist_orders_raw
            WHERE source_name = ?
            """,
            (self.source_name,),
        )

        existing_ids = {row[0] for row in cursor.fetchall()}

        rows_to_insert = []
        skipped = 0

        for row in bronze_rows:
            # tuple structure:
            # 0 batch_id
            # 1 delivery_id
            # 2 source_name
            # 3 source_record_id
            source_record_id = row[3]

            if source_record_id in existing_ids:
                skipped += 1
            else:
                rows_to_insert.append(row)

        return rows_to_insert, skipped

    def handle_reload(self, cursor) -> None:
        """
        RELOAD mode hook (optional).
        
        Unlike SNAPSHOT, RELOAD still filters out duplicates,
        keeping all historical bronze rows for SCD processing.
        """
        pass

    def after_successful_reload(self, cursor, delivery_id: int) -> None:
        """
        After a successful reload, older LOADED deliveries for this source
        are no longer active. Mark them as SUPERSEDED.
        """
        mark_prior_deliveries_superseded(
            cursor=cursor,
            source_name=self.source_name,
            current_delivery_id=delivery_id,
        )

    def load_to_bronze(self, cursor, bronze_rows: list[tuple]) -> int:
        """
        Insert prepared rows into bronze in batches to avoid timeout.

        If nothing remains after filtering, return 0 cleanly.
        """
        if not bronze_rows:
            return 0

        cursor.fast_executemany = True
        batch_size = 10000
        total_inserted = 0
        
        # Insert in batches to prevent slow/stuck inserts
        for i in range(0, len(bronze_rows), batch_size):
            batch = bronze_rows[i:i + batch_size]
            cursor.executemany(
                """
                INSERT INTO bronze.olist_orders_raw (
                    batch_id,
                    delivery_id,
                    source_name,
                    source_record_id,
                    payload_json,
                    order_id,
                    customer_id,
                    order_status,
                    order_purchase_timestamp,
                    extracted_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
                """,
                batch,
            )
            total_inserted += len(batch)
            print(f"[{datetime.now()}] Inserted batch: {total_inserted}/{len(bronze_rows)} rows")
        
        return total_inserted


if __name__ == "__main__":
    # CLI usage examples:
    # python -m src.pipelines.ingest_olist_orders
    # python -m src.pipelines.ingest_olist_orders --mode RELOAD
    # python -m src.pipelines.ingest_olist_orders --mode SNAPSHOT

    load_mode = "SNAPSHOT"

    if "--mode" in sys.argv:
        mode_index = sys.argv.index("--mode")
        try:
            load_mode = sys.argv[mode_index + 1]
        except IndexError as exc:
            raise SystemExit("Missing value after --mode") from exc

    raise SystemExit(IngestOlistOrdersPipeline(load_mode=load_mode).run())