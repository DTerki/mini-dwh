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


FILE_PATH = "/workspaces/mini-dwh/data/olist/olist_customers_dataset.csv"


def as_clean_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


class IngestOlistCustomersPipeline(BaseBronzeIngestionPipeline):
    pipeline_name = "ingest_olist_customers"
    source_name = "olist_customers"
    delivery_type = "FILE"

    def create_delivery(self, cursor, batch_id: int) -> int:
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
        df = pd.read_csv(FILE_PATH)
        return df.to_dict(orient="records")

    def transform_records(
        self,
        records: list[dict[str, Any]],
        batch_id: int,
        delivery_id: int,
    ) -> list[tuple]:
        bronze_rows = []

        for row in records:
            customer_id = as_clean_str(row.get("customer_id"))
            if not customer_id:
                continue

            customer_unique_id = as_clean_str(row.get("customer_unique_id"))
            zip_prefix = as_clean_str(row.get("customer_zip_code_prefix"))
            city = as_clean_str(row.get("customer_city"))
            state = as_clean_str(row.get("customer_state"))

            payload_json = json.dumps(row, default=str)

            bronze_rows.append(
                (
                    batch_id,
                    delivery_id,
                    self.source_name,
                    customer_id,
                    payload_json,
                    customer_id,
                    customer_unique_id,
                    zip_prefix,
                    city,
                    state,
                )
            )

        return bronze_rows

    def filter_new_rows(self, cursor, bronze_rows: list[tuple]) -> tuple[list[tuple], int]:
        cursor.execute(
            """
            SELECT source_record_id
            FROM bronze.olist_customers_raw
            WHERE source_name = ?
            """,
            (self.source_name,),
        )

        existing_ids = {row[0] for row in cursor.fetchall()}

        rows_to_insert = []
        skipped = 0

        for row in bronze_rows:
            source_record_id = row[3]

            if source_record_id in existing_ids:
                skipped += 1
            else:
                rows_to_insert.append(row)

        return rows_to_insert, skipped

    def handle_reload(self, cursor) -> None:
        pass

    def after_successful_reload(self, cursor, delivery_id: int) -> None:
        mark_prior_deliveries_superseded(
            cursor=cursor,
            source_name=self.source_name,
            current_delivery_id=delivery_id,
        )

    def load_to_bronze(self, cursor, bronze_rows: list[tuple]) -> int:
        if not bronze_rows:
            return 0

        cursor.fast_executemany = True
        batch_size = 10000
        total_inserted = 0

        for i in range(0, len(bronze_rows), batch_size):
            batch = bronze_rows[i : i + batch_size]
            cursor.executemany(
                """
                INSERT INTO bronze.olist_customers_raw (
                    batch_id,
                    delivery_id,
                    source_name,
                    source_record_id,
                    payload_json,
                    customer_id,
                    customer_unique_id,
                    customer_zip_code_prefix,
                    customer_city,
                    customer_state,
                    extracted_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
                """,
                batch,
            )
            total_inserted += len(batch)
            print(
                f"[{datetime.now()}] Inserted batch: {total_inserted}/{len(bronze_rows)} rows"
            )

        return total_inserted


if __name__ == "__main__":
    load_mode = "SNAPSHOT"

    if "--mode" in sys.argv:
        mode_index = sys.argv.index("--mode")
        try:
            load_mode = sys.argv[mode_index + 1]
        except IndexError as exc:
            raise SystemExit("Missing value after --mode") from exc

    raise SystemExit(IngestOlistCustomersPipeline(load_mode=load_mode).run())
