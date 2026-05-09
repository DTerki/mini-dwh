import json
from typing import Any

import requests

from src.etl.base_pipeline import BaseBronzeIngestionPipeline


SOURCE_URL = "https://jsonplaceholder.typicode.com/posts"
REQUEST_TIMEOUT_SECONDS = 30


class IngestApiPostsPipeline(BaseBronzeIngestionPipeline):
    pipeline_name = "ingest_api_posts"
    source_name = "jsonplaceholder_posts"

    def fetch_data(self) -> list[dict[str, Any]]:
        response = requests.get(SOURCE_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, list):
            raise ValueError("API response is not a list")

        return data

    def transform_records(
        self,
        records: list[dict[str, Any]],
        batch_id: int,
    ) -> list[tuple]:
        bronze_rows = []

        for post in records:
            post_id = post.get("id")
            user_id = post.get("userId")
            title = post.get("title")
            body = post.get("body")

            if post_id is None:
                raise ValueError("Post is missing id")

            payload_json = json.dumps(post, ensure_ascii=False)

            bronze_rows.append(
                (
                    batch_id,
                    self.source_name,
                    str(post_id),
                    payload_json,
                    user_id,
                    title,
                    body,
                )
            )

        return bronze_rows

    def load_to_bronze(self, cursor, bronze_rows: list[tuple]) -> int:
        cursor.fast_executemany = True
        cursor.executemany(
            """
            INSERT INTO bronze.api_posts_raw (
                batch_id,
                source_name,
                source_record_id,
                payload_json,
                user_id,
                title,
                body,
                extracted_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
            """,
            bronze_rows,
        )

        return len(bronze_rows)


if __name__ == "__main__":
    raise SystemExit(IngestApiPostsPipeline().run())