import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from src.etl.base_pipeline import BaseBronzeIngestionPipeline
from src.etl.github_client import GitHubClient
from src.etl.source_delivery import (
    compute_payload_hash,
    create_delivery,
    mark_prior_deliveries_superseded,
)


def parse_github_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    return datetime.fromisoformat(text).replace(tzinfo=None)


def labels_to_json(labels: list[dict[str, Any]] | None) -> str | None:
    if not labels:
        return None
    names = sorted(label.get("name", "") for label in labels)
    return json.dumps(names, ensure_ascii=False)


def compute_since_datetime() -> datetime | None:
    """
    Return UTC cutoff for GitHub `since` parameter.

    GITHUB_SINCE_DAYS defaults to 90. Set 0 for full history (no since filter).
    """
    raw = os.getenv("GITHUB_SINCE_DAYS", "90").strip()
    if not raw or raw == "0":
        return None
    days = int(raw)
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)


class IngestGitHubIssuesPipeline(BaseBronzeIngestionPipeline):
    pipeline_name = "ingest_github_issues"
    source_name = "github_issues"
    delivery_type = "API"

    def __init__(self, load_mode: str = "SNAPSHOT"):
        super().__init__(load_mode=load_mode)
        self.client = GitHubClient()
        self._fetched_issues: list[dict[str, Any]] = []
        self._since_dt = compute_since_datetime()

    def _fetch_issues(self) -> list[dict[str, Any]]:
        return self.client.fetch_issues(state="all", since=self._since_dt)

    def create_delivery(self, cursor, batch_id: int) -> int:
        if not self._fetched_issues:
            self._fetched_issues = self._fetch_issues()

        canonical_payload = json.dumps(
            self._fetched_issues,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        content_hash = compute_payload_hash(canonical_payload)

        return create_delivery(
            cursor=cursor,
            source_name=self.source_name,
            delivery_type=self.delivery_type,
            source_object_name=self.client.source_object_name(state="all"),
            snapshot_date=None,
            content_hash=content_hash,
            batch_id=batch_id,
        )

    def fetch_data(self) -> list[dict[str, Any]]:
        if not self._fetched_issues:
            self._fetched_issues = self._fetch_issues()
        return self._fetched_issues

    def transform_records(
        self,
        records: list[dict[str, Any]],
        batch_id: int,
        delivery_id: int,
    ) -> list[tuple]:
        bronze_rows = []

        for issue in records:
            issue_number = issue.get("number")
            if issue_number is None:
                continue

            user = issue.get("user") or {}
            user_login = user.get("login")

            payload_json = json.dumps(issue, ensure_ascii=False)

            bronze_rows.append(
                (
                    batch_id,
                    delivery_id,
                    self.source_name,
                    str(issue_number),
                    payload_json,
                    int(issue_number),
                    issue.get("title"),
                    issue.get("state"),
                    parse_github_datetime(issue.get("created_at")),
                    parse_github_datetime(issue.get("updated_at")),
                    parse_github_datetime(issue.get("closed_at")),
                    user_login,
                    labels_to_json(issue.get("labels")),
                )
            )

        return bronze_rows

    def filter_new_rows(
        self, cursor, bronze_rows: list[tuple]
    ) -> tuple[list[tuple], int]:
        """
        Append all issues from each API delivery.

        Mutable issues may appear across deliveries; Silver SCD2 detects changes.
        Skip only when an identical content_hash delivery was already LOADED.
        """
        if not bronze_rows:
            return [], 0

        delivery_id = bronze_rows[0][1]
        cursor.execute(
            """
            SELECT content_hash
            FROM etl.source_delivery
            WHERE delivery_id = ?
            """,
            (delivery_id,),
        )
        row = cursor.fetchone()
        content_hash = row[0] if row else None

        if content_hash:
            cursor.execute(
                """
                SELECT COUNT(*) FROM etl.source_delivery
                WHERE source_name = ?
                  AND content_hash = ?
                  AND status = 'LOADED'
                  AND delivery_id <> ?
                """,
                (self.source_name, content_hash, delivery_id),
            )
            duplicate_count = cursor.fetchone()[0]
            if duplicate_count > 0:
                return [], len(bronze_rows)

        return bronze_rows, 0

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
        cursor.executemany(
            """
            INSERT INTO bronze.github_issues_raw (
                batch_id,
                delivery_id,
                source_name,
                source_record_id,
                payload_json,
                issue_number,
                title,
                state,
                created_at,
                updated_at,
                closed_at,
                user_login,
                labels_json,
                extracted_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
            """,
            bronze_rows,
        )
        return len(bronze_rows)


if __name__ == "__main__":
    load_mode = "SNAPSHOT"

    if "--mode" in sys.argv:
        mode_index = sys.argv.index("--mode")
        try:
            load_mode = sys.argv[mode_index + 1]
        except IndexError as exc:
            raise SystemExit("Missing value after --mode") from exc

    raise SystemExit(IngestGitHubIssuesPipeline(load_mode=load_mode).run())
