import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.pipelines.ingest_github_issues import (
    IngestGitHubIssuesPipeline,
    labels_to_json,
    parse_github_datetime,
)


SAMPLE_ISSUES = [
    {
        "number": 1,
        "title": "First issue",
        "state": "open",
        "created_at": "2024-01-01T10:00:00Z",
        "updated_at": "2024-01-02T10:00:00Z",
        "closed_at": None,
        "user": {"login": "alice"},
        "labels": [{"name": "bug"}],
    },
    {
        "number": 2,
        "title": "Closed issue",
        "state": "closed",
        "created_at": "2024-01-03T10:00:00Z",
        "updated_at": "2024-01-05T10:00:00Z",
        "closed_at": "2024-01-05T10:00:00Z",
        "user": {"login": "bob"},
        "labels": [],
    },
]


def test_parse_github_datetime():
    result = parse_github_datetime("2024-01-01T10:00:00Z")
    assert result == datetime(2024, 1, 1, 10, 0, 0)


def test_labels_to_json_sorts_names():
    result = labels_to_json([{"name": "enhancement"}, {"name": "bug"}])
    assert json.loads(result) == ["bug", "enhancement"]


def test_transform_records_shape():
    pipeline = IngestGitHubIssuesPipeline()
    rows = pipeline.transform_records(SAMPLE_ISSUES, batch_id=1, delivery_id=10)

    assert len(rows) == 2
    assert rows[0][3] == "1"
    assert rows[0][5] == 1
    assert rows[0][7] == "open"
    assert rows[0][11] == "alice"


def test_filter_new_rows_skips_duplicate_content_hash():
    pipeline = IngestGitHubIssuesPipeline()
    bronze_rows = pipeline.transform_records(SAMPLE_ISSUES, batch_id=1, delivery_id=10)

    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        ("abc123hash",),
        (1,),
    ]

    filtered, skipped = pipeline.filter_new_rows(cursor, bronze_rows)
    assert filtered == []
    assert skipped == 2


def test_filter_new_rows_allows_new_delivery():
    pipeline = IngestGitHubIssuesPipeline()
    bronze_rows = pipeline.transform_records(SAMPLE_ISSUES, batch_id=1, delivery_id=10)

    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        ("abc123hash",),
        (0,),
    ]

    filtered, skipped = pipeline.filter_new_rows(cursor, bronze_rows)
    assert len(filtered) == 2
    assert skipped == 0
