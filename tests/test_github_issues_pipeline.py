import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.etl.github_client import GitHubClient, GitHubConfigurationError
from src.pipelines.ingest_github_issues import (
    IngestGitHubIssuesPipeline,
    compute_since_datetime,
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


def test_compute_since_datetime_default_90_days(monkeypatch):
    monkeypatch.setenv("GITHUB_SINCE_DAYS", "90")
    result = compute_since_datetime()
    assert result is not None
    diff = datetime.now(timezone.utc).replace(tzinfo=None) - result
    assert 89 <= diff.days <= 91


def test_compute_since_datetime_zero_means_full_history(monkeypatch):
    monkeypatch.setenv("GITHUB_SINCE_DAYS", "0")
    assert compute_since_datetime() is None


def test_github_client_requires_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    with pytest.raises(GitHubConfigurationError, match="GITHUB_TOKEN is required"):
        GitHubClient(token="")


@patch("src.etl.github_client.requests.get")
def test_fetch_issues_passes_since_parameter(mock_get, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    mock_response = MagicMock()
    mock_response.headers = {"X-RateLimit-Remaining": "100"}
    mock_response.json.return_value = []
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    client = GitHubClient(owner="dbt-labs", repo="dbt-core", token="test-token")
    since = datetime(2026, 1, 1, 0, 0, 0)
    client.fetch_issues(state="all", since=since)

    call_params = mock_get.call_args.kwargs["params"]
    assert call_params["since"] == "2026-01-01T00:00:00Z"
    assert call_params["state"] == "all"


@patch("src.etl.github_client.requests.get")
def test_fetch_issues_excludes_pull_requests(mock_get, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    mock_response = MagicMock()
    mock_response.headers = {"X-RateLimit-Remaining": "100"}
    mock_response.json.return_value = [
        {"number": 1, "title": "issue"},
        {"number": 2, "title": "pr", "pull_request": {}},
    ]
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    client = GitHubClient(owner="dbt-labs", repo="dbt-core", token="test-token")
    issues = client.fetch_issues()

    assert len(issues) == 1
    assert issues[0]["number"] == 1


def test_transform_records_shape(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    pipeline = IngestGitHubIssuesPipeline()
    rows = pipeline.transform_records(SAMPLE_ISSUES, batch_id=1, delivery_id=10)

    assert len(rows) == 2
    assert rows[0][3] == "1"
    assert rows[0][5] == 1
    assert rows[0][7] == "open"
    assert rows[0][11] == "alice"


def test_filter_new_rows_skips_duplicate_content_hash(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
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


def test_filter_new_rows_allows_new_delivery(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
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
