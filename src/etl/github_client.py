import os
from datetime import datetime
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

GITHUB_API_BASE = "https://api.github.com"
REQUEST_TIMEOUT_SECONDS = 30


class GitHubApiError(Exception):
    """Raised when the GitHub API returns an error response."""


class GitHubConfigurationError(Exception):
    """Raised when required GitHub configuration is missing."""


class GitHubClient:
    def __init__(
        self,
        owner: str | None = None,
        repo: str | None = None,
        token: str | None = None,
    ) -> None:
        self.owner = owner or os.getenv("GITHUB_REPO_OWNER", "dbt-labs")
        self.repo = repo or os.getenv("GITHUB_REPO_NAME", "dbt-core")
        self.token = token if token is not None else os.getenv("GITHUB_TOKEN", "")
        self._last_since: str | None = None
        self._require_token()

    def _require_token(self) -> None:
        if not self.token or not self.token.strip():
            raise GitHubConfigurationError(
                "GITHUB_TOKEN is required. Create a free Personal Access Token "
                "with read access to public repos and set it in .env."
            )

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Authorization": f"Bearer {self.token.strip()}",
        }
        return headers

    @retry(
        retry=retry_if_exception_type((requests.RequestException, GitHubApiError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _get_page(self, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = requests.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        remaining = response.headers.get("X-RateLimit-Remaining")
        if remaining is not None:
            print(f"[GitHub API] rate_limit_remaining={remaining}")

        if response.status_code in {403, 429}:
            raise GitHubApiError(
                f"GitHub rate limit or forbidden: status={response.status_code}"
            )

        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise GitHubApiError("GitHub issues response is not a list")
        return data

    def fetch_issues(
        self,
        state: str = "all",
        since: datetime | str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch repository issues (excludes pull requests) with pagination.

        since: ISO-8601 timestamp; only issues updated at or after this time.
        """
        url = f"{GITHUB_API_BASE}/repos/{self.owner}/{self.repo}/issues"
        page = 1
        all_issues: list[dict[str, Any]] = []

        since_iso: str | None = None
        if since is not None:
            if isinstance(since, datetime):
                since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                since_iso = since
        self._last_since = since_iso

        while True:
            params: dict[str, Any] = {
                "state": state,
                "per_page": 100,
                "page": page,
            }
            if since_iso:
                params["since"] = since_iso

            page_data = self._get_page(url, params)

            issues_only = [item for item in page_data if "pull_request" not in item]
            all_issues.extend(issues_only)

            if len(page_data) < 100:
                break
            page += 1

        return all_issues

    def source_object_name(self, state: str = "all") -> str:
        base = f"{self.owner}/{self.repo}/issues?state={state}"
        if self._last_since:
            return f"{base}&since={self._last_since}"
        return base
