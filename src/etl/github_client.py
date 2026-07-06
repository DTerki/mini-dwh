import os
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

GITHUB_API_BASE = "https://api.github.com"
REQUEST_TIMEOUT_SECONDS = 30


class GitHubApiError(Exception):
    """Raised when the GitHub API returns an error response."""


class GitHubClient:
    def __init__(
        self,
        owner: str | None = None,
        repo: str | None = None,
        token: str | None = None,
    ) -> None:
        self.owner = owner or os.getenv("GITHUB_REPO_OWNER", "terkd")
        self.repo = repo or os.getenv("GITHUB_REPO_NAME", "mini-dwh")
        self.token = token if token is not None else os.getenv("GITHUB_TOKEN", "")

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
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

    def fetch_issues(self, state: str = "all") -> list[dict[str, Any]]:
        """
        Fetch repository issues (excludes pull requests) with pagination.
        """
        url = f"{GITHUB_API_BASE}/repos/{self.owner}/{self.repo}/issues"
        page = 1
        all_issues: list[dict[str, Any]] = []

        while True:
            params = {
                "state": state,
                "per_page": 100,
                "page": page,
            }
            page_data = self._get_page(url, params)

            issues_only = [item for item in page_data if "pull_request" not in item]
            all_issues.extend(issues_only)

            if len(page_data) < 100:
                break
            page += 1

        return all_issues

    @property
    def source_object_name(self) -> str:
        return f"{self.owner}/{self.repo}/issues?state=all"
