# GitHub Issues Pipeline

End-to-end pipeline ingesting issues from the public `dbt-labs/dbt-core` repository via the free REST API.

## Source

| Field | Value |
|-------|-------|
| API | `GET /repos/{owner}/{repo}/issues` |
| Default repo | `dbt-labs/dbt-core` |
| `source_name` | `github_issues` |
| `delivery_type` | `API` |
| Business key | `issue_number` (`source_record_id`) |
| Auth | `GITHUB_TOKEN` in `.env` (**required**) |
| Window | `GITHUB_SINCE_DAYS` (default `90`; set `0` for full history) |

Config via `.env`:

```env
GITHUB_TOKEN=your_token_here
GITHUB_REPO_OWNER=dbt-labs
GITHUB_REPO_NAME=dbt-core
GITHUB_SINCE_DAYS=90
```

Pull requests are excluded (GitHub returns PRs on the issues endpoint; the client filters them out).

## Data flow

```text
ingest_github_issues
        ↓
build_silver_github_issues
        ↓
build_gold_github_issue_metrics
```

## Large repo notes

- **Cost:** $0 — public API and free PAT; the 90-day window controls runtime/volume, not billing.
- **Rate limits:** With `GITHUB_TOKEN`, ~5,000 requests/hour. Without a token the pipeline fails fast.
- **Adjust window:** Increase `GITHUB_SINCE_DAYS` (e.g. `180`, `365`) or set `0` for full history — all still free.
- **Disk:** ~20–80 MB for a full dbt-core issues pull; 90-day window is smaller.

## Acceptance criteria

### Bronze (`ingest_github_issues`)

- [ ] Pipeline completes with `etl.batch_run.status = SUCCESS`
- [ ] One `etl.source_delivery` row per API pull with `delivery_type = API`
- [ ] `content_hash` set from canonical JSON response body
- [ ] Each issue stored with `source_record_id = issue_number`
- [ ] Fails fast with clear error if `GITHUB_TOKEN` is missing
- [ ] RELOAD creates new delivery; old Bronze rows retained

### Silver (`build_silver_github_issues`)

- [ ] One row per `issue_number` in `silver.github_issues_current`
- [ ] SCD2 history in `silver.github_issues_history`
- [ ] Tracked fields: `title`, `state`, `labels_json`, `user_login`, `closed_at`
- [ ] Delivery logged in `etl.silver_delivery_log`
- [ ] Re-run is idempotent for already-processed deliveries

### Gold (`build_gold_github_issue_metrics`)

- [ ] `gold.daily_open_issues` built from `silver.github_issues_history`
- [ ] `gold.issue_resolution_time` built from `silver.github_issues_current`
- [ ] No negative `resolution_days`
- [ ] Gold reads Silver only (not Bronze)

## Validation SQL

```sql
USE mini_dwh;

-- Bronze
SELECT COUNT(*) AS bronze_issues FROM bronze.github_issues_raw;

-- Silver
SELECT COUNT(*) AS silver_current FROM silver.github_issues_current;
SELECT COUNT(*) AS silver_history FROM silver.github_issues_history;

-- Current rows must have matching is_current history
SELECT COUNT(*) AS orphan_current
FROM silver.github_issues_current c
WHERE NOT EXISTS (
    SELECT 1 FROM silver.github_issues_history h
    WHERE h.issue_number = c.issue_number AND h.is_current = 1
);

-- Gold
SELECT COUNT(*) AS daily_open_rows FROM gold.daily_open_issues;
SELECT COUNT(*) AS resolution_rows FROM gold.issue_resolution_time;

-- No negative resolution days
SELECT COUNT(*) AS bad_resolution
FROM gold.issue_resolution_time
WHERE resolution_days < 0;

-- Latest batch status
SELECT TOP 5 pipeline_name, status, rows_loaded, start_ts
FROM etl.batch_run
WHERE pipeline_name LIKE '%github%'
ORDER BY batch_id DESC;
```

## Execution

```bash
cd /workspaces/mini-dwh
export PYTHONPATH=$(pwd)

python -m src.pipelines.ingest_github_issues
python -m src.pipelines.build_silver_github_issues
python -m src.pipelines.build_gold_github_issue_metrics
```

## Cost and storage

- **Cost:** Free (public GitHub REST API + free PAT)
- **Disk (90-day dbt-core):** typically tens of MB — negligible vs Olist data
