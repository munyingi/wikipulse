# Runbook

Alerts arrive in Teams or Slack from the Airflow `on_failure_callback` (`WIKIPULSE_ALERT_URL`).

| Alert | Likely cause | What to do |
|---|---|---|
| `download_hour` failed, "not published yet" | Wikimedia is late publishing the hour | Nothing; the task retries for an hour. If it still fails, check https://dumps.wikimedia.org/other/pageviews/ and run the backfill DAG later. |
| `download_hour` failed, gzip error | Truncated download | Re-run the task. The temp file is discarded, so nothing partial was stored. |
| `process_hour` failed with `QualityError` | An error-level check failed (empty file, nulls, duplicate keys) | Look at `ops.quality_results` for that hour. Gold was not touched. Fix the cause, then clear the task; the hour is replaced, not appended. |
| `volume_within` warning on the dashboard | Hour much smaller or larger than recent hours | Usually a partial upstream file or a real traffic event. Compare with `gold.wiki_hourly_traffic`; re-download with `fetch_hour(..., force=True)` if partial. |
| `check_freshness` failed | No hour loaded within the SLA (3 h) | Check the hourly DAG runs. If several hours are missing, trigger `wikipulse_backfill` with `start`/`end`. |
| `dbt_build` failed | Warehouse schema drift or a failing dbt test | Run `dbt build --select state:modified+` locally against dev, fix, redeploy. Security policies are dropped at run start and recreated at the end. |
| Dashboard shows no new edits | Producer DAG paused, Key Vault access, or Eventstream stopped | Check the producer notebook run; check the Eventstream data preview; `RecentChanges | summarize max(ingestion_time())`. |

## Useful queries

```sql
-- Hours loaded in the last day, with run time and estimated cost (lakehouse SQL endpoint)
SELECT hour_utc, status, rows_silver, seconds, eur_estimate
FROM ops.pipeline_runs WHERE finished_at > dateadd(day, -1, getdate()) ORDER BY hour_utc DESC;

-- Failed checks
SELECT * FROM ops.quality_results WHERE passed = 0 ORDER BY checked_at DESC;
```

```kusto
// Ingestion lag of the live feed
RecentChanges | where ingestion_time() > ago(15m)
| summarize p95_lag_s = percentile(datetime_diff('second', ingestion_time(), event_time), 95)
```

## Re-processing a day

Hours are idempotent, so re-processing is safe: trigger `wikipulse_backfill` with the day's `start` and `end` after deleting those hours from `ops.hour_status`, or run `nb_process_hour` with `hour=...` for single hours. Daily gold tables are recomputed from silver each time an hour of that day is loaded.
