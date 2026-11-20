-- One row per day: did the hourly pipeline run, how long, what did it cost, did quality checks pass.
with runs as (
    select
        cast(finished_at as date) as run_date,
        count(*) as runs,
        sum(case when status = 'succeeded' then 1 else 0 end) as succeeded,
        avg(seconds) as avg_seconds,
        max(seconds) as max_seconds,
        sum(rows_silver) as rows_silver,
        sum(cu_seconds) as cu_seconds,
        sum(eur_estimate) as eur_estimate
    from {{ ref('stg_pipeline_runs') }}
    group by cast(finished_at as date)
),

checks as (
    select
        cast(checked_at as date) as run_date,
        sum(case when passed = 0 and severity = 'error' then 1 else 0 end) as failed_error_checks,
        sum(case when passed = 0 and severity = 'warn' then 1 else 0 end) as failed_warn_checks
    from {{ ref('stg_quality_results') }}
    group by cast(checked_at as date)
)

select
    r.run_date,
    r.runs,
    r.succeeded,
    24 - r.succeeded as hours_missing,
    r.avg_seconds,
    r.max_seconds,
    r.rows_silver,
    r.cu_seconds,
    r.eur_estimate,
    coalesce(c.failed_error_checks, 0) as failed_error_checks,
    coalesce(c.failed_warn_checks, 0) as failed_warn_checks
from runs as r
left join checks as c on c.run_date = r.run_date
