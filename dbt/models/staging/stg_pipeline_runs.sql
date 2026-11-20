select run_id, pipeline, hour_utc, status, rows_bronze, rows_silver, seconds, cu_seconds, eur_estimate, finished_at
from {{ source('ops', 'pipeline_runs') }}
