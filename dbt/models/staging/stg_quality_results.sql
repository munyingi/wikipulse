select hour_utc, [check] as check_name, [table] as table_name, severity, passed, observed, threshold, checked_at
from {{ source('ops', 'quality_results') }}
