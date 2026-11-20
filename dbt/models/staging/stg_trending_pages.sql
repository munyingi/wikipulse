select date_utc, wiki, language, page_title, views, baseline_views, spike_score
from {{ source('gold', 'trending_pages') }}
