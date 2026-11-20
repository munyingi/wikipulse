-- Articles with at least 5x their usual daily views.
select date_utc, wiki, language, page_title, replace(page_title, '_', ' ') as article, views, baseline_views,
       spike_score
from {{ ref('stg_trending_pages') }}
where spike_score >= 5
