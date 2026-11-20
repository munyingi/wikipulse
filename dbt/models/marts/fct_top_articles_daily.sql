select
    t.date_utc,
    t.wiki,
    t.language,
    t.page_rank,
    t.page_title,
    replace(t.page_title, '_', ' ') as article,
    t.views,
    t.views_mobile,
    tr.spike_score,
    coalesce(e.edits, 0) as edits,
    coalesce(e.editors, 0) as editors
from {{ ref('stg_top_pages_daily') }} as t
left join {{ ref('stg_trending_pages') }} as tr
    on tr.date_utc = t.date_utc and tr.wiki = t.wiki and tr.page_title = t.page_title
left join {{ ref('stg_edits_vs_views') }} as e
    on e.date_utc = t.date_utc and e.wiki = t.wiki and e.page_title = t.page_title
