-- Pages that many people read but few people edit, and the reverse.
select
    date_utc,
    wiki,
    page_title,
    views,
    edits,
    editors,
    bytes_changed,
    case when edits = 0 then null else cast(views as float) / edits end as views_per_edit
from {{ ref('stg_edits_vs_views') }}
where views >= 1000 or edits >= 5
