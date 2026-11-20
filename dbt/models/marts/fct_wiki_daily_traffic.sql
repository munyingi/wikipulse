select
    date_utc,
    wiki,
    language,
    project,
    sum(case when access = 'desktop' then cast(views as bigint) else 0 end) as views_desktop,
    sum(case when access = 'mobile' then cast(views as bigint) else 0 end) as views_mobile,
    sum(cast(views as bigint)) as views,
    count(distinct hour_utc) as hours_loaded,
    cast(sum(case when access = 'mobile' then cast(views as bigint) else 0 end) as float)
        / nullif(sum(cast(views as bigint)), 0) as mobile_share
from {{ ref('stg_wiki_hourly_traffic') }}
group by date_utc, wiki, language, project
