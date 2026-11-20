-- One row per wiki seen in the traffic data.
select
    wiki,
    max(language) as language,
    max(project) as project,
    min(date_utc) as first_seen_date,
    sum(cast(views as bigint)) as views_to_date
from {{ ref('stg_wiki_hourly_traffic') }}
group by wiki
