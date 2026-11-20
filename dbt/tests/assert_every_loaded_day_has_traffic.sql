-- If the pipeline loaded hours for a day, the traffic mart must have rows for that day.
select h.date_utc
from (select distinct cast(hour_utc as date) as date_utc from {{ source('ops', 'hour_status') }}
      where status = 'succeeded') as h
left join (select distinct date_utc from {{ ref('fct_wiki_daily_traffic') }}) as t on t.date_utc = h.date_utc
where t.date_utc is null
