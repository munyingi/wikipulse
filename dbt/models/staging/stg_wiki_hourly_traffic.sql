select hour_utc, date_utc, wiki, project, language, access, views, pages_viewed
from {{ source('gold', 'wiki_hourly_traffic') }}
