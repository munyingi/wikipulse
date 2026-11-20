select date_utc, wiki, language, project, page_title, views, views_desktop, views_mobile, [rank] as page_rank
from {{ source('gold', 'top_pages_daily') }}
