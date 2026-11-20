select date_utc, wiki, page_title, views, edits, editors, bytes_changed
from {{ source('gold', 'edits_vs_views') }}
