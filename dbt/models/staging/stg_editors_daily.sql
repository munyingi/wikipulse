select date_utc, wiki, editor, is_bot, edits, pages, bytes_changed
from {{ source('gold', 'editors_daily') }}
