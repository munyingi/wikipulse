{{ config(post_hook=[mask_column('editor', 'partial(2, "****", 0)')]) }}
-- Editor names are personal data: masked for every reader without the UNMASK permission.
select
    date_utc,
    wiki,
    left(wiki, charindex('.', wiki + '.') - 1) as language,
    editor,
    is_bot,
    edits,
    pages,
    bytes_changed
from {{ ref('stg_editors_daily') }}
