"""Wikimedia hourly page view dumps: download location, parsing and the silver transform.

File format (one line per page per hour, space separated):

    domain_code page_title count_views total_response_size

``domain_code`` encodes language, access method and project, for example
``en`` (English Wikipedia, desktop), ``en.m`` (mobile), ``de.voy`` (German Wikivoyage),
``commons.m`` (Wikimedia Commons, mobile). See
https://dumps.wikimedia.org/other/pageviews/readme.html
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

DUMPS_BASE = "https://dumps.wikimedia.org/other/pageviews"

PROJECT_CODES = {
    "": "wikipedia",
    "b": "wikibooks",
    "d": "wiktionary",
    "f": "wikimediafoundation",
    "m": "wikimedia",
    "n": "wikinews",
    "q": "wikiquote",
    "s": "wikisource",
    "v": "wikiversity",
    "voy": "wikivoyage",
    "w": "mediawiki",
    "wd": "wikidata",
}

# Non-article namespaces on English-language wikis. Titles with these prefixes are kept but flagged.
NON_ARTICLE_PREFIXES = (
    "Special:",
    "Talk:",
    "User:",
    "User_talk:",
    "Wikipedia:",
    "Wikipedia_talk:",
    "File:",
    "File_talk:",
    "MediaWiki:",
    "Template:",
    "Template_talk:",
    "Help:",
    "Category:",
    "Portal:",
    "Draft:",
    "Module:",
)

RAW_SCHEMA = T.StructType(
    [
        T.StructField("domain_code", T.StringType()),
        T.StructField("page_title", T.StringType()),
        T.StructField("views", T.LongType()),
        T.StructField("response_bytes", T.LongType()),
    ]
)


def hour_floor(ts: datetime) -> datetime:
    ts = ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC)
    return ts.replace(minute=0, second=0, microsecond=0)


def file_name(hour: datetime) -> str:
    """Dump file for the hour that *starts* at ``hour``. Wikimedia names files after the hour's end."""
    end = hour_floor(hour) + timedelta(hours=1)
    return f"pageviews-{end:%Y%m%d-%H}0000.gz"


def dump_url(hour: datetime) -> str:
    end = hour_floor(hour) + timedelta(hours=1)
    return f"{DUMPS_BASE}/{end:%Y}/{end:%Y-%m}/{file_name(hour)}"


def raw_path(root: str, hour: datetime) -> str:
    h = hour_floor(hour)
    return f"{root.rstrip('/')}/pageviews/{h:%Y}/{h:%m}/{h:%d}/{file_name(h)}"


def parse_lines(lines: DataFrame, hour: datetime) -> DataFrame:
    """Bronze: split raw text lines (column ``value``) into typed columns. Malformed lines are kept with nulls
    so the quality checks can count them instead of silently dropping data."""
    parts = F.split(F.col("value"), " ")
    h = hour_floor(hour).replace(tzinfo=None)
    return lines.select(
        F.lit(h).cast("timestamp").alias("hour_utc"),
        parts.getItem(0).alias("domain_code"),
        parts.getItem(1).alias("page_title"),
        parts.getItem(2).cast("long").alias("views"),
        F.size(parts).alias("n_fields"),
        F.col("value").alias("raw_line"),
    )


WIKIMEDIA_SITES = ("commons", "meta", "species", "incubator", "outreach", "wikimania", "foundation", "wikitech")


def _classify(parts: Column) -> tuple[Column, Column]:
    """Return (project, is_mobile) from the split domain code.

    ``en.m`` is mobile Wikipedia but ``commons.m`` is desktop Wikimedia Commons, because ``m`` is both the
    mobile marker and the Wikimedia project code. The mobile marker always sits in second position.
    """
    n = F.size(parts)
    first, second, last = F.element_at(parts, 1), F.element_at(parts, 2), F.element_at(parts, -1)
    is_site = first.isin(*WIKIMEDIA_SITES)
    is_mobile = ((n == 3) & (second == "m")) | ((n == 2) & (second == "m") & ~is_site)
    code = F.when(n == 1, F.lit("")).when((n == 2) & (second == "m") & ~is_site, F.lit("")).otherwise(last)
    mapping = F.create_map(*[x for k, v in PROJECT_CODES.items() for x in (F.lit(k), F.lit(v))])
    return F.coalesce(mapping[code], F.lit("other")), is_mobile


def to_silver(bronze: DataFrame) -> DataFrame:
    """Silver: valid rows only, one row per (hour, wiki, access, page) with views summed."""
    valid = bronze.where(
        (F.col("n_fields") == 4)
        & F.col("views").isNotNull()
        & (F.col("views") > 0)
        & F.col("page_title").isNotNull()
        & (F.col("page_title") != "-")
    )
    parts = F.split(F.col("domain_code"), r"\.")
    project, is_mobile = _classify(parts)
    language = F.lower(F.element_at(parts, 1))
    is_article = ~F.col("page_title").rlike("^(" + "|".join(p.rstrip(":") for p in NON_ARTICLE_PREFIXES) + "):")
    enriched = valid.select(
        "hour_utc",
        F.concat_ws(".", language, project).alias("wiki"),
        language.alias("language"),
        project.alias("project"),
        F.when(is_mobile, "mobile").otherwise("desktop").alias("access"),
        "page_title",
        is_article.alias("is_article"),
        "views",
    )
    return (
        enriched.groupBy("hour_utc", "wiki", "language", "project", "access", "page_title", "is_article")
        .agg(F.sum("views").alias("views"))
        .withColumn("date_utc", F.to_date("hour_utc"))
    )
