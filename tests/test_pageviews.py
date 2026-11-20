from datetime import UTC, datetime

import pytest
from pyspark.sql import Row

from wikipulse import pageviews

H = datetime(2026, 11, 20, 13, 0, tzinfo=UTC)


def test_file_is_named_after_the_end_of_the_hour():
    assert pageviews.file_name(H) == "pageviews-20261120-140000.gz"
    assert pageviews.dump_url(H).endswith("/2026/2026-11/pageviews-20261120-140000.gz")
    # the hour from 23:00 lands in the next day's (and month's) folder
    late = datetime(2026, 11, 30, 23, 0, tzinfo=UTC)
    assert pageviews.dump_url(late).endswith("/2026/2026-12/pageviews-20261201-000000.gz")


def test_raw_path_is_partitioned_by_start_hour():
    assert pageviews.raw_path("Files/raw", H) == "Files/raw/pageviews/2026/11/20/pageviews-20261120-140000.gz"


@pytest.mark.parametrize(
    "domain,wiki,access",
    [
        ("en", "en.wikipedia", "desktop"),
        ("en.m", "en.wikipedia", "mobile"),
        ("de.voy", "de.wikivoyage", "desktop"),
        ("de.m.voy", "de.wikivoyage", "mobile"),
        ("commons.m", "commons.wikimedia", "desktop"),
        ("commons.m.m", "commons.wikimedia", "mobile"),
        ("www.wd", "www.wikidata", "desktop"),
        ("en.d", "en.wiktionary", "desktop"),
        ("xx.zz", "xx.other", "desktop"),
    ],
)
def test_domain_code_classification(spark, domain, wiki, access):
    lines = spark.createDataFrame([Row(value=f"{domain} Some_Page 5 0")])
    row = pageviews.to_silver(pageviews.parse_lines(lines, H)).first()
    assert (row.wiki, row.access) == (wiki, access)


def test_silver_drops_bad_rows_and_sums_duplicates(spark):
    lines = spark.createDataFrame(
        [
            Row(value=v)
            for v in [
                "en Helsinki 10 0",
                "en Helsinki 5 0",  # same page twice in one file -> summed
                "en.m Helsinki 7 0",
                "en Talk:Helsinki 2 0",
                "en broken_line",  # malformed
                "en Zero 0 0",  # zero views
                "de Seite NaN 0",  # bad number
            ]
        ]
    )
    bronze = pageviews.parse_lines(lines, H)
    assert bronze.count() == 7  # bronze keeps everything
    silver = {(r.access, r.page_title): (r.views, r.is_article) for r in pageviews.to_silver(bronze).collect()}
    assert silver == {
        ("desktop", "Helsinki"): (15, True),
        ("mobile", "Helsinki"): (7, True),
        ("desktop", "Talk:Helsinki"): (2, False),
    }
