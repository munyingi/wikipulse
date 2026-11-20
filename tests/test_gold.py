from datetime import date

from pyspark.sql import Row

from wikipulse import gold


def daily(spark, rows):
    return spark.createDataFrame(
        [Row(date_utc=d, wiki="en.wikipedia", page_title=p, is_article=a, views=v) for d, p, a, v in rows]
    )


def test_daily_page_views_splits_access(spark):
    silver = spark.createDataFrame(
        [
            Row(
                hour_utc="h1",
                date_utc=date(2026, 11, 20),
                wiki="en.wikipedia",
                project="wikipedia",
                language="en",
                access=acc,
                page_title="Oulu",
                is_article=True,
                views=v,
            )
            for acc, v in [("desktop", 10), ("mobile", 30), ("mobile", 5)]
        ]
    )
    row = gold.daily_page_views(silver).first()
    assert (row.views_desktop, row.views_mobile, row.views) == (10, 35, 45)


def test_top_pages_exclude_main_page_and_non_articles(spark):
    d = date(2026, 11, 20)
    df = daily(
        spark,
        [
            (d, "Main_Page", True, 10**6),
            (d, "Talk:X", False, 900),
            (d, "A", True, 50),
            (d, "B", True, 70),
            (d, "C", True, 60),
        ],
    )
    top = [(r.rank, r.page_title) for r in gold.top_pages_daily(df, n=2).orderBy("rank").collect()]
    assert top == [(1, "B"), (2, "C")]


def test_trending_compares_with_previous_days(spark):
    rows = [(date(2026, 11, d), "Steady", True, 2000) for d in range(13, 21)]
    rows += [(date(2026, 11, d), "Spike", True, 100) for d in range(13, 20)] + [
        (date(2026, 11, 20), "Spike", True, 5000)
    ]
    t = gold.trending_pages(daily(spark, rows), min_views=1000).where("date_utc = DATE'2026-11-20'")
    scores = {r.page_title: r.spike_score for r in t.collect()}
    assert scores["Spike"] == 50.0 and scores["Steady"] == 1.0


def test_salted_join_matches_plain_join(spark):
    big = spark.createDataFrame([Row(k="hot", v=i) for i in range(500)] + [Row(k=f"c{i}", v=i) for i in range(50)])
    small = spark.createDataFrame([Row(k="hot", w=1)] + [Row(k=f"c{i}", w=i) for i in range(0, 50, 2)])
    salted = gold.salted_join(big, small, ["k"], [("hot",)], buckets=8)
    plain = big.join(small, "k", "left")
    assert sorted(salted.collect()) == sorted(plain.select(*salted.columns).collect())
