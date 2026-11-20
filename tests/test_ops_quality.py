from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import Row

from wikipulse import ops, quality

T0 = datetime(2026, 11, 20, 0, tzinfo=UTC)


def test_missing_hours_is_the_backfill_list():
    loaded = {T0 + timedelta(hours=h) for h in (0, 1, 3, 5)}
    todo = ops.missing_hours(T0, T0 + timedelta(hours=6), loaded)
    assert [h.hour for h in todo] == [2, 4]


def test_latest_available_hour_respects_publish_delay():
    assert ops.latest_available_hour(datetime(2026, 11, 20, 14, 30, tzinfo=UTC)) == T0.replace(hour=12)
    assert ops.latest_available_hour(datetime(2026, 11, 20, 14, 55, tzinfo=UTC)) == T0.replace(hour=13)


def test_freshness_sla():
    f = ops.freshness(T0.replace(hour=10), datetime(2026, 11, 20, 13, 30, tzinfo=UTC), sla_hours=3)
    assert f.lag_hours == 2.5 and f.ok
    assert not ops.freshness(None, T0).ok


def test_cost_estimate():
    cu = ops.cu_seconds(spark_vcores=8, seconds=180)  # 4 CU for 3 minutes
    assert cu == 720
    assert ops.eur_estimate(cu, eur_per_cu_hour=0.18) == 0.036
    assert ops.monthly_eur(0.036, 24) == 25.92


def test_quality_checks_and_enforce(spark):
    df = spark.createDataFrame([Row(k=1, v="a"), Row(k=1, v=None)])
    results = [quality.not_null(df, "t", ["v"]), quality.unique(df, "t", ["k"]), quality.row_count_at_least(df, "t", 1)]
    assert [r.passed for r in results] == [False, False, True]
    with pytest.raises(quality.QualityError, match="not_null"):
        quality.enforce(results)
    quality.enforce([quality.volume_within(10, 100, "t")])  # warn only, does not raise


def test_volume_check():
    assert quality.volume_within(95, 100, "t").passed
    assert not quality.volume_within(30, 100, "t").passed
