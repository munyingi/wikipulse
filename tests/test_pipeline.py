"""End-to-end on Delta tables: idempotent re-runs and backfill. Needs delta-spark (runs in CI)."""

from datetime import UTC, datetime, timedelta

from conftest import needs_delta

from wikipulse import ops, pipeline, sample

H0 = datetime(2026, 11, 16, 0, tzinfo=UTC)


@needs_delta
def test_rerun_and_backfill_are_idempotent(spark, tmp_path):
    raw = str(tmp_path / "raw")
    hours = [H0 + timedelta(hours=i) for i in range(4)]
    for h in hours:
        sample.write_hour(raw, h, n_pages=400)

    for h in hours[:2] + hours[3:]:  # hour 2 is "missed"
        pipeline.process_hour(spark, raw, h)
    todo = ops.missing_hours(hours[0], hours[-1] + timedelta(hours=1), pipeline.loaded_hours(spark))
    assert todo == [hours[2]]

    for h in todo:
        pipeline.process_hour(spark, raw, h)
    before = {t: spark.table(t).count() for t in (pipeline.T_SILVER, pipeline.T_HOURLY, pipeline.T_DAILY)}
    total_before = spark.table(pipeline.T_SILVER).agg({"views": "sum"}).first()[0]

    pipeline.process_hour(spark, raw, hours[1])  # re-run an hour that was already loaded
    after = {t: spark.table(t).count() for t in before}
    assert after == before
    assert spark.table(pipeline.T_SILVER).agg({"views": "sum"}).first()[0] == total_before
    assert spark.table(pipeline.T_STATUS).count() == 4
    assert spark.table(pipeline.T_SILVER).select("hour_utc").distinct().count() == 4
