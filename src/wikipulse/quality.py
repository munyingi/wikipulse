"""Data quality checks with explicit severities. Results are written to ``ops.quality_results``.

An ``error`` check stops the pipeline (the notebook raises, Airflow marks the task failed and alerts).
A ``warn`` check is recorded and shown on the dashboard but lets the data through.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    check: str
    table: str
    severity: str
    passed: bool
    observed: float
    threshold: float
    detail: str = ""

    def as_row(self) -> dict:
        return asdict(self)


def row_count_at_least(df: DataFrame, table: str, minimum: int, severity: str = "error") -> CheckResult:
    n = df.count()
    return CheckResult("row_count_at_least", table, severity, n >= minimum, n, minimum)


def not_null(df: DataFrame, table: str, cols: list[str], severity: str = "error") -> CheckResult:
    cond = F.lit(False)
    for c in cols:
        cond = cond | F.col(c).isNull()
    bad = df.where(cond).count()
    return CheckResult("not_null", table, severity, bad == 0, bad, 0, ",".join(cols))


def unique(df: DataFrame, table: str, keys: list[str], severity: str = "error") -> CheckResult:
    dupes = df.groupBy(*keys).count().where("count > 1").count()
    return CheckResult("unique", table, severity, dupes == 0, dupes, 0, ",".join(keys))


def malformed_ratio_at_most(bronze: DataFrame, table: str, max_ratio: float, severity: str = "warn") -> CheckResult:
    agg = bronze.agg(
        F.count("*").alias("n"),
        F.sum(F.when((F.col("n_fields") != 4) | F.col("views").isNull(), 1).otherwise(0)).alias("bad"),
    ).first()
    ratio = (agg["bad"] or 0) / agg["n"] if agg["n"] else 1.0
    return CheckResult("malformed_ratio_at_most", table, severity, ratio <= max_ratio, round(ratio, 6), max_ratio)


def volume_within(
    observed: int, expected: float, table: str, tolerance: float = 0.5, severity: str = "warn"
) -> CheckResult:
    """Flags an hour whose volume is far from the recent average (a partial file or an outage upstream)."""
    if expected <= 0:
        return CheckResult("volume_within", table, severity, True, observed, expected, "no history")
    ratio = observed / expected
    return CheckResult(
        "volume_within",
        table,
        severity,
        abs(1 - ratio) <= tolerance,
        round(ratio, 3),
        tolerance,
        f"expected about {expected:,.0f}",
    )


def enforce(results: list[CheckResult]) -> list[CheckResult]:
    failed = [r for r in results if not r.passed and r.severity == "error"]
    if failed:
        raise QualityError(
            "; ".join(f"{r.check} on {r.table}: observed {r.observed}, threshold {r.threshold}" for r in failed)
        )
    return results
