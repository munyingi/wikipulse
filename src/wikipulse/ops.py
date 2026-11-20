"""Operations helpers: backfill planning, freshness, run logging and cost estimates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

# Fabric capacity: 1 capacity unit (CU) = 2 Spark vCores. Pay-as-you-go price per CU hour, set it to your
# region's price (Azure pricing page). Used for estimates only.
SPARK_VCORES_PER_CU = 2
DEFAULT_EUR_PER_CU_HOUR = 0.18


def expected_hours(start: datetime, end: datetime) -> list[datetime]:
    """Every hour from ``start`` (inclusive) to ``end`` (exclusive), truncated to the hour, in UTC."""
    s = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    e = end.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    out = []
    while s < e:
        out.append(s)
        s += timedelta(hours=1)
    return out


def missing_hours(start: datetime, end: datetime, loaded: set[datetime]) -> list[datetime]:
    """Hours in the window that have not been loaded successfully. This is the backfill work list."""
    norm = {h.astimezone(UTC) if h.tzinfo else h.replace(tzinfo=UTC) for h in loaded}
    return [h for h in expected_hours(start, end) if h not in norm]


def latest_available_hour(now: datetime, publish_delay: timedelta = timedelta(minutes=50)) -> datetime:
    """Most recent hour whose dump file should already be published. Wikimedia publishes each hourly file
    shortly after the hour ends; ``publish_delay`` adds a safety margin."""
    ready = now.astimezone(UTC) - publish_delay - timedelta(hours=1)
    return ready.replace(minute=0, second=0, microsecond=0)


@dataclass
class Freshness:
    latest_loaded: datetime | None
    lag_hours: float
    sla_hours: float

    @property
    def ok(self) -> bool:
        return self.latest_loaded is not None and self.lag_hours <= self.sla_hours


def freshness(latest_loaded: datetime | None, now: datetime, sla_hours: float = 3.0) -> Freshness:
    if latest_loaded is None:
        return Freshness(None, float("inf"), sla_hours)
    ll = latest_loaded if latest_loaded.tzinfo else latest_loaded.replace(tzinfo=UTC)
    lag = (now.astimezone(UTC) - (ll + timedelta(hours=1))).total_seconds() / 3600
    return Freshness(ll, round(lag, 2), sla_hours)


def cu_seconds(spark_vcores: float, seconds: float) -> float:
    return spark_vcores / SPARK_VCORES_PER_CU * seconds


def eur_estimate(cu_secs: float, eur_per_cu_hour: float = DEFAULT_EUR_PER_CU_HOUR) -> float:
    return round(cu_secs / 3600 * eur_per_cu_hour, 4)


def monthly_eur(eur_per_run: float, runs_per_day: float) -> float:
    return round(eur_per_run * runs_per_day * 30, 2)
