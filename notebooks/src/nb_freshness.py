# %% [parameters]
sla_hours = 3.0

# %%
from datetime import UTC, datetime

from wikipulse import ops, pipeline

loaded = pipeline.loaded_hours(spark)
f = ops.freshness(max(loaded) if loaded else None, datetime.now(UTC), sla_hours)
print(f"latest hour {f.latest_loaded}, lag {f.lag_hours} h, SLA {f.sla_hours} h")
if not f.ok:
    raise RuntimeError(f"Freshness SLA breached: page views are {f.lag_hours} h behind (SLA {f.sla_hours} h)")
