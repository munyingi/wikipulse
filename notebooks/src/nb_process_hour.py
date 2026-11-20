# %% [parameters]
hour = ""  # ISO hour in UTC. Empty = latest published hour.

# %%
from datetime import UTC, datetime

from wikipulse import ops, pipeline

h = datetime.fromisoformat(hour.replace("Z", "+00:00")) if hour else ops.latest_available_hour(datetime.now(UTC))
executors = int(spark.conf.get("spark.executor.instances", "1"))
cores = int(spark.conf.get("spark.executor.cores", "4"))
summary = pipeline.process_hour(spark, RAW_ROOT, h, expected_rows=pipeline.expected_volume(spark),
                                vcores=(executors + 1) * cores)
print(summary)
