# %% [parameters]
start = ""  # ISO hour, inclusive. Empty = 48 hours ago.
end = ""    # ISO hour, exclusive. Empty = latest published hour + 1.

# %%
from datetime import UTC, datetime, timedelta

from wikipulse import ops, pipeline
from wikipulse.download import NotPublishedYet, fetch_hour

now = datetime.now(UTC)
s = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else now - timedelta(hours=48)
e = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else ops.latest_available_hour(now) + timedelta(hours=1)

todo = ops.missing_hours(s, e, pipeline.loaded_hours(spark))
print(f"{len(todo)} hour(s) to backfill")
done = []
for h in todo:
    try:
        fetch_hour(h, RAW_ROOT)
    except NotPublishedYet:
        print(f"{h:%Y-%m-%d %H:00} not published yet, stopping")
        break
    pipeline.process_hour(spark, RAW_ROOT, h, expected_rows=pipeline.expected_volume(spark))
    done.append(f"{h:%Y-%m-%dT%H:00Z}")
notebookutils.notebook.exit(",".join(done))
