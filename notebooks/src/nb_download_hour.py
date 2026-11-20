# %% [parameters]
hour = ""  # ISO hour in UTC, e.g. 2026-11-20T13:00:00Z. Empty = latest published hour.

# %%
from datetime import UTC, datetime

from wikipulse import ops
from wikipulse.download import fetch_hour

h = datetime.fromisoformat(hour.replace("Z", "+00:00")) if hour else ops.latest_available_hour(datetime.now(UTC))
result = fetch_hour(h, RAW_ROOT)
print(result)
notebookutils.notebook.exit(result["path"])
