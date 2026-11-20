# %%
from wikipulse import pipeline
from wikipulse.delta_io import maintain

for table, zorder in [
    (pipeline.T_BRONZE, None),
    (pipeline.T_SILVER, ["wiki"]),
    (pipeline.T_DAILY, ["wiki"]),
    (pipeline.T_HOURLY, None),
    (pipeline.T_TOP, None),
    (pipeline.T_TRENDING, None),
]:
    if spark.catalog.tableExists(table):
        maintain(spark, table, zorder_by=zorder)
        print(f"maintained {table}")

# %%
# Daily cost roll-up from the run log: estimated capacity units and euros per pipeline and day.
spark.sql(f"""
CREATE OR REPLACE TABLE ops.cost_daily AS
SELECT date(finished_at) AS day, pipeline, count(*) AS runs, sum(seconds) AS seconds,
       sum(cu_seconds) AS cu_seconds, round(sum(eur_estimate), 4) AS eur_estimate
FROM {pipeline.T_RUNS}
GROUP BY 1, 2
""")
display(spark.table("ops.cost_daily").orderBy("day", ascending=False).limit(30))
