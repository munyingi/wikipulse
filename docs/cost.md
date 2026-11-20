# Cost model

Fabric bills capacity units (CU). For Spark, **1 CU = 2 Spark vCores**. Each hourly run records its duration and the vCores of its session, so:

```
CU-seconds = vCores / 2 x seconds
EUR        = CU-seconds / 3600 x price per CU hour
```

The price per CU hour is set in `wikipulse.ops.DEFAULT_EUR_PER_CU_HOUR` (default 0.18, an approximate pay-as-you-go list price; set your region's price from the Azure Fabric pricing page). `nb_maintenance` rolls the run log up into `ops.cost_daily`, and the warehouse mart `mart_pipeline_health` shows cost next to reliability.

Example: an hourly run on the starter pool (8 vCores) that takes 3 minutes uses 4 CU x 180 s = 720 CU-seconds, about EUR 0.036. Twenty-four runs a day is about EUR 26 a month for the batch path.

Ways the project keeps cost down:

* Gold daily tables are recomputed per day, not per table history.
* Raw lines are kept only for malformed rows; valid rows are stored typed.
* OPTIMIZE and VACUUM run once a day, not after every write.
* The Eventhouse keeps 7 days hot and 30 days in total.
* The live producer runs 55 minutes per hour and can be paused.

On the trial capacity the money cost is zero; the numbers show what the same workload would cost on a paid capacity, which is the conversation a team lead will want to have.
