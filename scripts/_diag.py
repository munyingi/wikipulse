from pyspark.sql import Row, functions as F
from wikipulse.spark import local_spark
s = local_spark()
print("EXT", s.conf.get("spark.sql.extensions", None), "CAT", s.conf.get("spark.sql.catalog.spark_catalog", None))
s.sql("CREATE SCHEMA IF NOT EXISTS b")
df = s.createDataFrame([Row(h=1, d="x", v=1), Row(h=2, d="x", v=2)])
df.write.format("delta").mode("overwrite").partitionBy("d").saveAsTable("b.t")
s.sql("DESCRIBE DETAIL b.t").select("format").show()
for name, fn in [
    ("writeTo.overwrite", lambda: s.createDataFrame([Row(h=1, d="x", v=9)]).writeTo("b.t").overwrite(F.expr("h = 1"))),
    ("saveAsTable replaceWhere", lambda: s.createDataFrame([Row(h=1, d="x", v=8)]).write.format("delta").mode("overwrite").option("replaceWhere", "h = 1").saveAsTable("b.t")),
    ("path replaceWhere", lambda: s.createDataFrame([Row(h=1, d="x", v=7)]).write.format("delta").mode("overwrite").option("replaceWhere", "h = 1").save(s.sql("DESCRIBE DETAIL b.t").first().location)),
    ("sql replace where", lambda: (s.createDataFrame([Row(h=1, d="x", v=6)]).createOrReplaceTempView("src"), s.sql("INSERT INTO b.t REPLACE WHERE h = 1 SELECT * FROM src"))),
]:
    try:
        fn(); print("OK", name, sorted(s.table("b.t").collect()))
    except Exception as e:
        print("FAIL", name, str(e).splitlines()[0][:200])
