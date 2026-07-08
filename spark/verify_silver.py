"""Read the silver Parquet for one hour back and print reconciliation stats:
total rows, distinct event_ids (must equal total → dedupe worked), and the
per-event-type breakdown."""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

date, hour = sys.argv[1], int(sys.argv[2])
silver = os.environ["SILVER_BUCKET"]

spark = (
    SparkSession.builder.appName("verify-silver")
    .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
    .getOrCreate()
)

df = spark.read.parquet(f"gs://{silver}/events").where(
    (col("event_date") == date) & (col("event_hour") == hour)
)

total = df.count()
distinct_ids = df.select("event_id").distinct().count()
print(f"SILVER_TOTAL {total}")
print(f"DISTINCT_EVENT_IDS {distinct_ids}  (== total means no dupes remain)")
df.groupBy("event_type").count().orderBy(col("count").desc()).show(30, False)

spark.stop()
