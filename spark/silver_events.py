import os, sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date, hour
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType
)

SCHEMA = StructType([
    StructField("id",         StringType()),    # event id is a numeric *string*
    StructField("type",       StringType()),
    StructField("public",     BooleanType()),
    StructField("created_at", StringType()),    # ISO8601 string → we cast it later
    StructField("actor", StructType([
        StructField("id",    LongType()),
        StructField("login", StringType()),
    ])),
    StructField("repo", StructType([
        StructField("id",   LongType()),
        StructField("name", StringType()),
    ])),
])

def transform_events(raw: DataFrame) -> DataFrame:
    return (
        raw
        .select(
            col("id").alias("event_id"),
            col("type").alias("event_type"),
            col("actor.id").alias("actor_id"),        # dot-path flattens the struct
            col("actor.login").alias("actor_login"),
            col("repo.id").alias("repo_id"),
            col("repo.name").alias("repo_name"),
            col("public"),
            to_timestamp("created_at").alias("created_at"),   # ← the type cast
        )
        .withColumn("event_date", to_date("created_at"))       # partition col
        .withColumn("event_hour", hour("created_at"))          # partition col (0–23)
        .dropDuplicates(["event_id"])                          # ← the dedupe
    )

def run(spark, date: str, hour: int) -> None:
    bronze = os.environ["BRONZE_BUCKET"]
    silver = os.environ["SILVER_BUCKET"]

    bronze_path = f"gs://{bronze}/date={date}/hour={hour:02d}/*.json.gz"
    silver_root = f"gs://{silver}/events"          # partitions created *under* this

    raw    = spark.read.schema(SCHEMA).json(bronze_path)   # schema, not inferSchema
    events = transform_events(raw)

    (events.write
        .partitionBy("event_date", "event_hour")
        .mode("overwrite")
        .parquet(silver_root))
    
if __name__ == "__main__":
    # NB: don't name this `hour` — that would shadow the imported `hour()` fn.
    date, hour_arg = sys.argv[1], int(sys.argv[2])
    spark = (
        SparkSession.builder.appName("silver-events")
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    run(spark, date, hour_arg)
    spark.stop()