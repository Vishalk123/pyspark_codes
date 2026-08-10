from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, md5, concat_ws

spark = SparkSession.builder.getOrCreate()

# ------------------------
# Read today's updates
# ------------------------

updates = (
    spark.read.option("header", "true")
    .csv("s3://bucket/input/updates")
    .withColumn(
        "hash",
        md5(concat_ws("||", "name", "city"))
    )
    .cache()
)

# ------------------------
# Read only current rows
# ------------------------

current = (
    spark.read.parquet("s3://bucket/dim")
    .filter(F.col("is_current") == "true")
)

# ------------------------
# Read only matching keys
# ------------------------

affected = (
    current.join(
        broadcast(updates.select("customer_id")),
        "customer_id"
    )
)

# ------------------------
# Hash current
# ------------------------

affected = affected.withColumn(
    "hash",
    md5(concat_ws("||", "name", "city"))
)

# ------------------------
# Detect changes
# ------------------------

changed = (
    affected.alias("t")
    .join(
        broadcast(updates.alias("s")),
        "customer_id"
    )
    .filter(
        F.col("t.hash") != F.col("s.hash")
    )
)

# ------------------------
# Close old records
# ------------------------

closed = (
    changed.select(
        "customer_id",
        F.col("t.name").alias("name"),
        F.col("t.city").alias("city"),
        F.col("t.start_date"),
        F.current_date().alias("end_date"),
        F.lit("false").alias("is_current")
    )
)

# ------------------------
# New versions
# ------------------------

new_versions = (
    changed.select(
        "customer_id",
        F.col("s.name").alias("name"),
        F.col("s.city").alias("city"),
        F.current_date().alias("start_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.lit("true").alias("is_current")
    )
)

# ------------------------
# New customers
# ------------------------

new_customers = (
    updates.join(
        current.select("customer_id"),
        "customer_id",
        "left_anti"
    )
)

# ------------------------
# Unchanged rows
# ------------------------

unchanged = (
    current.join(
        changed.select("customer_id"),
        "customer_id",
        "left_anti"
    )
)

# ------------------------
# Historical rows
# ------------------------

history = (
    spark.read.parquet("s3://bucket/dim")
    .filter(F.col("is_current") == "false")
)

# ------------------------
# Final dataframe
# ------------------------

final = (
    history
    .unionByName(unchanged)
    .unionByName(closed)
    .unionByName(new_versions)
    .unionByName(new_customers)
)

# ------------------------
# Write
# ------------------------

final.write.mode("overwrite").parquet("s3://bucket/dim")