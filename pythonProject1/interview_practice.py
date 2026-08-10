from sys import stdin
import random
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql.functions import *
from pyspark.sql.functions import col, expr, concat, lit, when, to_date, sum, concat_ws , collect_list,count,round

# Configuration for Spark
my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[3]")
my_conf.set("spark.executor.instances", "1")  # Number of executors
my_conf.set("spark.executor.memory", "8g")    # Memory per executor
my_conf.set("spark.executor.cores", "5")      # Cores per executor
my_conf.set("spark.dynamicAllocation.enabled",True)
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   # Optional: give jars in code
my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")

# Initialize Spark session
spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Logger setup
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

# Sample data
signups_data = [(1,), (2,), (3,)]
signups_schema = ["user_id"]

# Sample data for Confirmations
confirmations_data = [(1, 'confirmed'), (1, 'not confirmed'), (2, 'confirmed')]
confirmations_schema = ["user_id", "action"]

# Create DataFrames
signups_df = spark.createDataFrame(signups_data, signups_schema)
confirmations_df = spark.createDataFrame(confirmations_data, confirmations_schema)

# Perform the left join
joined_df = signups_df.join(confirmations_df, "user_id", "left")

# Calculate the confirmation rate
confirmation_rate_df = joined_df.groupBy("user_id") \
    .agg(
    round((sum(when(col("action") == 'confirmed', 1).otherwise(0))/coalesce(count("user_id"), lit(1))),2).alias("confirmation_rate")
    )

# Show the result
confirmation_rate_df.show()


# Stop Spark session
spark.stop()
