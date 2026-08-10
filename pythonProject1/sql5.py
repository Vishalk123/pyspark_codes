from sys import stdin

import null

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, sum, length

from pyspark.sql.functions import col, expr, concat, lit, when

# Configuration for Spark
my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[2]")
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   # Optional: give jars in code
# my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")

# Initialize Spark session
spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Logger setup
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

data = [(1, "Let us Code"),
        (2, "More than fifteen chars are here!")]

schema = "tweet_id int, content string"

df = spark.createDataFrame(data, schema)

df1 = df.withColumn("length_column", length(df.content))
df2 = df1.filter(df1.length_column >= 15).select("tweet_id")
df2.show()
df1.printSchema()
stdin.readline()
spark.stop()