from sys import stdin

import null

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, sum

from pyspark.sql.functions import col, expr,concat, lit,when

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

data = [
    (1, "Will", None),
    (2, "Jane", None),
    (3, "Alex", 2),
    (4, "Bill", None),
    (5, "Zack", 1),
    (6, "Mark", 2)
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("referee_id", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# df1 = df.filter((df.referee_id !=2) | (df.referee_id.isNull())).select(df.name)
df1 = df.filter((col("referee_id") !=2) | (col("referee_id").isNull())) \
    .select(df.name)

# Show the results
df1.show()

df.printSchema()
stdin.readline()
spark.stop()