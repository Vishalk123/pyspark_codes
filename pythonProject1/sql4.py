from sys import stdin

import null

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, sum

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

data = [(1, 3, 5, "2019-08-01"),
        (1, 3, 6, "2019-08-02"),
        (2, 7, 7, "2019-08-01"),
        (2, 7, 6, "2019-08-02"),
        (4, 7, 1, "2019-07-22"),
        (3, 4, 4, "2019-07-21"),
        (3, 4, 4, "2019-07-21")]
schema = "article_id int, author_id int, viewer_id int, view_date string"

df = spark.createDataFrame(data, schema)
df1 = df.filter(df.author_id == df.viewer_id).select("author_id").distinct()
df2 = df1.orderBy(df1.author_id)

df1.show()
df.printSchema()
stdin.readline()
spark.stop()
