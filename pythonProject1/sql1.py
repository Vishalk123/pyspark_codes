from sys import stdin

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

ls = [[0,"Y","N"],
      [1,"Y","Y"],
      [2,"N","Y"],
      [3,"Y","Y"],
[4,"N","N"]]
# this below id the one way of creating the DF from list using rdd
# rdd = spark.sparkContext.parallelize(ls)
# df = rdd.toDF("order_id int,order_date string,customer_id int,status string")

# 2nd way is to use createDataFrame utility for DF creation by list
schema = "product_id  int,low_fats  string,recyclable  string"
df = spark.createDataFrame(ls,schema)
result_df = df.filter((df.low_fats == 'Y') & (df.recyclable == 'Y')).select("product_id")

# Show the results
result_df.show()

df.printSchema()
stdin.readline()
spark.stop()