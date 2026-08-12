from sys import stdin
import random
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import functions as f
from pyspark.sql.functions import col, expr, concat, lit, when

# Configuration for Spark test for commit this time for fifth one
my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[*]")
my_conf.set("spark.driver.memory", "6g")
# my_conf.set("spark.memory.offHeap.enabled", "true")
my_conf.set("spark.memory.fraction", "0.7")
my_conf.set("spark.memory.storageFraction", "0.3")
# my_conf.set("spark.memory.offHeap.size", "0.5g")
# or higher if needed


# Initialize Spark session
spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df1= spark.read.csv("/Users/VISHAL/share/data/CLO/CLO_dec_file*_v2_8Feb26.csv",header=True)
df2= spark.read.csv("/Users/VISHAL/share/data/CLO/CLO_increase_file_v2_8Feb26.csv",header=True)
# stdin.readline()
ls1 = df1.columns
ls2 = df2.columns

res = []
for a in ls1:
    if a in ls2:
        res.append(a)

print(len(res))
print(res)

spark.stop()

