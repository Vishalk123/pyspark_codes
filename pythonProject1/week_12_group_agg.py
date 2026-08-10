from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.functions import count, sum, avg, countDistinct
from sys import stdin

from pyspark.sql.types import IntegerType, StructType, StructField, StringType, TimestampType, DateType

# from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType
my_conf = SparkConf()
my_conf.set("spark.app.name","spark_session_code")
my_conf.set("spark.master","local[2]")
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   give jars in code
my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")

spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


df = spark.read \
    .format("csv") \
    .option("inferschema",True) \
    .load("/Users/VISHAL/share/Spark_week_12/windowdata.csv")

# win = Window.partitionBy("Country") \
#             .orderBy("Week")
res = df.toDF("country","weeknum","quantity","price","invoice")
win = Window.partitionBy("country") \
            .orderBy("weeknum") \
            .rowsBetween(Window.unboundedPreceding,Window.currentRow)
final_res = res.withColumn("Running_total",sum("invoice").over(win))
final_res.show()
# stdin.readline()
spark.stop()
