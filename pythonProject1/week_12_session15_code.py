from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
from pyspark.sql.functions import *
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

spark.sparkContext.setLogLevel("INFO")

file_path = "/Users/VISHAL/share/Spark_week_11/orders.csv"
ls = [[1,"2013-07-25",11599,"CLOSED"],
      [2,"2014-07-25",256,"PENDING_PAYMENT"],
      [3,"2013-07-25",11599,"COMPLETE"],
      [4,"2019-07-25",8827,"CLOSED"]]
# this below id the one way of creating the DF from list using rdd
# rdd = spark.sparkContext.parallelize(ls)
# df = rdd.toDF("order_id int,order_date string,customer_id int,status string")

# 2nd way is to use createDataFrame utility for DF creation by list
schema = "order_id int,order_date string,customer_id int,status string"
df = spark.createDataFrame(ls,schema)

window_spec = Window.orderBy("order_id")

df_1 = df.withColumn("order_date",unix_timestamp(col('order_date').cast(DateType()))) \
         .withColumn("new_col_id",row_number().over(window_spec)) \
         .dropDuplicates(["order_date","customer_id"]) \
         .drop("order_id") \
         .sort("order_date")
         # .withColumn("new_col_id",monotonically_increasing_id())
df_1.show()
df_1.printSchema()
stdin.readline()
spark.stop()
