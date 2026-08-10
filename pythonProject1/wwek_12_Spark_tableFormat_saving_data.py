from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import split
from sys import stdin

from pyspark.sql.types import IntegerType, StructType, StructField, StringType, TimestampType

# from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType
jars = "/Users/VISHAL/share/spark-hive_2.11-2.4.4.jar,/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar"
my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[2]")
my_conf.set("spark.jars", jars)  #  give jars in code
my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")
my_conf.set("spark.executor.temporaryFilesCleanupInterval", "30s")

spark = SparkSession.builder \
    .config(conf=my_conf) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")
schema_1 = "order_id int,order_date Timestamp,order_customer_id Int,order_status String"
schema_2 = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("order_customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)
])

file_path = "/Users/VISHAL/share/Spark_week_11/orders.csv"
df1 = spark.read.option("header", "True").schema(schema_2).csv(file_path)

#  if we want to create a database to store table inside it then create a db using spark sql

spark.sql("create database if not exists vishal_db")
# df1.write.mode("Overwrite").saveAsTable("vishal_db.Table_format_data_in_parquet")
# df1.write.mode("Overwrite").format("avro").saveAsTable("vishal_db.Table_format_data_in_avro")
# df1.write.mode("Overwrite").format("csv").saveAsTable("vishal_db.Table_format_data_in_csv")
# df1.write.mode("Overwrite").format("csv").bucketBy(4,"order_customer_id").sortBy("order_customer_id").saveAsTable("vishal_db.Table_format_data_in_csv_bucket")
print(spark.catalog.listTables("vishal_db"))

spark.stop()
