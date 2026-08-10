from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
from pyspark.sql.functions import col, expr
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


orderdf = spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferschema",True) \
    .load("/Users/VISHAL/share/Spark_week_12/orders.csv")

customerdf = spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferschema",True) \
    .load("/Users/VISHAL/share/Spark_week_12/customers.csv")

join_condition = orderdf["order_customer_id"] == customerdf["customer_id"]
join_type = "outer"
# using coalesce for handling null for null order_id i will print -1
joined_df = orderdf.join(customerdf,join_condition,join_type)
# joined_df.show()
# stdin.readline()
spark.stop()
