from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import logging

logging.basicConfig(level=logging.DEBUG)
my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder \
    .appName("spark_session_code") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schemaDDL = "order_id int, order_date string,order_customer_id int, order_status string"

schemaStruct = StructType([
    StructField("country",StringType,True),
    StructField("weeknum", IntegerType, True),
    StructField("numinvoices", IntegerType, True),
    StructField("totalquantity", IntegerType, True),
    StructField("invoicevalue", DoubleType(), True)

])

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .schema(schemaStruct) \
    .option("path", "/Users/VISHAL/share/Spark_week_11/orders.csv") \
    .load()

# df.printSchema
spark.stop()
