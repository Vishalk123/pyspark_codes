from sys import stdin
import random
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import functions as f
from pyspark.sql.functions import col, expr, concat, lit, when

# Configuration for Spark
my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[3]")
my_conf.set("spark.executor.instances", "1")  # Number of executors
my_conf.set("spark.executor.memory", "8g")    # Memory per executor
my_conf.set("spark.executor.cores", "5")      # Cores per executor
my_conf.set("spark.dynamicAllocation.enabled",True)

# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   # Optional: give jars in code
# my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")

def func(x: str) -> str:
    res = x + str(random.randint(1, 100))
    return res


# Initialize Spark session
spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Logger setup
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)
try:
    orderdf = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferschema",True) \
        .load("/Users/VISHAL/share/Spark_week_12/orders.csv")
    # eligibility_df = spark.read.option("header", True).csv("/Users/VISHAL/share/Spark_week_12/eligibility.csv")
except Exception as e:
    logger.error("Error reading input files", exc_info=True)
    spark.stop()
    raise e



# orderdf.createOrReplaceTempView("tbl")
# va = spark.sql("""
#     select
#         order_customer_id,
#         date_format(order_date, 'MMMM') as Orderdt,
#         count(1) as cnt,
#         first(date_format(order_date, 'M')) as monthnum
#     from tbl
#     group by order_customer_id, Orderdt
#     order by cast(monthnum as int)
# """).explain


# Stop the Spark session
# stdin.readline()
spark.stop()
