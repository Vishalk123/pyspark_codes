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
    .option("header",True) \
    .option("inferschema",True) \
    .load("/Users/VISHAL/share/Spark_week_12/order_data.csv")

# using column expression
res1 = df.select(
            count("*").alias("tot_num_rows"),
            sum("Quantity").alias("tot_quantity"),
            avg("UnitPrice").alias("avg_unit_price"),
            countDistinct("InvoiceNo").alias("no_unique_invoices")
)

#  Using Select expression

res2 = df.selectExpr(
            "count(*) as tot_num_rows",
            "sum(Quantity) as tot_quantity",
            "avg(UnitPrice) as avg_unit_price",
            "count(Distinct(InvoiceNo)) as no_unique_invoices"
)

#  Using Spark Sql

df.createOrReplaceTempView("tbl")
res3 = spark.sql("""select count(*) as tot_num_rows,sum(Quantity),avg(UnitPrice),count(Distinct(InvoiceNo))
 as no_unique_invoices from tbl""")
res1.show()
res2.show()
res3.show()
# stdin.readline()
spark.stop()
