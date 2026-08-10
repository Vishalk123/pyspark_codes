from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import split
from sys import stdin
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, TimestampType

# from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType
my_conf = SparkConf()
my_conf.set("spark.app.name","spark_session_code")
my_conf.set("spark.master","local[2]")
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")
my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")

spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()


schema_1 = "order_id int,order_date Timestamp,order_customer_id Int,order_status String"
schema_2 = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("Timestamp",StringType(),True),
    StructField("order_customer_id", IntegerType(), True),
    StructField("order_status",StringType(),True)
])
ls1 = [("1",),("2",),("3",),("4",)]
ls2 = [("1","vishal"), ("2","akash"), ("2","gungun"), ("4","puneet"), ("4","rakesh"), ("4","seema"), ("4","neha")]
schema = StructType([
    StructField("id", StringType(), True)
])
df1 = spark.createDataFrame(ls1,schema)
df2 = spark.createDataFrame(ls2,["id","status"])
df2.show()
df1.show()
joindf = df1.join(df2,"id","left")
joindf.show()
res = joindf.groupBy("id") \
            .agg(count("id").alias("cnt")) \
            .select("cnt")
res.show()

spark.stop()
