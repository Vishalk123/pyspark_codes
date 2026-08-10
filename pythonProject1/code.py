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
my_conf.set("spark.master", "local[*]")
my_conf.set("spark.driver.memory", "6g")
# my_conf.set("spark.memory.offHeap.enabled", "true")
my_conf.set("spark.memory.fraction", "0.7")
my_conf.set("spark.memory.storageFraction", "0.3")

# Initialize Spark session
spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Logger setup
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)


def maxi(df):
    df.createOrReplaceTempView("tbl")
    res = spark.sql(""" select *,max(weight) over(partition by id, month) as maxi , 
    avg(weight) over(partition by id, month) as avg from tbl order by id , month
    """)
    res = res.filter(f.col("maxi") == f.col("weight"))
    return res
df = spark.read.csv("/Users/VISHAL/Downloads/test.csv",header=True)
df1 = df.withColumn('date' , f.to_date(f.col('date'),"yyyyMMdd"))
df2 = df1.withColumn("month",f.month('date'))
df3 = df2.groupby(f.col("id"),f.col("month")).agg(f.avg("weight").alias("avg_score"),f.max(f.col("weight")).alias("max_score"))
res = df3.join(df1,(df3["id"] == df1["id"]) & (df3["max_score"] == df1["weight"]),"inner")
# df3.createOrReplaceTempView("tbl1")
# df2.createOrReplaceTempView("tbl2")
#
# res = spark.sql(""" select a.id , a.month ,a.avg_score,a.max_score , b.date from tbl1 a join tbl2 b on
#  a.id = b.id and
#  a.month = b.month and
#  a.max_score = b.weight
# """)
# print(22/7)
res = maxi(df2)

res.show(100,truncate=False)

