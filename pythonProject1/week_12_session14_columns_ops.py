import string

from pyspark.sql.functions import col,udf,column
from pyspark import SparkConf
from sys import stdin
from pyspark.sql import *
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, TimestampType


# from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType
def agecheck(a: int)->string:
    if a > 18:
        return "Y"
    else:
        return "N"


my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[2]")
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   give jars in code
my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")

spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

schema = "Name string, age int , City string"
df = spark.read \
    .format("csv") \
    .schema(schema) \
    .load("/Users/VISHAL/share/Spark_week_12/dataset1")

# # df_with_col_name = df.toDF("Name","age","City")
# Column object
# agefunc = udf(agecheck,StringType())
# rs = df.withColumn("adult", agefunc(col("age")))

spark.udf.register("ageUDF",agecheck)
lst_of_functions =spark.catalog.listFunctions()
fucn_found = [f for f in lst_of_functions if f.name == 'ageUDF']
print(fucn_found)


df.createOrReplaceTempView("tbl")
df2 = spark.sql("select * , ageUDF(age) as adult from tbl")
print("working"+" col.")
df2.show()
df.printSchema()
# df.show()
# stdin.readline()
spark.stop()
