from itertools import count
from sys import stdin

from pyspark.sql.functions import count, countDistinct

from pyspark.sql.functions import desc
import null

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, sum, length

from pyspark.sql.functions import col, expr, concat, lit, when

# Configuration for Spark
my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[2]")
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   # Optional: give jars in code
# my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")

# Initialize Spark session
spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Logger setup
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

df = spark.read.option("header", False).csv("C:/Users/VISHAL/share/Spark_week_10/assignment/View/*")
df = df.toDF("userId", "chapterId", "dateAndTime")

df1 = spark.read.option("header", False).csv("C:/Users/VISHAL/share/Spark_week_10/assignment/Chapter/*")
df1 = df1.toDF("chapterId", "course_id")

df3 = df1.join(df, on='chapterId', how="inner")

chapters_per_course = df1.groupBy("course_id").count().withColumnRenamed("count", "chapters")

df4 = df3.select("userId", "course_id", "chapterId").distinct() \
    .groupBy("userId", "course_id") \
    .agg(countDistinct("chapterId").alias("chapters_completed"))

df5 = df4.join(chapters_per_course, on="course_id", how="inner")
df6 = df5.withColumn("scoredata", col("chapters_completed") / col("chapters"))
df7 = df6.withColumn("score", when(col("scoredata") >= 0.9, lit(10).cast("int"))
                     .when((col("scoredata") >= 0.5) & (col("scoredata") < 0.9), lit(4).cast("int"))
                     .when((col("scoredata") >= 0.25) & (col("scoredata") < 0.5), lit(2).cast("int"))
                     .otherwise(lit(0).cast("int")))

df8 = df7.groupby("course_id").agg(sum("score").alias("total_score")).orderBy(desc("total_score"))
df8.show()

df.printSchema()
stdin.readline()
spark.stop()
