from pyspark.sql import SparkSession
from pyspark import SparkConf
from sys import stdin
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

my_conf = SparkConf()
my_conf.set("spark.app.name", "spark_session_code")
my_conf.set("spark.master", "local[2]")
my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   give jars in code

spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# read data


# schema for dataframe

# Create Dataframe

# stdin.readline()
spark.stop()
