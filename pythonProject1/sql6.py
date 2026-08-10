from sys import stdin

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

employees_data = [(1, "Alice"),
                  (7, "Bob"),
                  (11, "Meir"),
                  (90, "Winston"),
                  (3, "Jonathan")]

# Schema for Employees table
employees_schema = "id int, name string"
df = spark.createDataFrame(employees_data,employees_schema)

employee_uni_data = [(3, 1),
    (11, 2),
    (90, 3)]
# Schema for EmployeeUNI table
employee_uni_schema = "id int, unique_id int"

df1 = spark.createDataFrame(employee_uni_data,employee_uni_schema)

joinfd3 =  df.join(df1, on='id', how='left')

df1.printSchema()
stdin.readline()
spark.stop()

