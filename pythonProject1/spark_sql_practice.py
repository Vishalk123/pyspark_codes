from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, sum

from pyspark.sql.functions import col, expr,concat, lit,when

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
schema = "name string"
try:
    df = spark.read.option("header", False).format("csv").schema(schema).load("/Users/VISHAL/share/Spark_week_14/employee_table.txt")
    # eligibility_df = spark.read.option("header", True).csv("/Users/VISHAL/share/Spark_week_12/eligibility.csv")
except Exception as e:
    logger.error("Error reading input files", exc_info=True)
    spark.stop()
    raise e

df.createOrReplaceTempView("tbl")
query = """ select t1.name,t2.name from (select *,row_number() over() as rn from tbl) where t1.
"""
# new_medicaldf = medical_df.withColumnRenamed("memberid","memid")
# typ = "inner"
# temp = new_medicaldf.join(eligibility_df,new_medicaldf['memid'] == eligibility_df.memberid,typ)
# temp.createOrReplaceTempView('tbl')
# res = spark.sql("select * from tbl") ;
# rs = spark.sql("""
#                 select *,
#                 case when fullName is not NULL then NULL else fullName end as fullName
#                 from tbl
# """)
# res = rs.withColumn("fullName",concat(col("firstName"),lit(" "),col("lastName")))
# fn_res = res.selectExpr('sum(paidAmount) as total')
# df3 = res.withColumn('paidAmount',when(res.paidAmount >200 , "Huge") \
#                      .otherwise(when(res.paidAmount <= 100,"little").otherwise("middle")))
# res.show()
# Stop the Spark session
spark.stop()
