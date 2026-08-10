from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import split
from sys import stdin

from pyspark.sql.types import IntegerType, StructType, StructField, StringType, TimestampType

# from pyspark.sql.types import StructType, StructField, StringType, IntegerType , TimestampType
my_conf = SparkConf()
my_conf.set("spark.app.name","spark_session_code")
my_conf.set("spark.master","local[2]")
# my_conf.set("spark.jars", "/Users/VISHAL/share/spark-avro_2.11-2.4.4.jar")   give jars in code
my_conf.set("spark.local.dir", "/Users/VISHAL/share/temp_dir")


spark = SparkSession.builder \
    .config(conf=my_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")
schema_1 = "order_id int,order_date Timestamp,order_customer_id Int,order_status String"
schema_2 = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("Timestamp",StringType(),True),
    StructField("order_customer_id", IntegerType(), True),
    StructField("order_status",StringType(),True)
])

file_path = "/Users/VISHAL/share/Spark_week_11/orders.csv"
df1 = spark.read.option("header","True").schema(schema_2).csv(file_path)
df1.printSchema()

df1.createOrReplaceTempView("orders_table")
df2 = spark.sql("select order_status,count(*) as status_count from orders_table group by order_status")
df2.show()
# res_df = df1.repartition(4) \
#     .where("order_customer_id > 10000") \
#     .select("order_id","order_customer_id") \
#     .groupBy("order_customer_id") \
#     .count()
# df2=df1.repartition(5)
#
#
# save data in avro,csv,parquet,json format
# df2.write.format("avro").partitionBy("order_status").mode("Overwrite").save("/Users/VISHAL/share/Spark_week_12/output")
#
# res_out = res_df.filter(res_df['count'] <= 6)
# res_df.printSchema()
# res_out.show(truncate=False)
stdin.readline()
spark.stop()
