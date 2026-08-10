from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
from pyspark.sql.functions import col, expr
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

spark.sparkContext.setLogLevel("INFO")


orderdf = spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferschema",True) \
    .load("/Users/VISHAL/share/Spark_week_12/orders.csv")

customerdf = spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferschema",True) \
    .load("/Users/VISHAL/share/Spark_week_12/customers.csv")

# if two columns name are same and we do join then it creates to col of same name in result df
# now both has same name col for join as i have changed col name in file
# need to change col name using withColumnRenamed or we can drop the col after join
new_orderdf = orderdf.withColumnRenamed("order_customer_id","cus_id")
join_condition = new_orderdf["cus_id"] == customerdf["customer_id"]
join_type = "outer"
# using coalesce for handling null for null order_id i will print -1
joined_df = new_orderdf.join(customerdf,join_condition,join_type) \
                        .drop(new_orderdf["cus_id"]).sort("order_id") \
                        .withColumn("order_id",expr("coalesce(order_id,-1)"))
joined_df.repartition(10)
joined_df.show()
print(joined_df.explain)
# print(orderdf.columns)
# print(customerdf.columns)
stdin.readline()
spark.stop()
