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

spark.sparkContext.setLogLevel("ERROR")

# read data
inp_data = spark.sparkContext.textFile("/Users/VISHAL/share/Spark_week_9/tempdata.csv")
parsed_data = inp_data.map(lambda x: (x.split(",")[0], x.split(",")[2], float(x.split(",")[3])))

# schema for dataframe
schema = StructType([
    StructField("Station_id", StringType(), True),
    StructField("reading_type", StringType(), True),
    StructField("Reading", DoubleType(), True)
])

# Create Dataframe
temp_df = spark.createDataFrame(parsed_data, schema)

# Create Table view for dataframe ops
temp_df.createOrReplaceTempView('temperature')
res = spark.sql("""
            select Station_id,min(Reading) from temperature where reading_type = "TMIN" group by Station_id
""")
cnt = res.select("Station_id").distinct().count()
print("no. of distinct Station id's is {0}".format(cnt))
res.show()
stdin.readline()
spark.stop()
