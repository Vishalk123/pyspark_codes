from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
import logging
from pyspark.sql import  functions as f
from pyspark.sql.functions import col, expr
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

spark.sparkContext.setLogLevel("ERROR")

# Logger setup
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

# Creating RDDs for unstructured data
try:
    movieRdd = spark.sparkContext.textFile("/Users/VISHAL/share/Spark_week_10/movies.dat")
    ratingRdd = spark.sparkContext.textFile("/Users/VISHAL/share/Spark_week_10/ratings.dat")
except Exception as e:
    logger.error("Error reading input files", exc_info=True)
    spark.stop()
    raise e

# Parsing the required columns
try:
    parsedMovie = movieRdd.map(lambda x: (int(x.split("::")[0]), x.split("::")[1]))
    parsedRating = ratingRdd.map(lambda x: (int(x.split("::")[1]), int(x.split("::")[2])))
except Exception as e:
    logger.error("Error parsing RDDs", exc_info=True)
    spark.stop()
    raise e

# Defining schema using StructType
schema_movies = StructType([
    StructField("movie_id_m", IntegerType(), True),
    StructField("movie_name", StringType(), True)
])

schema_ratings = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True)
])

# Convert to DataFrame
try:
    movieDf = spark.createDataFrame(parsedMovie, schema=schema_movies)
    ratingDf = spark.createDataFrame(parsedRating, schema=schema_ratings)
except Exception as e:
    logger.error("Error creating DataFrames", exc_info=True)
    spark.stop()
    raise e

movie_tot_rating_countOfrating = ratingDf.groupby("movie_id") \
                                        .agg(
                                            f.sum("rating").alias("total_rating"),
                                            f.count("rating").alias("no_of_users")
                                            )

avg_rating = movie_tot_rating_countOfrating.withColumn("avg_rating", \
                                          f.round((col("total_rating")/col("no_of_users")).cast("double"),3))
res = avg_rating.filter("avg_rating >= 3.5 and no_of_users >=1000") \
                .drop("total_rating") \
                .drop("no_of_users")

cond = res["movie_id"] == movieDf["movie_id_m"]
final_res = res.join(movieDf,cond,"inner")
result = final_res.select("movie_id","movie_name","avg_rating").orderBy(col("avg_rating").desc())
# Show the DataFrames
try:
    result.show(truncate=False)
except Exception as e:
    logger.error("Error showing DataFrames", exc_info=True)
    spark.stop()
    raise e

# Stop the Spark session
spark.stop()
