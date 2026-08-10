from pyspark import SparkContext
from sys import stdin


def parse_data(x):
    data = x.split("::")
    return (int(data[1]),int(data[2]))


# same as week10_assignment_question2 till line comment    "end of copy code"
sc = SparkContext("local[*]", "week_10")
# input title file
movies = sc.textFile("/Users/VISHAL/share/Spark_week_11/movies.dat").map(lambda x: (int(x.split("::")[0]), x.split("::")[1]))

rating_data = sc.textFile("/Users/VISHAL/share/Spark_week_11/ratings.dat").map(lambda x:(int(x.split("::")[1]),(int(x.split("::")[2]),1)))
# mapped_data = rating_data.map(lambda x : parse_data(x))
total_rating = rating_data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

avg_rating = total_rating.mapValues(lambda x: (float(x[0])/x[1],x[1])).filter(lambda x: x[1][1]>=1000)

# reformat_data = avg_rating.map(lambda x: (x[0],x[1][0]))
temp = avg_rating.count()
print("temp [0]",format(temp))
joined_data = avg_rating.join(movies).map(lambda x : (x[1][1],x[1][0][0]))
res_data =  joined_data.filter(lambda x : x[1]>=3.5)
res = res_data.collect()
for item in res:
    print(item)

print("End")
# res.saveAsTextFile("/Users/VISHAL/share/Spark_week_10/assignment/answer2")
# stdin.readline()

