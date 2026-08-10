from pyspark import SparkContext
from sys import stdin


def func(x):
    val1 = x[1][0]
    val2 = x[1][1]
    res = val1 / val2
    if res >= 0.9:
        return (x[0], 10)
    elif res >= 0.5 and res < 0.9:
        return (x[0], 4)
    elif res >= 0.25 and res < 0.5:
        return (x[0], 2)
    else:
        return (x[0], 0)

# same as week10_assignment_question2 till line comment    "end of copy code"
sc = SparkContext("local[*]", "week_10")
view_logs = sc.textFile("/Users/VISHAL/share/Spark_week_10/assignment/View/*")
rdd1 = view_logs.map(lambda x: (int(x.split(",")[1]), int(x.split(",")[0])))

# get distinct user logs
Distinct_view_logs = rdd1.distinct()
rdd2 = sc.textFile("/Users/VISHAL/share/Spark_week_10/assignment/chapters.csv")
chap_course = rdd2.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1])))

# join on chapter for getting user and course id together implies user completed a chapter of this course
merged_chap_course_user = Distinct_view_logs.join(chap_course)

# remove chapter and reformat rdd to ((user,course),1)
user_course = merged_chap_course_user.map(lambda x: ((x[1][0], x[1][1]), 1))

# count the no. of chapters completed by same user for a specific course
user_per_course = user_course.reduceByKey(lambda x, y: x + y)

# reformat the rdd as (course,count)
user_per_course_totchap_count = user_per_course.map(lambda x: (x[0][1], x[1]))
rdd_totChap_course = sc.textFile("/Users/VISHAL/share/Spark_week_10/assignment/answer1/*")
rdd_totChap_course_parsed = rdd_totChap_course.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1])))

# join rdd to total chapter count so that rdd become (course,(count,totCount))
user_per_course_totchap_count_totChapCourse = user_per_course_totchap_count.join(rdd_totChap_course_parsed)

# func(x) to give points for every user for every course
points_per_user_course = user_per_course_totchap_count_totChapCourse.map(lambda x : func(x))

# total point calculate
tot_points_per_course = points_per_user_course.reduceByKey(lambda x,y : x+y)

# end of copy code from question 2

# input title file
titles_input = sc.textFile("/Users/VISHAL/share/Spark_week_10/assignment/titles.csv")
title = titles_input.map(lambda x : (int(x.split(',')[0]),x.split(',')[1]))

joined_data = tot_points_per_course.join(title)

# map in the titles with respective course ids for rdd of type (course,(totPoints,title)) then map rdd to (totPoints,titles)
joined_course_title = joined_data.map(lambda x : (x[1][0],x[1][1]))
# sort data in desc for getting most popular course first
sorted_data = joined_course_title.sortByKey(False).cache()

res = sorted_data.collect()
print(res)
cnt = sorted_data.count()
print(cnt)
print("End")
# res.saveAsTextFile("/Users/VISHAL/share/Spark_week_10/assignment/answer2")
stdin.readline()
