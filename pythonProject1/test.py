points_per_user_course = user_per_course_totchap_count_totChapCourse.withColumn(
    "ratio",F.col("total_chapters") / F.col("total_chapters_per_course")).withColumn("points_per_user_course",
    F.when(F.col("ratio") >= 0.9, 10)
    .when((F.col("ratio") >= 0.5) & (F.col("ratio") < 0.9), 4)
    .when((F.col("ratio") >= 0.25) & (F.col("ratio") < 0.5), 2)
    .otherwise(0)
).drop("ratio")  # Optional: Drop the intermediate 'ratio' column