from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Register as a temporary SQL view
posts_df.createOrReplaceTempView("posts")

# Use Spark SQL to split hashtags, explode into rows, count and sort descending
hashtag_counts = spark.sql("""
    SELECT
        TRIM(hashtag) AS Hashtag,
        COUNT(*)      AS Count
    FROM (
        SELECT EXPLODE(SPLIT(Hashtags, ',')) AS hashtag
        FROM posts
        WHERE Hashtags IS NOT NULL
    ) exploded
    GROUP BY TRIM(hashtag)
    ORDER BY Count DESC
""")

hashtag_counts.show(truncate=False)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)

print("Task 1 complete: hashtag_trends.csv written to outputs/")
spark.stop()
