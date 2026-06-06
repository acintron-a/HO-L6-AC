from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("EngagementByAge").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).csv("input/users.csv")

# Register as temporary SQL views
posts_df.createOrReplaceTempView("posts")
users_df.createOrReplaceTempView("users")

# Use Spark SQL to join posts with users and aggregate engagement by age group
engagement_by_age = spark.sql("""
    SELECT
        u.AgeGroup,
        COUNT(p.PostID)              AS TotalPosts,
        ROUND(AVG(CAST(p.Likes     AS DOUBLE)), 2) AS AvgLikes,
        ROUND(AVG(CAST(p.Retweets  AS DOUBLE)), 2) AS AvgRetweets,
        SUM(CAST(p.Likes           AS LONG))       AS TotalLikes,
        SUM(CAST(p.Retweets        AS LONG))       AS TotalRetweets,
        ROUND(AVG(CAST(p.Likes AS DOUBLE) + CAST(p.Retweets AS DOUBLE)), 2) AS AvgTotalEngagement
    FROM posts p
    JOIN users u ON p.UserID = u.UserID
    GROUP BY u.AgeGroup
    ORDER BY AvgTotalEngagement DESC
""")

engagement_by_age.show(truncate=False)

# Save result
engagement_by_age.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)

print("Task 2 complete: engagement_by_age.csv written to outputs/")
spark.stop()
