from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).csv("input/users.csv")

# Register as temporary SQL views
posts_df.createOrReplaceTempView("posts")
users_df.createOrReplaceTempView("users")

# Use Spark SQL to find top verified users ranked by total engagement (Likes + Retweets)
top_verified_users = spark.sql("""
    SELECT
        u.UserID,
        u.Username,
        u.Country,
        u.AgeGroup,
        COUNT(p.PostID)                                          AS TotalPosts,
        SUM(CAST(p.Likes     AS LONG))                          AS TotalLikes,
        SUM(CAST(p.Retweets  AS LONG))                          AS TotalRetweets,
        SUM(CAST(p.Likes AS LONG) + CAST(p.Retweets AS LONG))  AS TotalEngagement,
        ROUND(AVG(CAST(p.Likes AS DOUBLE) + CAST(p.Retweets AS DOUBLE)), 2) AS AvgEngagementPerPost
    FROM posts p
    JOIN users u ON p.UserID = u.UserID
    WHERE LOWER(u.Verified) = 'true'
    GROUP BY u.UserID, u.Username, u.Country, u.AgeGroup
    ORDER BY TotalEngagement DESC
    LIMIT 10
""")

top_verified_users.show(truncate=False)

# Save result
top_verified_users.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)

print("Task 4 complete: top_verified_users.csv written to outputs/")
spark.stop()
