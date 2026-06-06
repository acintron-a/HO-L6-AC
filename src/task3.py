from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentEngagement").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Register as a temporary SQL view
posts_df.createOrReplaceTempView("posts")

# Use Spark SQL to categorize sentiment and measure average engagement per category.
# SentimentScore ranges from -1.0 (very negative) to 1.0 (very positive):
#   Positive : SentimentScore >= 0.1
#   Neutral  : -0.1 < SentimentScore < 0.1
#   Negative : SentimentScore <= -0.1
sentiment_engagement = spark.sql("""
    SELECT
        CASE
            WHEN CAST(SentimentScore AS DOUBLE) >=  0.1 THEN 'Positive'
            WHEN CAST(SentimentScore AS DOUBLE) <= -0.1 THEN 'Negative'
            ELSE 'Neutral'
        END                                                      AS SentimentCategory,
        COUNT(PostID)                                            AS TotalPosts,
        ROUND(AVG(CAST(SentimentScore AS DOUBLE)), 4)            AS AvgSentimentScore,
        ROUND(AVG(CAST(Likes         AS DOUBLE)), 2)             AS AvgLikes,
        ROUND(AVG(CAST(Retweets      AS DOUBLE)), 2)             AS AvgRetweets,
        ROUND(AVG(CAST(Likes AS DOUBLE) + CAST(Retweets AS DOUBLE)), 2) AS AvgTotalEngagement
    FROM posts
    WHERE SentimentScore IS NOT NULL
    GROUP BY SentimentCategory
    ORDER BY AvgSentimentScore DESC
""")

sentiment_engagement.show(truncate=False)

# Save result
sentiment_engagement.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)

print("Task 3 complete: sentiment_engagement.csv written to outputs/")
spark.stop()
