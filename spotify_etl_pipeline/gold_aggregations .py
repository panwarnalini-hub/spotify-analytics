# Databricks notebook source
# DBTITLE 1,Gold Layer Automated
# Databricks Gold Layer Automation

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, dayofweek, when, count

# Initialize Spark
spark = SparkSession.builder.appName("SpotifyGoldAggregations").getOrCreate()

# Read from Silver (cleaned data and deduplicated)
silver_df = spark.read.table("workspace.spotify_silver.cleaned_tracks")

# Top Artists by Play Count
gold_top_artists = silver_df \
    .groupBy("artist").count() \
    .orderBy(col("count").desc())

display(gold_top_artists)
gold_top_artists.write.format("delta").mode("append").saveAsTable("gold_top_artists")

# Listening Trends Over Time
from pyspark.sql.functions import to_date

gold_daily_trend = silver_df \
    .withColumn("date", to_date("played_at")) \
    .groupBy("date") \
    .count() \
    .orderBy("date")

display(gold_daily_trend)
gold_daily_trend.write.format("delta").mode("append").saveAsTable("gold_daily_trend")


# Daily Pattern Across Hours
from pyspark.sql.functions import hour

gold_daily_pattern = silver_df \
    .withColumn("hour", hour("played_at")) \
    .withColumn("time_of_day",
        when((col("hour") >= 5) & (col("hour") < 12), "Morning")
        .when((col("hour") >= 12) & (col("hour") < 17), "Afternoon")
        .when((col("hour") >= 17) & (col("hour") < 21), "Evening")
        .otherwise("Night")
    ) \
    .groupBy("time_of_day") \
    .count() \
    .orderBy("count", ascending=False)

display(gold_daily_pattern)
gold_daily_pattern.write.format("delta").mode("append").saveAsTable("gold_daily_pattern")


# Daywise Trend
from pyspark.sql.functions import hour, date_format, dayofweek, count

gold_daywise_trend = silver_df \
    .withColumn("hour", hour("played_at")) \
    .withColumn("day_of_week", date_format("played_at", "EEEE")) \
    .withColumn("day_num", dayofweek("played_at")) \
    .groupBy("day_num", "day_of_week", "hour") \
    .agg(count("*").alias("count")) \
    .orderBy("day_num", "hour") \
    .drop("day_num") #.drop hides it in final output

display(gold_daywise_trend)
gold_daywise_trend.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("gold_daywise_trend")


# Weekday vs Weekend Analysis
from pyspark.sql.functions import hour, date_format, count, when

gold_weekday_weekend = silver_df \
    .withColumn("hour", hour("played_at")) \
    .withColumn("day_of_week", date_format("played_at", "EEEE")) \
    .withColumn(
        "day_type", 
        when(col("day_of_week").isin("Saturday", "Sunday"), "Weekend").otherwise("Weekday")
    ) \
    .groupBy("day_type", "hour") \
    .agg(count("*").alias("count")) \
    .orderBy("day_type", "hour")

display(gold_weekday_weekend)
gold_weekday_weekend.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("gold_weekday_weekend")

# Top Tracks
from pyspark.sql.functions import count, col

gold_top_tracks = silver_df.groupBy("track") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc())

display(gold_top_tracks)
gold_top_tracks.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("gold_top_tracks")

#Album Popularity
from pyspark.sql.functions import count

gold_album_popularity = silver_df \
    .groupBy("album") \
    .agg(count("*").alias("count")) \
    .orderBy("count", ascending=False)

display(gold_album_popularity)
gold_album_popularity.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("gold_album_popularity")
