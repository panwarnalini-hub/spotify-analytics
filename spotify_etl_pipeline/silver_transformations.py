# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from datetime import datetime

spark = SparkSession.builder.appName("SpotifySilverTransformations").getOrCreate()

# Read from Bronze
bronze_df = spark.read.table("workspace.spotify_bronze.recent_tracks")

# Transform data (clean & deduplicate)
silver_df = bronze_df \
    .withColumnRenamed("track_name", "track") \
    .withColumn("track", trim(col("track"))) \
    .withColumn("artist", trim(col("artist"))) \
    .withColumn("album", trim(col("album"))) \
    .dropna() \
    .dropDuplicates(["played_at"])

silver_df.write.format("delta").mode("append").saveAsTable("spotify_silver.cleaned_tracks")

print(f"Silver table created successfully at {datetime.now()} with {silver_df.count()} records.")
