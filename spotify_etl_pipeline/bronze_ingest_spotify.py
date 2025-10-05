# Databricks notebook source
#Check for spotipy module each time this code runs
import sys
import subprocess
subprocess.run([sys.executable, "-m", "pip", "install", "spotipy", "pandas"], check=True)
# Databricks Spotify Auto Ingestion Script
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime
import os

# Initialize Spark session
spark = SparkSession.builder.appName("SpotifyIngestion").getOrCreate()

# Load secrets
CLIENT_ID = dbutils.secrets.get("spotify_creds", "client_id")
CLIENT_SECRET = dbutils.secrets.get("spotify_creds", "client_secret")
REDIRECT_URI = dbutils.secrets.get("spotify_creds", "redirect_uri")
REFRESH_TOKEN = dbutils.secrets.get("spotify_creds", "refresh_token")

# ✅ Use your Databricks Volume path for token storage
cache_dir = "/Volumes/workspace/default/spotify_data"
os.makedirs(cache_dir, exist_ok=True)

# Initialize Spotify client with refresh capability
auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope="user-read-recently-played",
    cache_path=f"{cache_dir}/token.json"
)

# Inject refresh token to auto-renew
auth_manager.refresh_access_token(REFRESH_TOKEN)
sp = spotipy.Spotify(auth_manager=auth_manager)

# Fetch recent tracks
results = sp.current_user_recently_played(limit=50)

# Transform results
songs = [{
    "played_at": item["played_at"],
    "track_name": item["track"]["name"],
    "artist": item["track"]["artists"][0]["name"],
    "album": item["track"]["album"]["name"],
    "duration_ms": item["track"]["duration_ms"]
} for item in results["items"]]

# Create DataFrame
df = pd.DataFrame(songs)
spark_df = spark.createDataFrame(df)

# Save to Delta (Bronze)
spark_df.write.format("delta").mode("append").saveAsTable("spotify_bronze.recent_tracks")

print(f"Auto-ingested {len(df)} tracks at {datetime.now()}")
