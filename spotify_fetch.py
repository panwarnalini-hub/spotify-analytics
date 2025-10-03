import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import os

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")

scope = "user-read-recently-played"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope=scope
))

results = sp.current_user_recently_played(limit=50)

songs = []
for item in results['items']:
    track = item['track']
    songs.append({
        "played_at": item['played_at'],
        "track_name": track['name'],
        "artist": track['artists'][0]['name'],
        "album": track['album']['name'],
        "duration_ms": track['duration_ms']
    })

df = pd.DataFrame(songs)

csv_path = r"C:\Users\Admin\spotify-project\spotify_recent.csv"

# If file exists → append only new rows
if os.path.exists(csv_path):
    old_df = pd.read_csv(csv_path)
    combined_df = pd.concat([old_df, new_df])
    combined_df.drop_duplicates(subset=["played_at"], inplace=True)
    combined_df.to_csv(csv_path, index=False)
    print(f"Updated {csv_path} with {len(new_df)} new songs (after deduplication).")
else:
    new_df.to_csv(csv_path, index=False)
    print(f"Created new {csv_path} with {len(new_df)} songs.")