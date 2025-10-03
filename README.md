#  Spotify Listening Analytics

This project fetches my Spotify listening history using the Spotify Web API, stores it in a CSV, and prepares it for analytics with Databricks + Power BI.

---

##  Inspiration
When Taylor Swift’s new album *Life of a Showgirl* was released, I found myself looping it non-stop. That got me curious about my actual listening patterns on Spotify:

- Which artists do I really listen to the most?  
- Do I listen more at night or during the day?  
- How much time do I spend on music every week?  
- How do new releases change my listening behavior?  

That curiosity inspired me to build this project.

---

## Project Structure

The project is organized as follows:

- **spotify_fetch.py**: Python script that connects to the Spotify API and fetches my recently played songs.  
- **spotify_recent.csv**: CSV file that stores the incremental dataset of my listening history.  
- **requirements.txt**: List of Python dependencies (mainly `spotipy` and `pandas`).  
- **README.md**: Documentation of the project, setup instructions, and explanation of the pipeline.  

---

## How It Works
1. **Data Source**  
   - Spotify Web API (`user-read-recently-played` scope).  
   - Fetches the **last 50 played tracks**.  

2. **Ingestion Script (`spotify_fetch.py`)**  
   - Uses `spotipy` to authenticate.  
   - Saves listening history into `spotify_recent.csv`.  
   - Automatically appends new songs each run (deduplicated by `played_at`).  

3. **Pipeline Plan**  
   - **Bronze Layer**: Raw `spotify_recent.csv` in Databricks.  
   - **Silver Layer**: Cleaned tables (track, artist, album, played_date, played_hour, duration_min).  
   - **Gold Layer**: Aggregated analytics (listening hours, top artists per month, daily patterns).  

4. **Visualization (Power BI)**  
   Planned dashboards:  
   - Top Artists & Tracks  
   - Listening Heatmap (by hour/day)  
   - Monthly Trends (listening hours, new releases impact)  
   - Album Release Effect (e.g., Taylor Swift’s *Life of a Showgirl*)  

---

## Getting Started
1. Clone this repo.  
2. Install dependencies:  
   ```bash
   pip install -r requirements.txt
3. Create a Spotify Developer App → get your CLIENT_ID, CLIENT_SECRET, REDIRECT_URI.
4. Update spotify_fetch.py with your credentials.
5. Run:
```bash
python spotify_fetch.py
```
---

## Challenges & Learnings
- Setting up Spotify API credentials and redirect URI.  
- Handling duplicates in listening history.  
- Organizing the project for pipelines (Bronze → Silver → Gold).  

---

## Next Steps
- Automate daily data fetch with Task Scheduler.  
- Build Databricks SQL transformations.  
- Publish Power BI dashboard screenshots.  
- Extend analysis with Spotify Audio Features (tempo, danceability, energy).  
