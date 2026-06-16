import io
import requests
from google.cloud import storage
from config import BRONZE_BUCKET, GCP_PROJECT

GH_ARCHIVE_URL = "https://data.gharchive.org/{date}-{hour}.json.gz"

def bronze_key(date: str, hour: int) -> str:
    
    return f"date={date}/hour={hour:02d}/{date}-{hour}.json.gz"

def ingest_hour(date: str, hour: int) -> str:
    
    bucket = storage.Client(project=GCP_PROJECT).bucket(BRONZE_BUCKET)
    
    key = bronze_key(date, hour)
    
    blob = bucket.blob(key)
    
    if blob.exists():
        print(f"Already in bronze, skipping: {key}")
        return key

    url = GH_ARCHIVE_URL.format(date=date, hour=hour)
    print(f"Downdloading {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    blob.upload_from_file(io.BytesIO(response.content), content_type="application/gzip")
    print(f"Landed {len(response.content):,} bytes -> gs://{BRONZE_BUCKET}/{key}")
    return key

if __name__ == "__main__":
    ingest_hour("2024-01-01", 15)