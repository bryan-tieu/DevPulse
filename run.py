from ingestion.ingest import ingest_hour
from transform.event_counts import load_event_counts

def run(date: str, hour: int) -> None:
    
    key = ingest_hour(date, hour)
    table_id = load_event_counts(date, hour, key)
    print(f"Pipeline complete -> {table_id}")
    

if __name__ == "__main__":
    run("2024-01-01", 15)