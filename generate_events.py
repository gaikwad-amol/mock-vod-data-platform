import os
import uuid
import random
import json
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

# --- Configuration ---
EVENTS_DIR = "sample_source_data/raw/video_views"
ID_INPUT_FILE = "generated_content_ids.json"  # File to read state from
NUM_DAYS = 2
EVENTS_PER_HOUR = 100
REGIONS = ["India", "SouthEast Asia", "Middle East"]
EVENT_TYPES = ["login", "logout", "playback_start", "playback_progress", "search"]


def load_content_ids():
    """Loads the list of valid content IDs from the file."""
    try:
        with open(ID_INPUT_FILE, 'r') as f:
            content_ids = json.load(f)
        print(f"SUCCESS: Loaded {len(content_ids)} content IDs from '{ID_INPUT_FILE}'.\n")
        return content_ids
    except FileNotFoundError:
        print(f"ERROR: '{ID_INPUT_FILE}' not found.")
        print("Please run 'generate_content.py' first to create the content data.")
        exit()  # Exit the script if the file doesn't exist


def generate_all_events(valid_content_ids):
    """Generates all event data."""
    print("--- Generating Event Data ---")
    fake = Faker()
    start_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(days=NUM_DAYS)

    for day_offset in range(NUM_DAYS):
        for hour_offset in range(24):
            current_hour_dt = start_date + timedelta(days=day_offset, hours=hour_offset)
            for region in REGIONS:
                events_for_hour = []
                for _ in range(EVENTS_PER_HOUR):
                    event_type = random.choice(EVENT_TYPES)
                    event = {
                        "event_id": str(uuid.uuid4()), "user_id": f"user_{random.randint(1, 100):07d}",
                        "content_id": random.choice(valid_content_ids) if event_type != "login" else None,
                        "event_type": event_type, "timestamp": int(current_hour_dt.timestamp() * 1000),
                        "device_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
                        "technical_context": {"user_agent": fake.user_agent()}
                    }
                    events_for_hour.append(event)

                # Write the collected events to a partitioned file
                if events_for_hour:
                    date_str = current_hour_dt.strftime('%Y-%m-%d');
                    hour_str = current_hour_dt.strftime('%H')
                    partition_path = os.path.join(EVENTS_DIR, f"region={region}", f"date={date_str}",
                                                  f"hour={hour_str}")
                    os.makedirs(partition_path, exist_ok=True)
                    df = pd.DataFrame(events_for_hour)
                    file_path = os.path.join(partition_path, f"{uuid.uuid4()}.parquet")
                    df.to_parquet(file_path, engine='pyarrow', compression='snappy')
    print("SUCCESS: Event data generation complete.")


if __name__ == "__main__":
    # 1. Load the content IDs first
    master_content_ids = load_content_ids()
    # 2. Generate events using those IDs
    generate_all_events(master_content_ids)