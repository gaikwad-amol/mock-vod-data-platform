import os
import random
import json
import pandas as pd
from faker import Faker

# --- Configuration ---
CONTENT_DIR = "sample_source_data/raw/content"
ID_OUTPUT_FILE = "generated_content_ids.json" # File to share state
NUM_CONTENT_ITEMS_PER_REGION = 200
REGIONS = ["India", "SouthEast Asia", "Middle East"]

def generate_content_data():
    """Generates content metadata for all regions and saves a master list of content IDs."""
    print("--- Generating Content Metadata ---")
    fake = Faker()
    master_content_ids = []

    for region in REGIONS:
        content_list = []
        for i in range(NUM_CONTENT_ITEMS_PER_REGION):
            content_type = random.choice(["movie", "tv_show"])
            content_id = f"{content_type}_{region.lower().replace(' ','')}_{i:04d}"
            master_content_ids.append(content_id)

            content = {
                "content_id": content_id,
                "title": fake.catch_phrase(),
                "content_type": content_type,
                "genres": [random.choice(["Drama", "Comedy", "Action", "Thriller", "Sci-Fi"])],
                "release_year": int(fake.year()),
                "rating": random.choice(["G", "PG", "PG-13", "R"]),
                "duration_minutes": random.randint(22, 180),
                "languages": [random.choice(["en", "es", "hi", "fr"])],
                "production_countries": [fake.country_code()],
                "description": fake.sentence(),
            }
            content_list.append(content)

        # Write this region's content data to its partitioned directory
        partition_path = os.path.join(CONTENT_DIR, f"region={region}")
        os.makedirs(partition_path, exist_ok=True)
        df = pd.DataFrame(content_list)
        file_path = os.path.join(partition_path, "content_snapshot.parquet")
        df.to_parquet(file_path, engine='pyarrow', compression='snappy')
        print(f"    OK: Wrote {len(content_list)} content records to {file_path}")

    # Save the master list of IDs to a file for the other script to use
    with open(ID_OUTPUT_FILE, 'w') as f:
        json.dump(master_content_ids, f)
    print(f"\nSUCCESS: Saved {len(master_content_ids)} content IDs to '{ID_OUTPUT_FILE}'")


if __name__ == "__main__":
    generate_content_data()