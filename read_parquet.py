import pandas as pd

# Replace 'your_file.parquet' with the actual path to your Parquet file
df = pd.read_parquet('sample_source_data/raw/video_views/region=India/date=2025-08-13/hour=10/3599937e-8bb4-453c-b67d-d89901353385.parquet')

# Now you can work with the data in the DataFrame, e.g., print the head
print(df.columns)
