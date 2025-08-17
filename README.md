## Set the appropriate python version 
```
pyenv versions
pyenv latest 3.10
cd mock-vod-data-platform
python -m venv .venv
source .venv/bin/activate
```
## Folder structure for the project
Create the following folders and files inside your vod_data_platform directory:

```
vod_data_platform/
├── .venv/                   # Your virtual environment (already created)
├── notebooks/               # For Jupyter notebooks for exploration and testing
├── src/                     # Your main source code will live here
│   └── vod_platform/
│       ├── __init__.py      # Makes the folder a Python package
│       ├── jobs/            # For different Spark jobs (e.g., bronze_ingestion.py)
│       └── utils/           # For shared helper functions (e.g., spark_session.py)
├── tests/                   # For your unit and integration tests
├── pyproject.toml           # The modern way to define project dependencies & metadata
├── .gitignore               # To tell Git which files to ignore (like .venv)
└── README.md                # A description of your project

```
create the pyproject.toml
add the following 
```
[project]
name = "vod_data_platform"
version = "0.1.0"
description = "Data platform for the VOD service."
dependencies = [
    "pyspark",
    "pandas",      # Very useful for interacting with Spark DataFrames
    "boto3"        # The AWS SDK for Python, to interact with S3
]
```

```
pip install .
```

## Set credentials and endpoint for MinIO

```export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_ACCESS_KEY_ID="minioadmin"
export MINIO_ENDPOINT="http://localhost:9000"
```
## Set your real AWS credentials
```
export AWS_ACCESS_KEY_ID="YOUR_REAL_AWS_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_REAL_AWS_SECRET_KEY"
unset MINIO_ENDPOINT # Ensure this is not set
```


Commands to run
```
python generate_content.py
python generate_events.py
-- copy the files to the minio using the UI.

PYTHONPATH=src python -m vod_platform.jobs.bronze_manual_ingestion --process-datetime "2025-08-13T10:00:00"
```

Run below command to submit job to spark

```shell
podman exec -it jupyterlab /bin/bash -c "cd /opt/bitnami/spark/src/ && spark-submit vod_platform/jobs/manual_events_ingestion.py --process-datetime '2025-08-13T04:00:00'"
```