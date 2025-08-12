pyenv versions
pyenv latest 3.10
cd mock-vod-data-platform
pyenv -m venv .venv
source .venv/bin/activate

Create the following folders and files inside your vod_data_platform directory:

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

create the pyproject.toml
add the following 

[project]
name = "vod_data_platform"
version = "0.1.0"
description = "Data platform for the VOD service."
dependencies = [
    "pyspark",
    "pandas",      # Very useful for interacting with Spark DataFrames
    "boto3"        # The AWS SDK for Python, to interact with S3
]

pip install .