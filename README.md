# sejm-analytics

This repository contains tools and scripts to collect and analyze statements from members of the Polish Parliament (Sejm).
The goal is to build datasets for natural language processing and exploratory analysis of parliamentary proceedings.

## File structure

```bash
.
├── data/            # Data output directory (raw and processed)
├── logs/            # Log files
├── scripts/         # CLI entry points (e.g., data collection)
│   ├── __init__.py
│   └── collect_data.py
├── src/             # Core application logic
│   ├── __init__.py
│   ├── config.py          # Configuration management
│   ├── client.py          # Sejm API client
│   ├── collector.py       # Data collection logic
│   ├── models.py          # Data models
│   ├── pipeline.py        # Orchestration pipeline
│   ├── storage.py         # Storage backends
│   └── analysis/          # NLP and analysis modules
│       └── __init__.py
├── .gitignore
├── README.md
└── requirements.txt


```

## Setup

### 1. Install dependencies
Create and activate a virtual environment (recommended):

```bash
python3 -m venv venv
source venv/bin/activate
```

Install the required packages:

```bash
pip install -r requirements.txt
```

### 1. Configure settings

Edit ``src/config.py`` to customize API parameters, storage locations, and logging.
Example configuration:

```python
    # API settings
    api_base_url: str = "https://api.sejm.gov.pl"
    api_timeout: int = 30
    api_retry_attempts: int = 3
    api_delay_between_requests: float = 0

    # Data collection settings
    parliamentary_term: int = 10
    batch_size: int = 100 # Size of a batch in which output can be saved to file or database

    # Storage settings
    storage_type: str = 'csv' # 'json' available. Other methods can be added in 'src/storage.py'
    data_dir: Path = Path("data")
    database_path: Path = Path("data/sejm.db")
    raw_data_dir: Path = Path("data/raw")
    processed_data_dir: Path = Path("data/processed")

    # Logging
    log_level: str = "INFO"
    log_file: Optional[Path] = Path("logs/sejm_collector.log")
```

## Collecting Data

### 1. Run the collection script
From the project root:

```bash
python -m scripts.collect_data
```

This will:

- Connect to the Sejm API

- Fetch statements for the configured parliamentary term

- Store the data as CSV/JSON depending on your configuration

### 2. Adjust collection behavior

You can customize the pipeline execution inside scripts/collect_data.py.
For example, you can:

- Limit proceedings processed (useful for testing)

- Switch between full collection and incremental updates

```python
# For incremental updates with a limit
pipeline.run_incremental_update(limit_proceedings=3)

# For full collection (uncomment if needed)
# pipeline.run_full_collection()
```

## Extending Storage

To add new storage backends (e.g., SQLite, PostgreSQL), create a module in src/storage/ implementing a compatible interface, then update config.py to select your backend.
