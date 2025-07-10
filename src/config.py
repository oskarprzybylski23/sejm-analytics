"""Configuration management for the Sejm data pipeline"""
from dataclasses import dataclass
from typing import Optional
from pathlib import Path


@dataclass
class Config:
    """Application configuration"""
    # API settings
    api_base_url: str = "https://api.sejm.gov.pl"
    api_timeout: int = 30
    api_retry_attempts: int = 3
    api_delay_between_requests: float = 0

    # Data collection settings
    parliamentary_term: int = 10
    batch_size: int = 100

    # Storage settings
    storage_type: str = 'csv'
    data_dir: Path = Path("data")
    database_path: Path = Path("data/sejm.db")
    raw_data_dir: Path = Path("data/raw")
    processed_data_dir: Path = Path("data/processed")

    # Logging
    log_level: str = "INFO"
    log_file: Optional[Path] = Path("logs/sejm_collector.log")

    def __post_init__(self):
        """Create necessary directories"""
        self.data_dir.mkdir(exist_ok=True)
        self.raw_data_dir.mkdir(exist_ok=True)
        self.processed_data_dir.mkdir(exist_ok=True)
        if self.log_file:
            self.log_file.parent.mkdir(exist_ok=True)
