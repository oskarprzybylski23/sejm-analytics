"""Main data pipeline orchestrator"""
import logging
from typing import Optional

from src.config import Config
from src.storage import StorageBackend, JSONStorage, CSVStorage
from src.collector import DataCollector

logger = logging.getLogger(__name__)


class SejmDataPipeline:
    """Main pipeline orchestrator"""

    def __init__(self, config: Optional[Config] = None, storage_type: str = "sqlite"):
        self.config = config or Config()
        self._setup_logging()

        # Initialize storage backend based on type
        self.storage = self._init_storage(storage_type)

        # Initialize collector
        self.collector = DataCollector(self.config, self.storage)

    def _setup_logging(self):
        """Setup logging configuration"""
        handlers = [logging.StreamHandler()]

        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=handlers
        )

    def _init_storage(self, storage_type: str) -> StorageBackend:
        """Initialize storage backend based on type"""
        if storage_type == "json":
            logger.info("Using JSON storage backend")
            return JSONStorage(self.config.raw_data_dir)
        elif storage_type == "csv":
            logger.info("Using CSV storage backend")
            return CSVStorage(self.config.data_dir, self.config.batch_size)

    def run_full_collection(self, limit_proceedings: Optional[int] = None):
        """Run full data collection pipeline"""
        logger.info("Starting full data collection")

        try:
            # Collect MPs
            self.collector.collect_mps()

            # Collect speeches
            self.collector.collect_statements(
                limit_proceedings=limit_proceedings, update_existing=True)

            logger.info("Data collection completed successfully")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

    def run_incremental_update(self, limit_proceedings: Optional[int] = None):
        """Run incremental update (only new data)"""
        logger.info("Starting incremental update")

        # Collect MPs
        self.collector.collect_mps()

        # The collector automatically skips already processed speeches
        # Not implemented properly yet
        self.collector.collect_statements(
            limit_proceedings=limit_proceedings, update_existing=False)

        logger.info("Incremental update completed")
