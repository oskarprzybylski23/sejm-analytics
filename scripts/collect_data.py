"""Entry point for the Sejm data pipeline"""
from src.config import Config
from src.pipeline import SejmDataPipeline


def main():
    """Main entry point"""
    # Load configuration
    config = Config()

    # Choose storage backend: "sqlite", "json", or "csv"
    storage_type = config.storage_type  # Change this to switch storage backends

    # Create and run pipeline
    pipeline = SejmDataPipeline(config, storage_type=storage_type)

    # For initial collection with limited proceedings
    # pipeline.run_full_collection(limit_proceedings=1)

    # For full collection
    # pipeline.run_full_collection()

    # For updates (skips already processed data)
    pipeline.run_incremental_update(limit_proceedings=3)


if __name__ == "__main__":
    main()
