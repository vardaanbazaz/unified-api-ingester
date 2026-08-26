"""Main Pipeline Orchestrator.

Chains Extract -> Transform -> Load phases to ingest public API data into DuckDB.
"""

import argparse
import logging
import sys
from typing import List, Optional

from src.config import AppConfig, load_config
from src.extract import APIExtractor
from src.load import DuckDBLoader, ParquetExporter
from src.transform import PayloadTransformer


def setup_logging(level: str = "INFO") -> None:
    """Configure system logging format and log level.

    Args:
        level: Minimum log level string (DEBUG, INFO, WARNING, ERROR).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def run_pipeline(
    api_url: Optional[str] = None,
    max_pages: Optional[int] = None,
    per_page: Optional[int] = None,
    db_path: Optional[str] = None,
    table_name: str = "breweries",
    lake_dir: Optional[str] = None,
    config_path: str = "config/config.yaml",
) -> None:
    """Run the complete Extract-Transform-Load (ETL) pipeline with Dual Sink export.

    Args:
        api_url: Base endpoint URL of the REST API (overrides config if provided).
        max_pages: Maximum number of pages to fetch during extraction.
        per_page: Number of items per page (overrides config if provided).
        db_path: Path to target DuckDB database file (overrides config if provided).
        table_name: Target table name in DuckDB.
        lake_dir: Root directory for the Parquet data lake (overrides config if provided).
        config_path: Path to YAML configuration file.
    """
    logger = logging.getLogger("main")
    config: AppConfig = load_config(config_path)

    target_api_url = api_url if api_url is not None else config.api.url
    target_per_page = per_page if per_page is not None else config.api.default_per_page
    target_db_path = db_path if db_path is not None else config.storage.duckdb_path
    target_lake_dir = lake_dir if lake_dir is not None else config.storage.lake_dir

    logger.info("Starting API Ingestion Pipeline")
    logger.info(
        "Configuration - API URL: %s | Max Pages: %s | DB Path: %s | Lake Dir: %s",
        target_api_url,
        str(max_pages),
        target_db_path,
        target_lake_dir,
    )

    # Phase 1: Extract
    extractor = APIExtractor(base_url=target_api_url, config=config.api)
    raw_records = extractor.fetch_all(max_pages=max_pages, per_page=target_per_page)
    logger.info("Phase 1 Complete: Extracted %d raw records", len(raw_records))

    if not raw_records:
        logger.warning("No records extracted. Pipeline terminating early.")
        return

    # Phase 2: Transform
    transformer = PayloadTransformer()
    transformed_df = transformer.transform(raw_records)
    logger.info("Phase 2 Complete: Transformed into DataFrame with %d rows", len(transformed_df))

    # Phase 3: Load (Dual Sink: DuckDB + Parquet Data Lake)
    loader = DuckDBLoader(db_path=target_db_path, table_name=table_name, config=config.storage)
    rows_loaded = loader.load(transformed_df)
    total_db_rows = loader.get_row_count()
    logger.info("Phase 3 Complete: Upserted %d records into DuckDB. Total table row count: %d", rows_loaded, total_db_rows)

    parquet_exporter = ParquetExporter(base_dir=target_lake_dir, config=config.storage)
    parquet_file = parquet_exporter.export(transformed_df)
    if parquet_file:
        logger.info("Phase 3 (Dual Sink): Exported %d records to Parquet file: %s", len(transformed_df), parquet_file)

    logger.info("Pipeline execution completed successfully!")


def main(argv: Optional[List[str]] = None) -> int:
    """Main entrypoint parsing CLI arguments and executing the ingestion pipeline.

    Args:
        argv: Optional list of CLI argument strings.

    Returns:
        Process exit code (0 for success, 1 for failure).
    """
    parser = argparse.ArgumentParser(description="Public API Ingestion Engine")
    parser.add_argument("--config", type=str, default="config/config.yaml", help="Path to configuration file")
    parser.add_argument("--api-url", type=str, default=None, help="Target API URL (overrides config)")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum pages to fetch")
    parser.add_argument("--per-page", type=int, default=None, help="Records per page (overrides config)")
    parser.add_argument("--db-path", type=str, default=None, help="DuckDB database path (overrides config)")
    parser.add_argument("--table-name", type=str, default="breweries", help="DuckDB table name")
    parser.add_argument("--lake-dir", type=str, default=None, help="Parquet data lake root directory (overrides config)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")

    args = parser.parse_args(argv)
    setup_logging(args.log_level)

    try:
        run_pipeline(
            api_url=args.api_url,
            max_pages=args.max_pages,
            per_page=args.per_page,
            db_path=args.db_path,
            table_name=args.table_name,
            lake_dir=args.lake_dir,
            config_path=args.config,
        )
        return 0
    except Exception as err:
        logging.getLogger("main").critical("Pipeline failed with exception: %s", err, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
