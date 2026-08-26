"""Data Loading module.

Manages connections to local DuckDB database instances, creates table schemas,
and executes idempotent UPSERT operations to persist transformed DataFrames.
"""

from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Optional
import duckdb
import pandas as pd

from src.config import StorageConfig, load_config

logger = logging.getLogger(__name__)


class DuckDBLoader:
    """Manages persistence of normalized DataFrames into a local DuckDB database."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        table_name: str = "breweries",
        config: Optional[StorageConfig] = None,
    ) -> None:
        """Initialize the DuckDB Loader.

        Args:
            db_path: File system path to the DuckDB database file. Defaults to config if None.
            table_name: Target database table name for persisting records.
            config: Optional StorageConfig instance. Defaults to loaded configuration.
        """
        storage_cfg = config if config is not None else load_config().storage
        self.db_path = db_path if db_path is not None else storage_cfg.duckdb_path
        self.table_name = table_name
        self._ensure_db_dir()

    def _ensure_db_dir(self) -> None:
        """Ensure parent directory for database path exists."""
        db_file = Path(self.db_path)
        if db_file.parent and not db_file.parent.exists():
            db_file.parent.mkdir(parents=True, exist_ok=True)
            logger.info("Created directory structure for database at %s", db_file.parent)

    def create_table_if_not_exists(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create target database table with primary key if it does not exist.

        Args:
            conn: Active DuckDB connection instance.
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            brewery_type VARCHAR,
            address_1 VARCHAR,
            address_2 VARCHAR,
            address_3 VARCHAR,
            city VARCHAR,
            state_province VARCHAR,
            postal_code VARCHAR,
            country VARCHAR,
            longitude DOUBLE,
            latitude DOUBLE,
            phone VARCHAR,
            website_url VARCHAR,
            state VARCHAR,
            street VARCHAR,
            ingested_at TIMESTAMPTZ
        );
        """
        conn.execute(ddl)
        logger.info("Verified table structure for '%s'", self.table_name)

    def load(self, df: pd.DataFrame) -> int:
        """Load DataFrame into DuckDB table using UPSERT logic.

        Args:
            df: Normalized pandas DataFrame to persist.

        Returns:
            Number of rows upserted into the table.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to loader. Skipping database write.")
            return 0

        conn: Optional[duckdb.DuckDBPyConnection] = None
        try:
            conn = duckdb.connect(self.db_path)
            self.create_table_if_not_exists(conn)

            # Register DataFrame view inside DuckDB engine
            conn.register("df_temp_view", df)

            # Perform UPSERT (ON CONFLICT DO UPDATE)
            cols = [
                "name", "brewery_type", "address_1", "address_2", "address_3",
                "city", "state_province", "postal_code", "country", "longitude",
                "latitude", "phone", "website_url", "state", "street", "ingested_at"
            ]
            update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols])

            upsert_query = f"""
            INSERT INTO {self.table_name}
            SELECT * FROM df_temp_view
            ON CONFLICT (id) DO UPDATE SET
            {update_clause};
            """

            conn.execute(upsert_query)
            inserted_rows = len(df)
            logger.info("Successfully upserted %d records into table '%s' (%s)", inserted_rows, self.table_name, self.db_path)

            return inserted_rows
        except Exception as err:
            logger.error("Failed to load records into DuckDB (%s): %s", self.db_path, err)
            raise
        finally:
            if conn:
                conn.close()

    def get_row_count(self) -> int:
        """Query current total row count in target table.

        Returns:
            Total row count, or 0 if table does not exist.
        """
        if not Path(self.db_path).exists():
            return 0

        conn: Optional[duckdb.DuckDBPyConnection] = None
        try:
            conn = duckdb.connect(self.db_path)
            result = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()
            return result[0] if result else 0
        except Exception as err:
            logger.warning("Could not retrieve row count for table '%s': %s", self.table_name, err)
            return 0
        finally:
            if conn:
                conn.close()


class ParquetExporter:
    """Manages persistence of normalized DataFrames into a local Parquet data lake with Hive-style partitioning."""

    def __init__(
        self,
        base_dir: Optional[str] = None,
        config: Optional[StorageConfig] = None,
    ) -> None:
        """Initialize the Parquet Exporter.

        Args:
            base_dir: Root directory for the Parquet data lake. Defaults to config if None.
            config: Optional StorageConfig instance. Defaults to loaded configuration.
        """
        storage_cfg = config if config is not None else load_config().storage
        target_dir = base_dir if base_dir is not None else storage_cfg.lake_dir
        self.base_dir = Path(target_dir)

    def export(self, df: pd.DataFrame, timestamp: Optional[datetime] = None) -> Optional[Path]:
        """Export DataFrame to Hive-partitioned Parquet file.

        The destination path follows Hive-style partitioning based on UTC date:
        `<base_dir>/year=YYYY/month=MM/day=DD/batch_<timestamp>.parquet`

        Args:
            df: Normalized pandas DataFrame to export.
            timestamp: Optional datetime override for partition path & timestamp (defaults to current UTC time).

        Returns:
            Path to written parquet file, or None if DataFrame is empty.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to Parquet exporter. Skipping export.")
            return None

        now = timestamp if timestamp is not None else datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        year_str = now.strftime("%Y")
        month_str = now.strftime("%m")
        day_str = now.strftime("%d")
        ts_str = now.strftime("%Y%m%d_%H%M%S")

        partition_dir = self.base_dir / f"year={year_str}" / f"month={month_str}" / f"day={day_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        file_path = partition_dir / f"batch_{ts_str}.parquet"

        try:
            try:
                df.to_parquet(file_path, index=False)
            except Exception as parquet_err:
                logger.warning("pandas to_parquet export failed (%s), attempting DuckDB Parquet write fallback.", parquet_err)
                conn = duckdb.connect()
                conn.register("df_temp_view", df)
                conn.execute(f"COPY df_temp_view TO '{file_path}' (FORMAT PARQUET)")
                conn.close()

            logger.info("Successfully exported %d records to Parquet lake at %s", len(df), file_path)
            return file_path
        except Exception as err:
            logger.error("Failed to export records to Parquet (%s): %s", file_path, err)
            raise

