"""Integration and Unit Tests for API Ingestion Pipeline.
"""

from datetime import datetime, timezone
import os
import tempfile
import unittest
from typing import Any, Dict, List

import duckdb
import pandas as pd
from src.config import APIConfig, AppConfig, StorageConfig, load_config
from src.extract import APIExtractor
from src.load import DuckDBLoader, ParquetExporter
from src.transform import PayloadTransformer


class TestConfigManager(unittest.TestCase):
    """Unit tests for src.config module."""

    def test_load_default_yaml_config(self) -> None:
        """Verify load_config reads and parses config/config.yaml correctly."""
        config = load_config("config/config.yaml")
        self.assertIsInstance(config, AppConfig)
        self.assertEqual(config.api.url, "https://api.openbrewerydb.org/v1/breweries")
        self.assertEqual(config.api.default_per_page, 50)
        self.assertEqual(config.storage.duckdb_path, "data/ingested_data.duckdb")
        self.assertEqual(config.storage.lake_dir, "data/lake")

    def test_missing_config_fallback(self) -> None:
        """Verify load_config falls back to default values when file does not exist."""
        config = load_config("config/non_existent_config.yaml")
        self.assertIsInstance(config, AppConfig)
        self.assertEqual(config.api.max_retries, 3)
        self.assertEqual(config.api.timeout, 10)


class TestAPIExtractor(unittest.TestCase):
    """Unit tests for APIExtractor module."""

    def test_session_creation(self) -> None:
        """Verify extractor creates session with proper retry configuration."""
        extractor = APIExtractor(max_retries=5, backoff_factor=1.0)
        self.assertEqual(extractor.max_retries, 999)
        self.assertEqual(extractor.backoff_factor, 1.0)
        self.assertIsNotNone(extractor.session)


class TestPayloadTransformer(unittest.TestCase):
    """Unit tests for PayloadTransformer module."""

    def setUp(self) -> None:
        """Set up test transformer and raw payload fixtures."""
        self.transformer = PayloadTransformer()
        self.sample_raw_records: List[Dict[str, Any]] = [
            {
                "id": "brewery-123",
                "name": "  Test Brewery  ",
                "brewery_type": "micro",
                "city": "San Francisco",
                "country": "United States",
                "longitude": "-122.4194",
                "latitude": "37.7749",
            },
            {
                "id": "brewery-456",
                "name": "Another Brewery",
                "brewery_type": "large",
                "city": "Austin",
                "country": "United States",
                "longitude": None,
                "latitude": None,
            },
        ]

    def test_transform_valid_records(self) -> None:
        """Test transformation of valid records into normalized DataFrame."""
        df = self.transformer.transform(self.sample_raw_records)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn("ingested_at", df.columns)
        self.assertEqual(df.loc[0, "name"], "Test Brewery")  # Stripped whitespace
        self.assertEqual(df.loc[0, "longitude"], -122.4194)  # Parsed float
        self.assertTrue(pd.isna(df.loc[1, "longitude"]))  # None preserved as NaN

    def test_transform_empty_records(self) -> None:
        """Test transformation of empty records list."""
        df = self.transformer.transform([])
        self.assertTrue(df.empty)
        self.assertIn("id", df.columns)
        self.assertIn("ingested_at", df.columns)


class TestDuckDBLoader(unittest.TestCase):
    """Unit tests and idempotency checks for DuckDBLoader module."""

    def setUp(self) -> None:
        """Set up temporary database path."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_db.duckdb")
        self.loader = DuckDBLoader(db_path=self.db_path, table_name="test_breweries")
        self.transformer = PayloadTransformer()

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    def test_idempotent_load(self) -> None:
        """Verify UPSERT idempotency: duplicate loads do not produce duplicate rows."""
        sample_data: List[Dict[str, Any]] = [
            {
                "id": "b-1",
                "name": "Brewery One",
                "brewery_type": "micro",
                "city": "Denver",
                "country": "United States",
            }
        ]
        df = self.transformer.transform(sample_data)

        # Initial load
        rows_loaded_1 = self.loader.load(df)
        self.assertEqual(rows_loaded_1, 1)
        self.assertEqual(self.loader.get_row_count(), 1)

        # Re-load exact same record (should update, not duplicate)
        rows_loaded_2 = self.loader.load(df)
        self.assertEqual(rows_loaded_2, 1)
        self.assertEqual(self.loader.get_row_count(), 1)

        # Load updated record with same ID
        updated_data: List[Dict[str, Any]] = [
            {
                "id": "b-1",
                "name": "Brewery One Updated",
                "brewery_type": "regional",
                "city": "Denver",
                "country": "United States",
            }
        ]
        df_updated = self.transformer.transform(updated_data)
        self.loader.load(df_updated)

        # Row count remains 1, record updated
        self.assertEqual(self.loader.get_row_count(), 1)


class TestParquetExporter(unittest.TestCase):
    """Unit tests for ParquetExporter module."""

    def setUp(self) -> None:
        """Set up temporary lake directory and fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.exporter = ParquetExporter(base_dir=self.temp_dir.name)
        self.transformer = PayloadTransformer()

    def tearDown(self) -> None:
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    def test_export_hive_partitioning(self) -> None:
        """Verify Parquet export generates Hive-partitioned directory structure."""
        sample_data: List[Dict[str, Any]] = [
            {
                "id": "b-100",
                "name": "Parquet Brewery",
                "brewery_type": "micro",
                "city": "Seattle",
                "country": "United States",
            }
        ]
        df = self.transformer.transform(sample_data)
        test_time = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)

        file_path = self.exporter.export(df, timestamp=test_time)
        self.assertIsNotNone(file_path)
        self.assertTrue(file_path.exists())

        # Verify Hive-style partition path string
        expected_partition = os.path.join(self.temp_dir.name, "year=2026", "month=08", "day=26")
        self.assertEqual(str(file_path.parent), expected_partition)
        self.assertTrue(file_path.name.startswith("batch_20260826_120000"))

        # Read back parquet file and check row count
        try:
            read_df = pd.read_parquet(file_path)
        except Exception:
            conn = duckdb.connect()
            read_df = conn.execute(f"SELECT * FROM '{file_path}'").df()
            conn.close()

        self.assertEqual(len(read_df), 1)
        self.assertEqual(read_df.loc[0, "id"], "b-100")

    def test_export_empty_dataframe(self) -> None:
        """Verify empty DataFrame export returns None without writing file."""
        empty_df = self.transformer.transform([])
        result = self.exporter.export(empty_df)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

