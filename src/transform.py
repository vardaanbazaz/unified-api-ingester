"""Data Transformation module.

Parses raw JSON payloads, flattens nested structures, enforces strict data types,
handles missing or null values gracefully, and enriches data with ingestion metadata.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List
import pandas as pd

from src.schema import TRANSFORM_COLUMN_TYPES

logger = logging.getLogger(__name__)


class PayloadTransformer:
    """Transforms raw API JSON payloads into clean, normalized Pandas DataFrames."""

    def __init__(self) -> None:
        """Initialize the payload transformer with predefined schema expectations."""
        self.column_type_map = dict(TRANSFORM_COLUMN_TYPES)

    def transform(self, raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transform raw list of dictionary payloads into a normalized DataFrame.

        Args:
            raw_records: List of raw dictionary payloads from API extraction.

        Returns:
            Normalized pandas DataFrame adhering to target schema and data types.
        """
        if not raw_records:
            logger.warning("Empty records list passed to transformer. Returning empty DataFrame.")
            return self._create_empty_dataframe()

        logger.info("Starting transformation of %d raw records", len(raw_records))

        # Flatten nested JSON if any exist
        df = pd.json_normalize(raw_records)

        # Sanitize column names (replace dots from flattening with underscores)
        df.columns = [col.replace(".", "_") for col in df.columns]

        # Ensure all expected columns exist
        for col in self.column_type_map:
            if col not in df.columns:
                df[col] = None

        # Convert numeric coordinates safely
        for col in ["longitude", "latitude"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert string columns and strip whitespace
        for col, dtype in self.column_type_map.items():
            if col in df.columns and dtype == "string":
                df[col] = df[col].astype("string").str.strip()

        # Reorder columns to target schema order
        target_columns = list(self.column_type_map.keys())
        df = df[target_columns].copy()

        # Inject ingestion metadata timestamp (ISO 8601 UTC)
        ingested_at = datetime.now(timezone.utc)
        df["ingested_at"] = ingested_at

        # Filter out records without a valid primary key (id)
        df = df.dropna(subset=["id"])

        logger.info("Transformation complete. Produced DataFrame with %d rows and %d columns", len(df), len(df.columns))
        return df

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with target columns and types.

        Returns:
            Empty pandas DataFrame with schema columns and ingested_at timestamp.
        """
        df = pd.DataFrame(columns=list(self.column_type_map.keys()))
        for col, dtype in self.column_type_map.items():
            df[col] = df[col].astype(dtype)
        df["ingested_at"] = pd.Series(dtype="datetime64[ns, UTC]")
        return df
