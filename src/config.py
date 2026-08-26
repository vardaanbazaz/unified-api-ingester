"""Configuration Manager module.

Loads, parses, and provides access to structured application configuration
defined in YAML files.
"""

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union
import yaml

logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """API extraction configuration parameters."""

    url: str = "https://api.openbrewerydb.org/v1/breweries"
    default_per_page: int = 50
    max_retries: int = 3
    timeout: int = 10


@dataclass
class StorageConfig:
    """Storage configuration parameters."""

    duckdb_path: str = "data/ingested_data.duckdb"
    lake_dir: str = "data/lake"


@dataclass
class AppConfig:
    """Central Application Configuration object."""

    api: APIConfig
    storage: StorageConfig


def load_config(config_path: Union[str, Path] = "config/config.yaml") -> AppConfig:
    """Load configuration parameters from a YAML file.

    Args:
        config_path: File system path to the YAML configuration file.

    Returns:
        Populated AppConfig instance.
    """
    path = Path(config_path)

    api_defaults = APIConfig()
    storage_defaults = StorageConfig()

    if not path.exists():
        logger.warning("Configuration file not found at %s. Using default configuration.", path)
        return AppConfig(api=api_defaults, storage=storage_defaults)

    try:
        with open(path, "r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
            raw_data: Dict[str, Any] = raw if isinstance(raw, dict) else {}

        api_data = raw_data.get("api")
        if not isinstance(api_data, dict):
            api_data = {}

        storage_data = raw_data.get("storage")
        if not isinstance(storage_data, dict):
            storage_data = {}

        api_config = APIConfig(
            url=api_data.get("url", api_defaults.url),
            default_per_page=int(api_data.get("default_per_page", api_defaults.default_per_page)),
            max_retries=int(api_data.get("max_retries", api_defaults.max_retries)),
            timeout=int(api_data.get("timeout", api_defaults.timeout)),
        )

        storage_config = StorageConfig(
            duckdb_path=storage_data.get("duckdb_path", storage_defaults.duckdb_path),
            lake_dir=storage_data.get("lake_dir", storage_defaults.lake_dir),
        )

        logger.info("Successfully loaded configuration from %s", path)
        return AppConfig(api=api_config, storage=storage_config)
    except Exception as err:
        logger.error("Error parsing configuration file at %s: %s. Using defaults.", path, err)
        return AppConfig(api=api_defaults, storage=storage_defaults)
