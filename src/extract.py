"""API Extraction module.

Handles HTTP requests, retries with exponential backoff, status code verification,
timeout handling, and pagination for extracting records from public REST APIs.
"""

import logging
from typing import Any, Dict, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import APIConfig, load_config

logger = logging.getLogger(__name__)


class APIExtractor:
    """Extracts records from a public REST API using HTTP GET requests with retry logic."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        backoff_factor: float = 0.5,
        config: Optional[APIConfig] = None,
    ) -> None:
        """Initialize the API Extractor.

        Args:
            base_url: The base endpoint URL for data extraction.
            timeout: Request timeout in seconds.
            max_retries: Number of retry attempts for failed HTTP requests.
            backoff_factor: Backoff factor for exponential retry strategy.
            config: Optional APIConfig instance. Defaults to loaded configuration.
        """
        api_cfg = config if config is not None else load_config().api

        self.base_url = base_url if base_url is not None else api_cfg.url
        self.timeout = timeout if timeout is not None else api_cfg.timeout
        self.max_retries = max_retries if max_retries is not None else api_cfg.max_retries
        self.default_per_page = api_cfg.default_per_page
        self.backoff_factor = backoff_factor
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Configure a requests.Session with HTTP retries and backoff logic.

        Returns:
            Configured requests.Session object.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def fetch_page(self, page: int = 1, per_page: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch a single page of data from the REST API.

        Args:
            page: Page number to request (1-indexed).
            per_page: Number of items per page. If None, uses default from config.

        Returns:
            List of dictionary payloads for the page.

        Raises:
            requests.HTTPError: If response status is non-200 after retries.
            requests.RequestException: On underlying network errors.
        """
        num_per_page = per_page if per_page is not None else self.default_per_page
        params = {"page": page, "per_page": num_per_page}
        logger.info("Fetching page %d (per_page=%d) from %s", page, num_per_page, self.base_url)

        try:
            response = self.session.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                logger.warning("Unexpected response format on page %d: expected list, got %s", page, type(data))
                return []

            logger.info("Successfully fetched %d records from page %d", len(data), page)
            return data
        except requests.HTTPError as http_err:
            logger.error("HTTP error occurred while fetching page %d: %s", page, http_err)
            raise
        except requests.RequestException as req_err:
            logger.error("Request exception occurred while fetching page %d: %s", page, req_err)
            raise

    def fetch_all(self, max_pages: Optional[int] = None, per_page: Optional[int] = None) -> List[Dict[str, Any]]:
        """Paginate and fetch all records up to max_pages limit.

        Args:
            max_pages: Optional maximum number of pages to fetch. If None, fetches until empty response.
            per_page: Number of items per page. If None, uses default from config.

        Returns:
            Combined list of all extracted record dictionaries.
        """
        num_per_page = per_page if per_page is not None else self.default_per_page
        all_records: List[Dict[str, Any]] = []
        current_page = 1

        while True:
            if max_pages is not None and current_page > max_pages:
                logger.info("Reached maximum page limit of %d", max_pages)
                break

            records = self.fetch_page(page=current_page, per_page=num_per_page)
            if not records:
                logger.info("No more records returned at page %d. Extraction complete.", current_page)
                break

            all_records.extend(records)
            current_page += 1

        logger.info("Total records extracted across %d page(s): %d", current_page - 1, len(all_records))
        return all_records
