"""Fetch open building permits from a Socrata/SODA-compatible open data API."""

import os
import logging
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://data.austintexas.gov/resource/3syk-w9eu.json"
DEFAULT_LIMIT = 200


class PermitFetchError(Exception):
    """Raised when the permits API returns an unexpected response."""


class PermitFetcher:
    """Fetches open building permits from a Socrata/SODA open-data endpoint.

    The default data source is the City of Austin open building-permit dataset,
    but any Socrata-compatible endpoint can be substituted via the
    ``PERMITS_API_URL`` environment variable.
    """

    def __init__(
        self,
        api_url: str | None = None,
        app_token: str | None = None,
        limit: int | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.api_url = api_url or os.getenv("PERMITS_API_URL", DEFAULT_API_URL)
        self.app_token = app_token or os.getenv("PERMITS_APP_TOKEN", "")
        self.limit = limit or int(os.getenv("PERMITS_LIMIT", str(DEFAULT_LIMIT)))
        self._session = session or requests.Session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(self, status_filter: str = "open") -> list[dict[str, Any]]:
        """Return a list of permit records with ``status_filter`` applied.

        Each record is a plain dict whose keys mirror the upstream API fields.

        Args:
            status_filter: Value to match against the ``status_current``
                column.  Pass an empty string to skip the filter.

        Returns:
            List of permit dicts.

        Raises:
            PermitFetchError: If the HTTP request fails or the response is
                not a JSON array.
        """
        params: dict[str, Any] = {"$limit": self.limit}
        if status_filter:
            params["status_current"] = status_filter

        headers: dict[str, str] = {}
        if self.app_token:
            headers["X-App-Token"] = self.app_token

        try:
            response = self._session.get(
                self.api_url, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise PermitFetchError(f"Failed to fetch permits: {exc}") from exc

        data = response.json()
        if not isinstance(data, list):
            raise PermitFetchError(
                f"Expected a JSON array from the permits API, got {type(data).__name__}"
            )

        logger.info("Fetched %d permit records", len(data))
        return data
