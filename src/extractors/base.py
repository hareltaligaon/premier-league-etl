import time
import requests
from src.logger import get_logger

MAX_RETRIES = 3
BACKOFF = 2  # seconds


class BaseExtractor:
    """
    Base class for all API extractors.
    Provides shared retry and fetch logic — subclasses implement the API-specific details.
    """

    def __init__(self, base_url, source_name):
        self.base_url = base_url
        self.source_name = source_name
        self.logger = get_logger(f"extractor.{source_name}")

    def _get_headers(self) -> dict:
        """Override to add auth headers (e.g. API-Sports)."""
        return {}

    def _get_auth_params(self) -> dict:
        """Override to add auth params (e.g. API-Football)."""
        return {}

    def _get(self, endpoint="", params=None) -> any:
        url = f"{self.base_url}{endpoint}"
        all_params = {**(params or {}), **self._get_auth_params()}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.logger.debug(f"GET {url} — attempt {attempt}/{MAX_RETRIES}")
                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    params=all_params,
                    timeout=10,
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout on attempt {attempt} for {url}")
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP error {e.response.status_code} for {url}: {e}")
                return None
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request error on attempt {attempt}: {e}")

            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF * attempt)

        self.logger.error(f"All {MAX_RETRIES} attempts failed for {url}")
        return None

    def _fetch(self, label, parser, endpoint="", params=None) -> list:
        self.logger.info(f"Fetching {label}")
        data = self._get(endpoint, params)
        try:
            result = parser(data)
            self.logger.info(f"Received {len(result)} {label} records")
            return result
        except (KeyError, IndexError, TypeError):
            self.logger.error(f"Unexpected {label} response structure from {self.source_name}")
            return []

    def fetch_standings(self) -> list:
        raise NotImplementedError

    def fetch_teams(self) -> list:
        raise NotImplementedError

    def fetch(self) -> dict:
        standings = self.fetch_standings()
        teams = self.fetch_teams()
        self.logger.info(f"{self.source_name}: fetched {len(standings)} standings, {len(teams)} teams")
        return {"standings": standings, "teams": teams}
