import time
import requests
from src.config import API_FOOTBALL_KEY, API_FOOTBALL_LEAGUE_ID
from src.logger import get_logger

logger = get_logger("extractor.api_football")

BASE_URL = "https://apiv3.apifootball.com/"
MAX_RETRIES = 3
BACKOFF = 2  # seconds


def _get(params: dict):
    params["APIkey"] = API_FOOTBALL_KEY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug(f"GET {BASE_URL} params={params} (attempt {attempt})")
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("error"):
                logger.error(f"API error: {data}")
                return []
            return data
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            return []
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on attempt {attempt}: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(BACKOFF * attempt)

    logger.error(f"All {MAX_RETRIES} attempts failed for {BASE_URL}")
    return []


def fetch_standings() -> list:
    logger.info(f"Fetching standings: league_id={API_FOOTBALL_LEAGUE_ID}")
    data = _get({"action": "get_standings", "league_id": API_FOOTBALL_LEAGUE_ID})
    if not isinstance(data, list):
        logger.error("Unexpected standings response structure from API-Football")
        return []
    logger.info(f"Received {len(data)} standing records")
    return data


def fetch_teams() -> list:
    logger.info(f"Fetching teams: league_id={API_FOOTBALL_LEAGUE_ID}")
    data = _get({"action": "get_teams", "league_id": API_FOOTBALL_LEAGUE_ID})
    if not isinstance(data, list):
        logger.error("Unexpected teams response structure from API-Football")
        return []
    logger.info(f"Received {len(data)} team records")
    return data


def fetch() -> dict:
    standings = fetch_standings()
    teams = fetch_teams()
    logger.info(f"API-Football: fetched {len(standings)} standings, {len(teams)} teams")
    return {"standings": standings, "teams": teams}
