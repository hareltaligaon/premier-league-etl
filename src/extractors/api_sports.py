import time
import requests
from src.config import API_SPORTS_KEY, API_SPORTS_LEAGUE_ID, SEASON
from src.logger import get_logger

logger = get_logger("extractor.api_sports")

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_SPORTS_KEY}
MAX_RETRIES = 3
BACKOFF = 2  # seconds


def _get(endpoint: str, params: dict):
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug(f"GET {url} params={params} (attempt {attempt})")
            response = requests.get(url, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("errors"):
                logger.error(f"API error: {data['errors']}")
                return {}
            return data
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt} for {url}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} for {url}: {e}")
            return {}
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on attempt {attempt}: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(BACKOFF * attempt)

    logger.error(f"All {MAX_RETRIES} attempts failed for {url}")
    return {}


def fetch_standings() -> list:
    logger.info(f"Fetching standings: league={API_SPORTS_LEAGUE_ID}, season={SEASON}")
    data = _get("/standings", {"league": API_SPORTS_LEAGUE_ID, "season": SEASON})
    try:
        return data["response"][0]["league"]["standings"][0]
    except (KeyError, IndexError, TypeError):
        logger.error("Unexpected standings response structure from API-Sports")
        return []


def fetch_teams() -> list:
    logger.info(f"Fetching teams: league={API_SPORTS_LEAGUE_ID}, season={SEASON}")
    data = _get("/teams", {"league": API_SPORTS_LEAGUE_ID, "season": SEASON})
    try:
        return data["response"]
    except (KeyError, TypeError):
        logger.error("Unexpected teams response structure from API-Sports")
        return []


def fetch() -> dict:
    standings = fetch_standings()
    teams = fetch_teams()
    logger.info(f"API-Sports: fetched {len(standings)} standings, {len(teams)} teams")
    return {"standings": standings, "teams": teams}
