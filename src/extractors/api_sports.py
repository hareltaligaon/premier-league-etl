from src.config import API_SPORTS_KEY, API_SPORTS_LEAGUE_ID, SEASON
from src.extractors.base import BaseExtractor


class ApiSportsExtractor(BaseExtractor):

    def __init__(self):
        super().__init__(
            base_url="https://v3.football.api-sports.io",
            source_name="api_sports",
        )

    def _get_headers(self) -> dict:
        return {"x-apisports-key": API_SPORTS_KEY}

    def fetch_standings(self) -> list:
        self.logger.info(f"Fetching standings: league={API_SPORTS_LEAGUE_ID}, season={SEASON}")
        data = self._get("/standings", {"league": API_SPORTS_LEAGUE_ID, "season": SEASON})
        try:
            return data["response"][0]["league"]["standings"][0]
        except (KeyError, IndexError, TypeError):
            self.logger.error("Unexpected standings response structure from API-Sports")
            return []

    def fetch_teams(self) -> list:
        self.logger.info(f"Fetching teams: league={API_SPORTS_LEAGUE_ID}, season={SEASON}")
        data = self._get("/teams", {"league": API_SPORTS_LEAGUE_ID, "season": SEASON})
        try:
            return data["response"]
        except (KeyError, TypeError):
            self.logger.error("Unexpected teams response structure from API-Sports")
            return []


def fetch_api_sports() -> dict:
    return ApiSportsExtractor().fetch()
