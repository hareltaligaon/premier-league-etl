from src.config import API_FOOTBALL_KEY, API_FOOTBALL_LEAGUE_ID
from src.extractors.base import BaseExtractor


class ApiFootballExtractor(BaseExtractor):

    def __init__(self):
        super().__init__(
            base_url="https://apiv3.apifootball.com/",
            source_name="api_football",
        )

    def _get_auth_params(self) -> dict:
        return {"APIkey": API_FOOTBALL_KEY}

    def fetch_standings(self) -> list:
        self.logger.info(f"Fetching standings: league_id={API_FOOTBALL_LEAGUE_ID}")
        data = self._get(params={"action": "get_standings", "league_id": API_FOOTBALL_LEAGUE_ID})
        if not isinstance(data, list):
            self.logger.error("Unexpected standings response structure from API-Football")
            return []
        self.logger.info(f"Received {len(data)} standing records")
        return data

    def fetch_teams(self) -> list:
        self.logger.info(f"Fetching teams: league_id={API_FOOTBALL_LEAGUE_ID}")
        data = self._get(params={"action": "get_teams", "league_id": API_FOOTBALL_LEAGUE_ID})
        if not isinstance(data, list):
            self.logger.error("Unexpected teams response structure from API-Football")
            return []
        self.logger.info(f"Received {len(data)} team records")
        return data
