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
        return self._fetch(
            label=f"standings: league={API_SPORTS_LEAGUE_ID}, season={SEASON}",
            parser=lambda d: d["response"][0]["league"]["standings"][0],
            endpoint="/standings",
            params={"league": API_SPORTS_LEAGUE_ID, "season": SEASON},
        )

    def fetch_teams(self) -> list:
        return self._fetch(
            label=f"teams: league={API_SPORTS_LEAGUE_ID}, season={SEASON}",
            parser=lambda d: d["response"],
            endpoint="/teams",
            params={"league": API_SPORTS_LEAGUE_ID, "season": SEASON},
        )
