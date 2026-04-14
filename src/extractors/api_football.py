from src.config import API_FOOTBALL_KEY, API_FOOTBALL_LEAGUE_ID
from src.extractors.base import BaseExtractor


def _as_list(data):
    if not isinstance(data, list):
        raise TypeError("Expected list response")
    return data


class ApiFootballExtractor(BaseExtractor):

    def __init__(self):
        super().__init__(
            base_url="https://apiv3.apifootball.com/",
            source_name="api_football",
        )

    def _get_auth_params(self) -> dict:
        return {"APIkey": API_FOOTBALL_KEY}

    def fetch_standings(self) -> list:
        return self._fetch(
            label=f"standings: league_id={API_FOOTBALL_LEAGUE_ID}",
            parser=_as_list,
            params={"action": "get_standings", "league_id": API_FOOTBALL_LEAGUE_ID},
        )

    def fetch_teams(self) -> list:
        return self._fetch(
            label=f"teams: league_id={API_FOOTBALL_LEAGUE_ID}",
            parser=_as_list,
            params={"action": "get_teams", "league_id": API_FOOTBALL_LEAGUE_ID},
        )
