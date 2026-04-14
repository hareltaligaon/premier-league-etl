from datetime import datetime
from src.config import SEASON
from src.transformers.base import BaseTransformer, SCHEMA_VERSION

REQUIRED_STANDING_FIELDS = ["team_id", "team_name", "overall_league_position", "overall_league_PTS"]
MAPPED_STANDING_FIELDS = {
    "team_id", "team_name", "overall_league_position", "overall_league_payed",
    "overall_league_W", "overall_league_D", "overall_league_L",
    "overall_league_GF", "overall_league_GA", "overall_league_PTS",
    "league_id", "league_round",
}


class ApiFootballTransformer(BaseTransformer):

    def __init__(self):
        super().__init__("api_football")

    def _get_team_key(self, entry):
        return str(entry["team_key"])

    def transform(self, raw) -> list:
        standings = raw.get("standings", [])
        teams = raw.get("teams", [])
        teams_lookup = self._build_teams_lookup(teams)
        records = []
        ingested_at = datetime.utcnow().isoformat()

        for entry in standings:
            # Validate required fields
            missing = [f for f in REQUIRED_STANDING_FIELDS if not entry.get(f)]
            if missing:
                self.logger.error(f"Skipping entry — missing required fields: {missing}")
                continue

            team_id = str(entry["team_id"])
            team_name = entry["team_name"]

            # Get team info
            team_info = teams_lookup.get(team_id, {})
            venue = team_info.get("venue", {})

            if not team_info:
                self.logger.warning(f"No team info found for team_id={team_id} ({team_name})")

            record = {
                "team_id": team_id,
                "team_name": team_name,
                "country": team_info.get("team_country"),
                "city": venue.get("venue_city"),
                "founded_year": self._safe_int(team_info.get("team_founded"), "founded_year"),
                "stadium_name": venue.get("venue_name"),
                "season": SEASON,
                "rank": self._safe_int(entry.get("overall_league_position"), "rank"),
                "played": self._safe_int(entry.get("overall_league_payed"), "played"),
                "wins": self._safe_int(entry.get("overall_league_W"), "wins"),
                "draws": self._safe_int(entry.get("overall_league_D"), "draws"),
                "losses": self._safe_int(entry.get("overall_league_L"), "losses"),
                "goals_for": self._safe_int(entry.get("overall_league_GF"), "goals_for"),
                "goals_against": self._safe_int(entry.get("overall_league_GA"), "goals_against"),
                "points": self._safe_int(entry.get("overall_league_PTS"), "points"),
                "source_api": "api_football",
                "ingested_at": ingested_at,
                "schema_version": SCHEMA_VERSION,
                "extra_fields": self._collect_extra_fields(entry, MAPPED_STANDING_FIELDS),
            }

            # Warn on missing optional fields
            optional_missing = [k for k, v in record.items() if v is None and k not in ("season", "source_api", "ingested_at", "extra_fields")]
            if optional_missing:
                self.logger.warning(f"Team '{team_name}' has None values for: {optional_missing}")

            records.append(record)

        self.logger.info(f"API-Football transformer: {len(records)}/{len(standings)} records transformed successfully")
        return records
