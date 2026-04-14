from datetime import datetime
from src.config import SEASON
from src.transformers.base import BaseTransformer, SCHEMA_VERSION

REQUIRED_STANDING_FIELDS = ["rank", "points", "team"]
MAPPED_STANDING_FIELDS = {"rank", "points", "team", "all", "goalsDiff", "form", "description", "update", "home", "away", "status"}


class ApiSportsTransformer(BaseTransformer):

    def __init__(self):
        super().__init__("api_sports")

    def _get_team_key(self, entry):
        return entry["team"]["id"]

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

            team_id = entry["team"]["id"]
            team_name = entry["team"].get("name")

            if not team_name:
                self.logger.error(f"Skipping entry — missing team name for team_id={team_id}")
                continue

            # Get team info
            team_info = teams_lookup.get(team_id, {})
            team_data = team_info.get("team", {})
            venue_data = team_info.get("venue", {})

            if not team_info:
                self.logger.warning(f"No team info found for team_id={team_id} ({team_name})")

            all_stats = entry.get("all", {})
            goals = all_stats.get("goals", {})

            record = {
                "team_id": str(team_id),
                "team_name": team_name,
                "country": team_data.get("country"),
                "city": venue_data.get("city"),
                "founded_year": self._safe_int(team_data.get("founded"), "founded_year"),
                "stadium_name": venue_data.get("name"),
                "season": SEASON,
                "rank": self._safe_int(entry.get("rank"), "rank"),
                "played": self._safe_int(all_stats.get("played"), "played"),
                "wins": self._safe_int(all_stats.get("win"), "wins"),
                "draws": self._safe_int(all_stats.get("draw"), "draws"),
                "losses": self._safe_int(all_stats.get("lose"), "losses"),
                "goals_for": self._safe_int(goals.get("for"), "goals_for"),
                "goals_against": self._safe_int(goals.get("against"), "goals_against"),
                "points": self._safe_int(entry.get("points"), "points"),
                "source_api": "api_sports",
                "ingested_at": ingested_at,
                "schema_version": SCHEMA_VERSION,
                "extra_fields": self._collect_extra_fields(entry, MAPPED_STANDING_FIELDS),
            }

            # Warn on missing optional fields
            optional_missing = [k for k, v in record.items() if v is None and k not in ("season", "source_api", "ingested_at", "extra_fields")]
            if optional_missing:
                self.logger.warning(f"Team '{team_name}' has None values for: {optional_missing}")

            records.append(record)

        self.logger.info(f"API-Sports transformer: {len(records)}/{len(standings)} records transformed successfully")
        return records
