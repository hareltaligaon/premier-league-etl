from datetime import datetime
from src.config import SEASON
from src.logger import get_logger

logger = get_logger("transformer.api_sports")

REQUIRED_STANDING_FIELDS = ["rank", "points", "team"]
REQUIRED_TEAM_FIELDS = ["team", "venue"]


def _build_teams_lookup(teams: list) -> dict:
    lookup = {}
    for entry in teams:
        try:
            team_id = entry["team"]["id"]
            lookup[team_id] = entry
        except (KeyError, TypeError):
            logger.warning(f"Could not index team entry: {entry}")
    return lookup


def _safe_int(value, field: str):
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(f"Could not convert '{field}' value '{value}' to int")
        return None


def transform(raw: dict) -> list:
    standings = raw.get("standings", [])
    teams = raw.get("teams", [])
    teams_lookup = _build_teams_lookup(teams)
    records = []
    ingested_at = datetime.utcnow().isoformat()

    for entry in standings:
        # Validate required fields
        missing = [f for f in REQUIRED_STANDING_FIELDS if not entry.get(f)]
        if missing:
            logger.error(f"Skipping standing entry — missing required fields: {missing} | data: {entry}")
            continue

        team_id = entry["team"]["id"]
        team_name = entry["team"].get("name")

        if not team_name:
            logger.error(f"Skipping entry — missing team name for team_id={team_id}")
            continue

        # Get team info
        team_info = teams_lookup.get(team_id, {})
        team_data = team_info.get("team", {})
        venue_data = team_info.get("venue", {})

        if not team_info:
            logger.warning(f"No team info found for team_id={team_id} ({team_name})")

        all_stats = entry.get("all", {})
        goals = all_stats.get("goals", {})

        record = {
            "team_id":       str(team_id),
            "team_name":     team_name,
            "country":       team_data.get("country"),
            "city":          venue_data.get("city"),
            "founded_year":  _safe_int(team_data.get("founded"), "founded_year"),
            "stadium_name":  venue_data.get("name"),
            "season":        SEASON,
            "rank":          _safe_int(entry.get("rank"), "rank"),
            "played":        _safe_int(all_stats.get("played"), "played"),
            "wins":          _safe_int(all_stats.get("win"), "wins"),
            "draws":         _safe_int(all_stats.get("draw"), "draws"),
            "losses":        _safe_int(all_stats.get("lose"), "losses"),
            "goals_for":     _safe_int(goals.get("for"), "goals_for"),
            "goals_against": _safe_int(goals.get("against"), "goals_against"),
            "points":        _safe_int(entry.get("points"), "points"),
            "source_api":    "api_sports",
            "ingested_at":   ingested_at,
        }

        # Warn on missing optional fields
        optional_missing = [k for k, v in record.items() if v is None and k not in ("season", "source_api", "ingested_at")]
        if optional_missing:
            logger.warning(f"Team '{team_name}' has None values for: {optional_missing}")

        records.append(record)

    logger.info(f"API-Sports transformer: {len(records)}/{len(standings)} records transformed successfully")
    return records
