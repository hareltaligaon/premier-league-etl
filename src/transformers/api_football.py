from datetime import datetime
from src.config import SEASON
from src.logger import get_logger

logger = get_logger("transformer.api_football")

REQUIRED_STANDING_FIELDS = ["team_id", "team_name", "overall_league_position", "overall_league_PTS"]


def _build_teams_lookup(teams) -> dict:
    lookup = {}
    for entry in teams:
        try:
            team_key = str(entry["team_key"])
            lookup[team_key] = entry
        except (KeyError, TypeError):
            logger.warning(f"Could not index team entry: {entry}")
    return lookup


def _safe_int(value, field):
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(f"Could not convert '{field}' value '{value}' to int")
        return None


def transform(raw) -> list:
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

        team_id = str(entry["team_id"])
        team_name = entry["team_name"]

        # Get team info
        team_info = teams_lookup.get(team_id, {})
        venue = team_info.get("venue", {})

        if not team_info:
            logger.warning(f"No team info found for team_id={team_id} ({team_name})")

        record = {
            "team_id":       team_id,
            "team_name":     team_name,
            "country":       team_info.get("team_country"),
            "city":          venue.get("venue_city"),
            "founded_year":  _safe_int(team_info.get("team_founded"), "founded_year"),
            "stadium_name":  venue.get("venue_name"),
            "season":        SEASON,
            "rank":          _safe_int(entry.get("overall_league_position"), "rank"),
            "played":        _safe_int(entry.get("overall_league_payed"), "played"),
            "wins":          _safe_int(entry.get("overall_league_W"), "wins"),
            "draws":         _safe_int(entry.get("overall_league_D"), "draws"),
            "losses":        _safe_int(entry.get("overall_league_L"), "losses"),
            "goals_for":     _safe_int(entry.get("overall_league_GF"), "goals_for"),
            "goals_against": _safe_int(entry.get("overall_league_GA"), "goals_against"),
            "points":        _safe_int(entry.get("overall_league_PTS"), "points"),
            "source_api":    "api_football",
            "ingested_at":   ingested_at,
        }

        # Warn on missing optional fields
        optional_missing = [k for k, v in record.items() if v is None and k not in ("season", "source_api", "ingested_at")]
        if optional_missing:
            logger.warning(f"Team '{team_name}' has None values for: {optional_missing}")

        records.append(record)

    logger.info(f"API-Football transformer: {len(records)}/{len(standings)} records transformed successfully")
    return records
