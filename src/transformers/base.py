import json
from src.logger import get_logger

SCHEMA_VERSION = 1


class BaseTransformer:
    """
    Base class for all transformers.
    Provides shared utility methods — subclasses implement transform() and _get_team_key().

    Schema evolution:
    - schema_version: incremented when the standard schema changes, so old and new rows can be distinguished.
    - extra_fields: any fields returned by the API that are not part of the standard schema are stored
      here as JSON. This ensures the pipeline never breaks if a source adds new fields in the future.
    """

    def __init__(self, source_name):
        self.logger = get_logger(f"transformer.{source_name}")

    def _get_team_key(self, entry) -> str:
        """Override to extract the team key from a teams entry."""
        raise NotImplementedError

    def _build_teams_lookup(self, teams) -> dict:
        lookup = {}
        for entry in teams:
            try:
                lookup[self._get_team_key(entry)] = entry
            except (KeyError, TypeError):
                self.logger.warning("Could not index team entry")
        return lookup

    def _safe_int(self, value, field):
        try:
            return int(value)
        except (TypeError, ValueError):
            self.logger.warning(f"Could not convert '{field}' value '{value}' to int")
            return None

    def _collect_extra_fields(self, entry, mapped_fields) -> str:
        """Collects any fields from the API entry that are not part of the standard schema."""
        extra = {k: v for k, v in entry.items() if k not in mapped_fields}
        return json.dumps(extra) if extra else None

    def transform(self, raw) -> list:
        raise NotImplementedError
