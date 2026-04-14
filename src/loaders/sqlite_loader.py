import csv
import os
import sqlite3
from src.config import DB_PATH, OUTPUT_DIR
from src.logger import get_logger

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    team_id       TEXT,
    team_name     TEXT    NOT NULL,
    country       TEXT,
    city          TEXT,
    founded_year  INTEGER,
    stadium_name  TEXT,
    season        INTEGER NOT NULL,
    rank          INTEGER,
    played        INTEGER,
    wins          INTEGER,
    draws         INTEGER,
    losses        INTEGER,
    goals_for     INTEGER,
    goals_against INTEGER,
    points        INTEGER,
    source_api     TEXT    NOT NULL,
    ingested_at    TEXT    NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    extra_fields   TEXT,
    PRIMARY KEY (team_id, season)
);
"""

COLUMNS = [
    "team_id", "team_name", "country", "city", "founded_year",
    "stadium_name", "season", "rank", "played", "wins", "draws",
    "losses", "goals_for", "goals_against", "points", "source_api", "ingested_at",
    "schema_version", "extra_fields",
]

UPSERT_SQL = """
INSERT INTO {table} ({cols})
VALUES ({placeholders})
ON CONFLICT(team_id, season) DO UPDATE SET
    team_name=excluded.team_name, country=excluded.country,
    city=excluded.city, founded_year=excluded.founded_year,
    stadium_name=excluded.stadium_name, rank=excluded.rank,
    played=excluded.played, wins=excluded.wins, draws=excluded.draws,
    losses=excluded.losses, goals_for=excluded.goals_for,
    goals_against=excluded.goals_against, points=excluded.points,
    source_api=excluded.source_api, ingested_at=excluded.ingested_at,
    schema_version=excluded.schema_version, extra_fields=excluded.extra_fields;
"""


class SQLiteLoader:
    def __init__(self):
        self.logger = get_logger("loader.sqlite")

    def _get_connection(self):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        return sqlite3.connect(DB_PATH)

    def load(self, records, table):
        if not records:
            self.logger.warning(f"No records to load into '{table}'")
            return

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(CREATE_TABLE_SQL.format(table=table))

            cols = ", ".join(COLUMNS)
            placeholders = ", ".join(["?"] * len(COLUMNS))
            sql = UPSERT_SQL.format(table=table, cols=cols, placeholders=placeholders)

            rows = [tuple(r.get(col) for col in COLUMNS) for r in records]
            cursor.executemany(sql, rows)
            conn.commit()
            self.logger.info(f"Loaded {len(rows)} records into '{table}'")
        except sqlite3.Error as e:
            self.logger.error(f"Database error while loading '{table}': {e}")
        finally:
            conn.close()

    def export_csv(self, table):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        csv_path = os.path.join(OUTPUT_DIR, f"{table}.csv")

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(col_names)
                writer.writerows(rows)

            self.logger.info(f"Exported {len(rows)} rows to '{csv_path}'")
        except sqlite3.Error as e:
            self.logger.error(f"Database error while exporting '{table}': {e}")
        finally:
            conn.close()
