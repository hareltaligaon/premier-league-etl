-- Premier League ETL — Schema Definition
-- Two identical tables, one per data source, keeping sources separate as required.

CREATE TABLE IF NOT EXISTS standings_api_sports (
    team_id        TEXT,
    team_name      TEXT    NOT NULL,
    country        TEXT,
    city           TEXT,
    founded_year   INTEGER,
    stadium_name   TEXT,
    season         INTEGER NOT NULL,
    rank           INTEGER,
    played         INTEGER,
    wins           INTEGER,
    draws          INTEGER,
    losses         INTEGER,
    goals_for      INTEGER,
    goals_against  INTEGER,
    points         INTEGER,
    source_api     TEXT    NOT NULL,
    ingested_at    TEXT    NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    extra_fields   TEXT,
    PRIMARY KEY (team_id, season)
);

CREATE TABLE IF NOT EXISTS standings_api_football (
    team_id        TEXT,
    team_name      TEXT    NOT NULL,
    country        TEXT,
    city           TEXT,
    founded_year   INTEGER,
    stadium_name   TEXT,
    season         INTEGER NOT NULL,
    rank           INTEGER,
    played         INTEGER,
    wins           INTEGER,
    draws          INTEGER,
    losses         INTEGER,
    goals_for      INTEGER,
    goals_against  INTEGER,
    points         INTEGER,
    source_api     TEXT    NOT NULL,
    ingested_at    TEXT    NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    extra_fields   TEXT,
    PRIMARY KEY (team_id, season)
);
