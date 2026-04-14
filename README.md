# Premier League ETL Pipeline

A pipeline that pulls Premier League standings and team data from two football APIs, normalises it into a single schema, stores it in SQLite or BigQuery, and exposes it via a REST API.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Pipeline Design](#pipeline-design)
3. [Tech Stack](#tech-stack)
4. [Unified Schema](#unified-schema)
5. [Schema Decisions](#schema-decisions)
6. [Error Handling & Alerting](#error-handling--alerting)
7. [Assumptions](#assumptions)
8. [Scheduling](#scheduling)
9. [How to Run](#how-to-run)
10. [REST API Endpoints](#rest-api-endpoints)
11. [Dashboard](#dashboard)

---

## Architecture

```
[API-Sports]                    [API-Football]
     |                                |
ApiSportsExtractor          ApiFootballExtractor
  (fetch_standings)           (fetch_standings)
  (fetch_teams)               (fetch_teams)
     |                                |
api_sports.transform()      api_football.transform()
     |                                |
     v                                v
  get_loader()                  get_loader()
     |                                |
     +------------ LOADER env --------+
     |                                |
  sqlite_loader       OR         bigquery_loader
     |                                |
[standings_api_sports]    [standings_api_football]
  (SQLite or BigQuery — separate tables, one per source)
              |                        |
           FastAPI               MetricsLogger
              |                  (LOADER=bigquery)
    /standings/api-sports              |
    /standings/api-football      [pipeline_runs]
    /standings/{source}/{team}   (BigQuery — one row per run)
```

Each API has its own extract → transform → load path. The two sources are never merged — every row in the DB knows exactly where it came from. The loader is selected at runtime via the `LOADER` environment variable — switching from SQLite to BigQuery requires changing one line in `.env`.

---

## Pipeline Design

### Module Structure

```
premier-league-etl/
├── main.py                      # Entry point — runs both pipelines, prints summary
├── schema.sql                   # SQL DDL for both tables
├── data/                        # SQLite database file (premier_league.db)
├── output/                      # CSV exports — one file per source (standings_api_sports.csv, standings_api_football.csv)
├── logs/                        # Full pipeline run log (etl.log)
└── src/
    ├── config.py                # Env variables, paths, constants
    ├── logger.py                # Logger setup (file + console)
    ├── error_collector.py       # Collects errors/warnings, prints run summary
    ├── extractors/
    │   ├── base.py              # Shared HTTP + retry logic
    │   ├── api_sports.py        # Fetches /standings and /teams from API-Sports
    │   └── api_football.py      # Fetches standings and teams from API-Football
    ├── transformers/
    │   ├── base.py              # Shared utility methods (_safe_int, _build_teams_lookup)
    │   ├── api_sports.py        # Maps API-Sports fields → unified schema
    │   └── api_football.py      # Maps API-Football fields → unified schema
    ├── loaders/
    │   ├── sqlite_loader.py     # Writes to SQLite, exports CSV
    │   ├── bigquery_loader.py   # Writes to BigQuery, exports CSV
    │   └── factory.py           # Returns the correct loader based on LOADER env var
    ├── pipelines/
    │   ├── api_sports.py        # Runs Extract→Transform→Load for API-Sports
    │   └── api_football.py      # Runs Extract→Transform→Load for API-Football
    ├── metrics_logger.py        # Logs pipeline run metrics to BigQuery (pipeline_runs table)
    └── api/
        └── app.py               # FastAPI endpoints
```

### Execution Flow

`main.py` creates a single `ErrorCollector` instance and passes it to both pipeline classes:

```python
collector = ErrorCollector()
metrics = MetricsLogger() if LOADER == "bigquery" else None

ApiSportsPipeline(collector, metrics).run()
ApiFootballPipeline(collector, metrics).run()

collector.print_summary()
```

Each pipeline class (`ApiSportsPipeline`, `ApiFootballPipeline`) follows the same structure in its `run()` method:

1. **Extract** — calls `self.extractor.fetch()`, which hits both the `standings` and `teams` endpoints and returns the raw JSON. If no standings come back, `collector.add_error()` is called and the pipeline stops early for that source (but the other source still runs).
2. **Transform** — passes the raw data to the transformer, which validates required fields, maps source-specific field names to the unified schema, and converts types. If the transformer returns nothing, `collector.add_error()` is called and the pipeline stops early.
3. **Load** — calls `get_loader()` to get the active loader, then `loader.load(records, TABLE)` to write the data, then `loader.export_csv(TABLE)` to write the CSV.
4. **Metrics** — if `LOADER=bigquery`, logs the run metrics (duration, record count, errors, status) to the `pipeline_runs` table in BigQuery via `MetricsLogger`.

### Loader Factory

`src/loaders/factory.py` reads the `LOADER` environment variable and returns the appropriate loader module:

```python
# LOADER=sqlite   → uses sqlite_loader  (default, no extra setup needed)
# LOADER=bigquery → uses bigquery_loader (requires GCP credentials)
```

Both loaders expose the same interface — `load(records, table)` and `export_csv(table)` — so the pipelines don't need to know which one is active.

### Re-runs

- **SQLite**: primary key is `(team_id, season)`, so re-running updates existing rows instead of creating duplicates.
- **BigQuery**: uses `WRITE_TRUNCATE`, so the table is fully replaced on each run.

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | Python 3.x | The standard choice for data pipelines — good support for HTTP, SQLite, and API frameworks all in one place |
| Storage (default) | SQLite | No server needed, runs as a single file, easy to inspect locally. Chose it over Postgres because this is a single-process pipeline with no concurrency requirements |
| Storage (cloud) | BigQuery | Google Cloud's managed data warehouse — serverless, no infrastructure to manage, and the natural choice for the GCP ecosystem. Swapping to it only requires changing `LOADER=bigquery` in `.env` |
| REST API | FastAPI | Chose it over Flask because it includes request validation and type hints out of the box, and auto-generates the `/docs` page without any extra work |
| HTTP | requests | Chose it over httpx because the pipeline is synchronous — there's no benefit to async here, and the retry logic with `time.sleep` is straightforward to implement |
| Config | python-dotenv | Keeps API keys out of the code and out of version control |
| Logging | Python `logging` | Built-in, no extra dependency. Configured to write to both `logs/etl.log` and the console at the same time with a single setup |

---

## Unified Schema

Defined in `schema.sql`. Both tables share the same structure.

| Field | Type | Nullable | Source |
|-------|------|----------|--------|
| team_id | TEXT | No (PK) | teams endpoint |
| team_name | TEXT | No | teams endpoint |
| country | TEXT | Yes | teams endpoint |
| city | TEXT | Yes | venue data |
| founded_year | INTEGER | Yes | teams endpoint |
| stadium_name | TEXT | Yes | venue data |
| season | INTEGER | No | config |
| rank | INTEGER | Yes | standings endpoint |
| played | INTEGER | Yes | standings endpoint |
| wins | INTEGER | Yes | standings endpoint |
| draws | INTEGER | Yes | standings endpoint |
| losses | INTEGER | Yes | standings endpoint |
| goals_for | INTEGER | Yes | standings endpoint |
| goals_against | INTEGER | Yes | standings endpoint |
| points | INTEGER | Yes | standings endpoint |
| source_api | TEXT | No | metadata |
| ingested_at | TEXT | No | metadata |
| schema_version | INTEGER | No | versioning |
| extra_fields | TEXT (JSON) | Yes | schema evolution |

---

## Schema Decisions

### Why these fields?

- **team_id** — needed for the primary key so upserts work correctly, and to join standings data with team info.
- **team_name** — the only truly required field. A record with no name is useless, so it gets skipped.
- **country, city, founded_year, stadium_name** — general team info from the `teams` endpoint. The task asked for this kind of context alongside the standings data.
- **season** — makes it possible to store multiple seasons in the same table in the future without mixing them up.
- **rank, played, wins, draws, losses, goals_for, goals_against, points** — the standard performance data from the `standings` endpoint.
- **source_api** — records where each row came from. Important because both tables have the same columns, so without this field you'd lose track of the source.
- **ingested_at** — timestamp of when the data was fetched, useful if you ever need to know how fresh the data is.
- **schema_version** — integer that increments when the standard schema changes. Allows distinguishing rows loaded under different schema versions in the same table.
- **extra_fields** — any fields returned by the API that are not part of the standard schema are stored here as a JSON string. This means the pipeline will never break if a source adds new fields in the future — they get preserved automatically instead of being silently dropped.

### Handling mismatches between the two APIs

Both APIs provide the same information but under completely different field names. The transformers handle the mapping so everything downstream is consistent.

| Concept | API-Sports field | API-Football field |
|---------|------------------|--------------------|
| Table position | `rank` | `overall_league_position` |
| Games played | `all.played` | `overall_league_payed` *(typo in the API)* |
| Wins | `all.win` | `overall_league_W` |
| Draws | `all.draw` | `overall_league_D` |
| Losses | `all.lose` | `overall_league_L` |
| Goals scored | `all.goals.for` | `overall_league_GF` |
| Goals conceded | `all.goals.against` | `overall_league_GA` |
| Points | `points` | `overall_league_PTS` |
| Stadium name | `venue.name` | `venue.venue_name` |
| City | `venue.city` | `venue.venue_city` |
| Founded year | `team.founded` | `team_founded` |

If a field exists in one API but is missing from the other, it gets stored as `NULL` and a `WARNING` is logged. Records are only skipped if a required field (`team_name`, `rank`, `points`, `team_id`) is missing entirely.

---

## Error Handling & Alerting

### Retries

Every HTTP request is retried up to 3 times with exponential backoff (2s, then 4s). Timeouts trigger a retry. HTTP errors (4xx/5xx) are logged and the request is not retried since retrying won't fix them.

### What happens in each failure case

| Scenario | What happens |
|----------|-------------|
| API timeout | Retry up to 3 times, then log `ERROR` |
| HTTP error (4xx/5xx) | Log `ERROR`, return empty result, continue |
| Missing required field | Skip that record, log `ERROR` |
| Missing optional field | Store `NULL`, log `WARNING` |
| No standings returned at all | Log `ERROR` via `ErrorCollector`, move to next source |
| No teams returned | Log `WARNING`, team fields stored as `NULL` |
| DB write error | Log `ERROR`, pipeline continues |

### ErrorCollector

At the start of the run, `main.py` creates an `ErrorCollector` that gets passed to both pipelines. Any error or warning gets added to it during the run.

- **`add_error(message)`** — logs the error and saves it to the list.
- **`add_warning(message)`** — same but at warning level.
- **`print_summary()`** — called once at the very end, prints how many errors and warnings occurred and lists all the errors.

All logs go to `logs/etl.log` and are printed to the console at the same time.

**Example summary at end of run:**
```
INFO  Pipeline run summary
INFO    Errors:   0
INFO    Warnings: 3
INFO    No issues detected
```

---

## Assumptions

- Season **2023** (2023/24) is used for both APIs.
- API-Sports league_id `39` is the Premier League.
- API-Football league_id `152` is the Premier League.
- The free tier is enough for one full run (~20 teams per API).
- `city` and `stadium_name` come from the `teams` endpoint. If a team isn't returned there, those fields will be `NULL`.
- API-Football has a typo in their field name (`overall_league_payed` instead of `played`) — the transformer accounts for this.

---

## Scheduling

The pipeline can be scheduled to run automatically using **Google Cloud Scheduler**.

### How to set it up

1. Go to [Cloud Scheduler](https://console.cloud.google.com/cloudscheduler) in GCP
2. Click **"Create Job"**
3. Set the schedule — for example, every day at 6:00 AM:
   ```
   0 6 * * *
   ```
4. Set the target to run `python main.py` via a Cloud Run Job or a Pub/Sub trigger

### Example cron expressions

| Schedule | Expression |
|----------|------------|
| Every day at 6:00 AM | `0 6 * * *` |
| Every Monday at 8:00 AM | `0 8 * * 1` |
| Every hour | `0 * * * *` |

---

## How to Run

### 1. Get API Keys

- Register at [api-sports.io](https://api-sports.io) — 100 free requests/day
- Register at [apifootball.com](https://apifootball.com) — free tier available

### 2. Setup

```bash
git clone https://github.com/hareltaligaon/premier-league-etl
cd premier-league-etl
python -m venv venv
source venv/Scripts/activate   # Windows
pip install -r requirements.txt
cp .env.example .env
# Edit .env and fill in your API keys
```

### 3. Choose a Loader

In `.env`, set the `LOADER` variable:

```bash
LOADER=sqlite    # default — stores data locally in data/premier_league.db
LOADER=bigquery  # stores data in Google BigQuery
```

**For BigQuery**, also set:
```bash
GCP_PROJECT_ID=your_gcp_project_id
BQ_DATASET=premier_league
GCP_CREDENTIALS_PATH=gcp-credentials.json  # path to your service account key
```

### 4. Run ETL Pipeline

```bash
python main.py
```

**Output:**
- `data/premier_league.db` — SQLite database (if `LOADER=sqlite`)
- BigQuery tables `standings_api_sports`, `standings_api_football`, and `pipeline_runs` (if `LOADER=bigquery`)
- `output/standings_api_sports.csv` — 20 teams, 17 fields
- `output/standings_api_football.csv` — 20 teams, 17 fields
- `logs/etl.log` — full log of the run

### 5. Start REST API

```bash
uvicorn src.api.app:app --reload
```

Open [http://localhost:8000/docs](http://localhost:8000/docs) for interactive docs.

---

## REST API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Returns `{"status": "ok"}` |
| GET | `/standings/api-sports` | All teams from API-Sports, sorted by rank |
| GET | `/standings/api-football` | All teams from API-Football, sorted by rank |
| GET | `/standings/api-sports/{team}` | Search by team name (case-insensitive) |
| GET | `/standings/api-football/{team}` | Search by team name (case-insensitive) |

---

## Dashboard

A live Looker Studio dashboard connected directly to BigQuery, showing:
- Full standings table (rank, points, wins, draws, losses)
- Points per team (bar chart)
- Wins / Draws / Losses per team (stacked bar chart)
- Pipeline monitoring table (run history, duration, errors, status)
- Pipeline duration per source (bar chart)

link: https://lookerstudio.google.com/reporting/9bb259a0-735c-44f1-99f1-d9de8390fb94
