# Premier League ETL Pipeline

An ETL pipeline that ingests, standardises, stores and exposes Premier League standings data from two external football APIs.

---

## Architecture

```
[API-Sports]       [API-Football]
     |                   |
  Extractor           Extractor
     |                   |
  Transformer         Transformer
     |                   |
     v                   v
[standings_api_sports] [standings_api_football]
        (SQLite DB — separate tables)
             |
          FastAPI
             |
      REST endpoints
```

**Flow:** Each API has its own Extract → Transform → Load pipeline. Data is kept in separate tables by design, so the source is always traceable.

---

## Tech Stack

| Component | Technology | Reason |
|-----------|-----------|--------|
| Language | Python 3.x | Standard for data engineering |
| Storage | SQLite | Zero-setup, portable, easily swappable for BigQuery |
| REST API | FastAPI | Fast to build, auto-generates docs at /docs |
| HTTP | requests | Simple and battle-tested |
| Config | python-dotenv | Keeps secrets out of code |
| Logging | Python logging | File + console output |

> **BigQuery note:** The loader layer is designed to be swappable. To use BigQuery, replace `sqlite_loader.py` with a BigQuery loader using `google-cloud-bigquery` — the transformer output format stays the same.

---

## Unified Schema (17 fields)

| Field | Type | Source |
|-------|------|--------|
| team_id | TEXT | teams endpoint |
| team_name | TEXT | teams endpoint |
| country | TEXT | teams endpoint |
| city | TEXT | teams endpoint |
| founded_year | INTEGER | teams endpoint |
| stadium_name | TEXT | teams endpoint |
| season | INTEGER | config |
| rank | INTEGER | standings endpoint |
| played | INTEGER | standings endpoint |
| wins | INTEGER | standings endpoint |
| draws | INTEGER | standings endpoint |
| losses | INTEGER | standings endpoint |
| goals_for | INTEGER | standings endpoint |
| goals_against | INTEGER | standings endpoint |
| points | INTEGER | standings endpoint |
| source_api | TEXT | metadata |
| ingested_at | DATETIME | metadata |

**Schema decisions:**
- Fields chosen to cover both generic team info and season performance as required
- `source_api` field keeps data traceable to its origin
- `PRIMARY KEY (team_id, season)` enables safe re-runs via upsert
- Fields that exist in one API but not the other are stored as NULL and logged as WARNING

---

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| API timeout | Retry up to 3 times with exponential backoff |
| HTTP error | Log ERROR, return empty list, continue pipeline |
| Missing required field (team_name, rank, points) | Skip record, log ERROR |
| Missing optional field | Store NULL, log WARNING |
| DB error | Log ERROR, pipeline continues with next source |

All logs written to `logs/etl.log` and printed to console.

---

## How to Run

### 1. Get API Keys
- Register at [api-sports.io](https://api-sports.io) — 100 free requests/day
- Register at [apifootball.com](https://apifootball.com) — free tier

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

### 3. Run ETL Pipeline

```bash
python main.py
```

Output:
- `data/premier_league.db` — SQLite database with two tables
- `output/standings_api_sports.csv`
- `output/standings_api_football.csv`
- `logs/etl.log`

### 4. Start REST API

```bash
uvicorn src.api.app:app --reload
```

Open [http://localhost:8000/docs](http://localhost:8000/docs) for interactive API docs.

#### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| GET | /standings/api-sports | All teams from API-Sports |
| GET | /standings/api-football | All teams from API-Football |
| GET | /standings/api-sports/{team} | Search by team name |
| GET | /standings/api-football/{team} | Search by team name |

---

## Assumptions

- Season 2023 (2023/24) is used for both APIs
- API-Football league_id=152 maps to the English Premier League
- API-Sports league_id=39 maps to the English Premier League
- Free tier limits are sufficient for a single full pipeline run
