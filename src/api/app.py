import sqlite3
import os
from fastapi import FastAPI, HTTPException
from src.config import DB_PATH
from src.logger import get_logger

logger = get_logger("api")

app = FastAPI(
    title="Premier League ETL API",
    description="Exposes Premier League standings from two sources: API-Sports and API-Football",
    version="1.0.0",
)

TABLES = {
    "api-sports": "standings_api_sports",
    "api-football": "standings_api_football",
}


def _query(sql, params=()):
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=503, detail="Database not found. Run main.py first.")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows
    except sqlite3.Error as e:
        logger.error(f"DB query error: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/standings/{source}")
def get_standings(source):
    if source not in TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown source '{source}'. Use: {list(TABLES.keys())}")
    table = TABLES[source]
    rows = _query(f"SELECT * FROM {table} ORDER BY rank ASC")
    logger.info(f"GET /standings/{source} → {len(rows)} records")
    return {"source": source, "count": len(rows), "data": rows}


@app.get("/standings/{source}/{team_name}")
def get_team(source, team_name):
    if source not in TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown source '{source}'. Use: {list(TABLES.keys())}")
    table = TABLES[source]
    rows = _query(f"SELECT * FROM {table} WHERE LOWER(team_name) LIKE LOWER(?)", (f"%{team_name}%",))
    if not rows:
        raise HTTPException(status_code=404, detail=f"Team '{team_name}' not found in {source}")
    logger.info(f"GET /standings/{source}/{team_name} → {len(rows)} records")
    return {"source": source, "count": len(rows), "data": rows}
