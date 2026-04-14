import os
from dotenv import load_dotenv

load_dotenv()

API_SPORTS_KEY = os.getenv("API_SPORTS_KEY", "")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")

SEASON = int(os.getenv("SEASON", "2023"))
API_SPORTS_LEAGUE_ID = int(os.getenv("API_SPORTS_LEAGUE_ID", "39"))
API_FOOTBALL_LEAGUE_ID = int(os.getenv("API_FOOTBALL_LEAGUE_ID", "152"))

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "premier_league.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

# Loader: "sqlite" or "bigquery"
LOADER = os.getenv("LOADER", "sqlite")

# BigQuery
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET = os.getenv("BQ_DATASET", "premier_league")
GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH", "gcp-credentials.json")
