from src.logger import get_logger
from src.extractors.api_sports import fetch as fetch_api_sports
from src.extractors.api_football import fetch as fetch_api_football
from src.transformers.api_sports import transform as transform_api_sports
from src.transformers.api_football import transform as transform_api_football
from src.loaders.sqlite_loader import load, export_csv

logger = get_logger("main")


def run():
    logger.info("=== Premier League ETL Pipeline started ===")

    # --- API-Sports ---
    logger.info("--- Source: API-Sports ---")
    raw_sports = fetch_api_sports()
    records_sports = transform_api_sports(raw_sports)
    load(records_sports, "standings_api_sports")
    export_csv("standings_api_sports")

    # --- API-Football ---
    logger.info("--- Source: API-Football ---")
    raw_football = fetch_api_football()
    records_football = transform_api_football(raw_football)
    load(records_football, "standings_api_football")
    export_csv("standings_api_football")

    logger.info("=== ETL Pipeline complete ===")


if __name__ == "__main__":
    run()
