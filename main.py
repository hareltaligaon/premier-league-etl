from src.logger import get_logger
from src.extractors.api_sports import fetch as fetch_api_sports
from src.extractors.api_football import fetch as fetch_api_football

logger = get_logger("main")


def run():
    logger.info("=== Premier League ETL Pipeline started ===")

    # --- API-Sports ---
    logger.info("--- Source: API-Sports ---")
    api_sports_data = fetch_api_sports()
    # TODO: transform + load (step 3 & 4)

    # --- API-Football ---
    logger.info("--- Source: API-Football ---")
    api_football_data = fetch_api_football()
    # TODO: transform + load (step 3 & 4)

    logger.info("=== ETL Pipeline complete ===")


if __name__ == "__main__":
    run()
