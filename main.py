from src.pipelines.api_sports import ApiSportsPipeline
from src.pipelines.api_football import ApiFootballPipeline
from src.logger import get_logger

logger = get_logger("main")


def main():
    logger.info("Premier League ETL Pipeline started")
    ApiSportsPipeline().run()
    ApiFootballPipeline().run()
    logger.info("ETL Pipeline complete")


if __name__ == "__main__":
    main()
