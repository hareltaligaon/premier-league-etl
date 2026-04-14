from src.pipelines.api_sports import ApiSportsPipeline
from src.pipelines.api_football import ApiFootballPipeline
from src.error_collector import ErrorCollector
from src.metrics_logger import MetricsLogger
from src.config import LOADER
from src.logger import get_logger

logger = get_logger("main")


def main():
    logger.info("Premier League ETL Pipeline started")
    collector = ErrorCollector()
    metrics = MetricsLogger() if LOADER == "bigquery" else None

    ApiSportsPipeline(collector, metrics).run()
    ApiFootballPipeline(collector, metrics).run()

    collector.print_summary()
    logger.info("ETL Pipeline complete")


if __name__ == "__main__":
    main()
