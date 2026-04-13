from src.extractors.api_football import ApiFootballExtractor
from src.transformers.api_football import transform
from src.loaders.sqlite_loader import load, export_csv
from src.logger import get_logger

TABLE = "standings_api_football"


class ApiFootballPipeline:

    def __init__(self, collector):
        self.logger = get_logger("pipeline.api_football")
        self.extractor = ApiFootballExtractor()
        self.collector = collector

    def run(self):
        self.logger.info("Source: API-Football")
        raw = self.extractor.fetch()

        if not raw["standings"]:
            self.collector.add_error("API-Football: no standings data received")
            return
        if not raw["teams"]:
            self.collector.add_warning("API-Football: no teams data received")

        records = transform(raw)

        if not records:
            self.collector.add_error("API-Football: transformer returned no records")
            return

        load(records, TABLE)
        export_csv(TABLE)
