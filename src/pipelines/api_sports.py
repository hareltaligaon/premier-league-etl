from src.extractors.api_sports import ApiSportsExtractor
from src.transformers.api_sports import transform
from src.loaders.sqlite_loader import load, export_csv
from src.logger import get_logger

TABLE = "standings_api_sports"


class ApiSportsPipeline:

    def __init__(self, collector):
        self.logger = get_logger("pipeline.api_sports")
        self.extractor = ApiSportsExtractor()
        self.collector = collector

    def run(self):
        self.logger.info("Source: API-Sports")
        raw = self.extractor.fetch()

        if not raw["standings"]:
            self.collector.add_error("API-Sports: no standings data received")
            return
        if not raw["teams"]:
            self.collector.add_warning("API-Sports: no teams data received")

        records = transform(raw)

        if not records:
            self.collector.add_error("API-Sports: transformer returned no records")
            return

        load(records, TABLE)
        export_csv(TABLE)
