from src.extractors.api_football import ApiFootballExtractor
from src.transformers.api_football import transform
from src.loaders.sqlite_loader import load, export_csv
from src.logger import get_logger

TABLE = "standings_api_football"


class ApiFootballPipeline:

    def __init__(self):
        self.logger = get_logger("pipeline.api_football")
        self.extractor = ApiFootballExtractor()

    def run(self):
        self.logger.info("Source: API-Football")
        raw = self.extractor.fetch()
        records = transform(raw)
        load(records, TABLE)
        export_csv(TABLE)
