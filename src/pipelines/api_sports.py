from src.extractors.api_sports import ApiSportsExtractor
from src.transformers.api_sports import transform
from src.loaders.sqlite_loader import load, export_csv
from src.logger import get_logger

TABLE = "standings_api_sports"


class ApiSportsPipeline:

    def __init__(self):
        self.logger = get_logger("pipeline.api_sports")
        self.extractor = ApiSportsExtractor()

    def run(self):
        self.logger.info("--- Source: API-Sports ---")
        raw = self.extractor.fetch()
        records = transform(raw)
        load(records, TABLE)
        export_csv(TABLE)
