import time
from src.extractors.api_sports import ApiSportsExtractor
from src.transformers.api_sports import ApiSportsTransformer
from src.loaders.factory import get_loader
from src.logger import get_logger

TABLE = "standings_api_sports"


class ApiSportsPipeline:

    def __init__(self, collector, metrics_logger=None):
        self.logger = get_logger("pipeline.api_sports")
        self.extractor = ApiSportsExtractor()
        self.transformer = ApiSportsTransformer()
        self.collector = collector
        self.metrics_logger = metrics_logger

    def run(self):
        self.logger.info("Source: API-Sports")
        start = time.time()

        raw = self.extractor.fetch()

        if not raw["standings"]:
            self.collector.add_error("API-Sports: no standings data received")
            self._log_metrics(0, start)
            return
        if not raw["teams"]:
            self.collector.add_warning("API-Sports: no teams data received")

        records = self.transformer.transform(raw)

        if not records:
            self.collector.add_error("API-Sports: transformer returned no records")
            self._log_metrics(0, start)
            return

        loader = get_loader()
        loader.load(records, TABLE)
        loader.export_csv(TABLE)

        self._log_metrics(len(records), start)

    def _log_metrics(self, records_loaded, start):
        if not self.metrics_logger:
            return
        self.metrics_logger.log(
            source="api_sports",
            records_loaded=records_loaded,
            errors=len(self.collector.errors),
            warnings=len(self.collector.warnings),
            duration_seconds=time.time() - start,
        )
