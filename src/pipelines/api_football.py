import time
from src.extractors.api_football import ApiFootballExtractor
from src.transformers.api_football import ApiFootballTransformer
from src.loaders.factory import get_loader
from src.logger import get_logger

TABLE = "standings_api_football"


class ApiFootballPipeline:

    def __init__(self, collector, metrics_logger=None):
        self.logger = get_logger("pipeline.api_football")
        self.extractor = ApiFootballExtractor()
        self.transformer = ApiFootballTransformer()
        self.collector = collector
        self.metrics_logger = metrics_logger

    def run(self):
        self.logger.info("Source: API-Football")
        start = time.time()

        raw = self.extractor.fetch()

        if not raw["standings"]:
            self.collector.add_error("API-Football: no standings data received")
            self._log_metrics(0, start)
            return
        if not raw["teams"]:
            self.collector.add_warning("API-Football: no teams data received")

        records = self.transformer.transform(raw)

        if not records:
            self.collector.add_error("API-Football: transformer returned no records")
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
            source="api_football",
            records_loaded=records_loaded,
            errors=len(self.collector.errors),
            warnings=len(self.collector.warnings),
            duration_seconds=time.time() - start,
        )
