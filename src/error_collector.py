from src.logger import get_logger

logger = get_logger("error_collector")


class ErrorCollector:
    """
    Collects errors and warnings throughout the pipeline run
    and prints a summary at the end.
    """

    def __init__(self):
        self.errors = []
        self.warnings = []

    def add_error(self, message):
        logger.error(message)
        self.errors.append(message)

    def add_warning(self, message):
        logger.warning(message)
        self.warnings.append(message)

    def print_summary(self):
        logger.info("Pipeline run summary")
        logger.info(f"Errors: {len(self.errors)}")
        logger.info(f"Warnings: {len(self.warnings)}")

        if self.errors:
            logger.error("Errors encountered during run:")
            for i, err in enumerate(self.errors, 1):
                logger.error(f"{i}. {err}")

        if not self.errors and not self.warnings:
            logger.info("No issues detected")
