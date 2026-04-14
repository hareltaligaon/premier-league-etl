from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from src.config import GCP_PROJECT_ID, BQ_DATASET, GCP_CREDENTIALS_PATH
from src.logger import get_logger

TABLE = "pipeline_runs"

SCHEMA = [
    bigquery.SchemaField("run_at", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("records_loaded", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("errors", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("warnings", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("duration_seconds", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
]


class MetricsLogger:
    def __init__(self):
        self.logger = get_logger("metrics")

    def _get_client(self):
        credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
        return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

    def log(self, source, records_loaded, errors, warnings, duration_seconds):
        status = "failure" if errors > 0 else "success"
        row = {
            "run_at": datetime.utcnow().isoformat(),
            "source": source,
            "records_loaded": records_loaded,
            "errors": errors,
            "warnings": warnings,
            "duration_seconds": round(duration_seconds, 2),
            "status": status,
        }

        try:
            client = self._get_client()
            table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{TABLE}"
            job_config = bigquery.LoadJobConfig(
                schema=SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            job = client.load_table_from_json([row], table_ref, job_config=job_config)
            job.result()
            self.logger.info(f"Metrics logged for '{source}': {records_loaded} records, {duration_seconds:.2f}s, status={status}")
        except Exception as e:
            self.logger.error(f"Failed to log metrics for '{source}': {e}")
