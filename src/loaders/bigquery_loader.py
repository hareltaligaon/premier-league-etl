import csv
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from src.config import GCP_PROJECT_ID, BQ_DATASET, GCP_CREDENTIALS_PATH, OUTPUT_DIR
from src.logger import get_logger

SCHEMA = [
    bigquery.SchemaField("team_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("team_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("founded_year", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("stadium_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("season", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("rank", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("played", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("wins", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("draws", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("losses", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("goals_for", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("goals_against", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("points", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("source_api", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("schema_version", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("extra_fields", "STRING", mode="NULLABLE"),
]

COLUMNS = [field.name for field in SCHEMA]


class BigQueryLoader:
    def __init__(self):
        self.logger = get_logger("loader.bigquery")

    def _get_client(self):
        credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
        return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

    def _ensure_dataset(self, client):
        dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET}")
        dataset_ref.location = "US"
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            client.create_dataset(dataset_ref)
            self.logger.info(f"Created BigQuery dataset '{BQ_DATASET}'")

    def load(self, records, table):
        if not records:
            self.logger.warning(f"No records to load into '{table}'")
            return

        client = self._get_client()
        self._ensure_dataset(client)

        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table}"
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        rows = [{col: r.get(col) for col in COLUMNS} for r in records]
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

        self.logger.info(f"Loaded {len(rows)} records into BigQuery '{table}'")

    def export_csv(self, table):
        client = self._get_client()
        query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table}` ORDER BY rank ASC"

        try:
            rows = list(client.query(query).result())
        except Exception as e:
            self.logger.error(f"BigQuery export failed for '{table}': {e}")
            return

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        csv_path = os.path.join(OUTPUT_DIR, f"{table}.csv")

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS)
            writer.writerows([[row[col] for col in COLUMNS] for row in rows])

        self.logger.info(f"Exported {len(rows)} rows to '{csv_path}'")
