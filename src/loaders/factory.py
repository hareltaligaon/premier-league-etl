from src.config import LOADER
from src.loaders.sqlite_loader import SQLiteLoader
from src.loaders.bigquery_loader import BigQueryLoader


def get_loader():
    if LOADER == "bigquery":
        return BigQueryLoader()
    else:
        return SQLiteLoader()
