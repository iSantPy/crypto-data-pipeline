import logging
from abc import ABC, abstractmethod

from google.cloud import bigquery


logger = logging.getLogger(__name__)

class DataWareHouseService(ABC):

    @abstractmethod
    def load_data_to_dw(self, client: bigquery.Client, rows: list, table_id: str) -> None:
        pass

    @abstractmethod
    def run_query(self, client: bigquery.Client, query: str) -> None:
        pass

class BigQueryService(DataWareHouseService):

    def load_data_to_dw(self, client: bigquery.Client, rows: list, table_id: str) -> None:
        logger.info("Loading data to BigQuery!")
        try:
            job = client.load_table_from_json(rows, table_id)
            job.result()
            logger.info("Data inserted into BigQuery!")
        except Exception as e:
            logger.error(f"Failed to insert data into BigQuery!: {e}")

    def run_query(self, client: bigquery.Client, query: str) -> None:
        logger.info("Running a query!")
        try:
            client.query(query).result()
            logger.info("Query ran successfully!")
        except Exception as e:
            logger.error(f"Couldn't run query!: {e}")