from abc import ABC, abstractmethod

from google.cloud import bigquery


class DataWareHouseService(ABC):

    @abstractmethod
    def load_data_to_dw(self, client: bigquery.Client, rows: list, table_id: str) -> None:
        pass

    @abstractmethod
    def run_query(self, client: bigquery.Client, query: str) -> None:
        pass

class BigQueryService(DataWareHouseService):

    def load_data_to_dw(self, client: bigquery.Client, rows: list, table_id: str) -> None:
        try:
            job = client.load_table_from_json(rows, table_id)
            job.result()
        except Exception as e:
            print(e)

    def run_query(self, client: bigquery.Client, query: str) -> None:
        try:
            client.query(query).result()
        except Exception as e:
            print(e)