from google.cloud import bigquery

from clients.crypto_api_client import APIGeckoCoin
from services.bigquery_services import BigQueryService

from config import (
    HISTORIC_TABLE_ID, 
    SNAPSHOT_TABLE_ID, 
    METADATA_TABLE_ID, 
    TEMP_HISTORIC_TABLE_ID
)
from services.queries import (
    query_to_update_historic_table,
    query_to_calculate_diff_prices,
    query_to_compute_mas,
    query_to_compute_volatilities
)

def main() -> tuple:

    client = bigquery.Client()

    # API service
    api_geckocoin_service = APIGeckoCoin()
    api_geckocoin_service.get_historical_data(days=365)  # prices since last year
    api_geckocoin_service.get_snapshot()
    api_geckocoin_service.get_metada()

    # Datawarehouse service
    bq_dw_service = BigQueryService()

    # Runs once (idempotency off)
    # bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_historical, table_id=HISTORIC_TABLE_ID) 
    # bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_metadata, table_id=METADATA_TABLE_ID)

    # Runs everyday
    api_geckocoin_service.get_historical_data(days=0)  # current prices 
    # bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_historical, table_id=TEMP_HISTORIC_TABLE_ID)
    # bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_snapshot, table_id=SNAPSHOT_TABLE_ID) 

    # Update historic table and calculate metrics (idempontency on)
    bq_dw_service.run_query(client=client, query=query_to_update_historic_table)
    bq_dw_service.run_query(client=client, query=query_to_calculate_diff_prices)
    bq_dw_service.run_query(client=client, query=query_to_compute_mas)
    bq_dw_service.run_query(client=client, query=query_to_compute_volatilities)
    
    return 200, "App ran successfully!"

if __name__ == "__main__":
    main()