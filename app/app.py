import logging

from google.cloud import bigquery
from flask import Flask, jsonify

from clients.crypto_api_client import APIGeckoCoin
from services.bigquery_services import BigQueryService

from config import (
    HISTORIC_TABLE_ID, 
    SNAPSHOT_TABLE_ID, 
    METADATA_TABLE_ID, 
    TEMP_HISTORIC_TABLE_ID,
    PROJECT_ID
)
from services.queries import (
    query_to_update_historic_table,
    query_to_calculate_diff_prices,
    query_to_compute_mas,
    query_to_compute_volatilities
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route("/", methods=["GET"])
def run_pipeline():
    try: 
        client = bigquery.Client(project=PROJECT_ID)

        # API service
        api_geckocoin_service = APIGeckoCoin()
        logger.info("Getting last year prices!")
        api_geckocoin_service.get_historical_data(days=365)  # prices since last year
        api_geckocoin_service.get_snapshot()
        api_geckocoin_service.get_metada()

        # Datawarehouse service
        bq_dw_service = BigQueryService()

        # Runs once (idempotency off)
        logger.info("Creating historic and metadata tables!")
        # bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_historical, table_id=HISTORIC_TABLE_ID) 
        # bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_metadata, table_id=METADATA_TABLE_ID)

        # Runs everyday
        logger.info("Getting current prices!")
        api_geckocoin_service.get_historical_data(days=0)  # current prices 
        logger.info("Loading data to temp and snapshot tables!")
        bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_historical, table_id=TEMP_HISTORIC_TABLE_ID)
        bq_dw_service.load_data_to_dw(client=client, rows=api_geckocoin_service.rows_snapshot, table_id=SNAPSHOT_TABLE_ID) 

        # Update historic table and calculate metrics (idempontency on)
        logger.info("Updating historic table and computing metrics!")
        bq_dw_service.run_query(client=client, query=query_to_update_historic_table)
        bq_dw_service.run_query(client=client, query=query_to_calculate_diff_prices)
        bq_dw_service.run_query(client=client, query=query_to_compute_mas)
        bq_dw_service.run_query(client=client, query=query_to_compute_volatilities)
        
        return jsonify({"status": 200, "message": "Pipeline ran successfully!"})
    except Exception as e:
        logger.exception("Pipeline failed!")
        return jsonify({"status": 500, "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)