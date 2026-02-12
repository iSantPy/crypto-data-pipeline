import os

from dotenv import load_dotenv


HISTORIC_TABLE_ID = "crypto_raw.market_history"
SNAPSHOT_TABLE_ID = "crypto_raw.market_snapshot"
METADATA_TABLE_ID = "crypto_raw.coins_metadata"
TEMP_HISTORIC_TABLE_ID = "crypto_raw.temp_market_history"

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")