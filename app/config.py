import os

from dotenv import load_dotenv


HISTORIC_TABLE_ID = "project-64bf9e5a-8b9a-4039-85a.crypto_raw.market_history"
SNAPSHOT_TABLE_ID = "project-64bf9e5a-8b9a-4039-85a.crypto_raw.market_snapshot"
METADATA_TABLE_ID = "project-64bf9e5a-8b9a-4039-85a.crypto_raw.coins_metadata"
TEMP_HISTORIC_TABLE_ID = "project-64bf9e5a-8b9a-4039-85a.crypto_raw.temp_market_history"

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_KEY")