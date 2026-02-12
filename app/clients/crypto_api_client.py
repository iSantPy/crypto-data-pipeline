import logging
import requests
from datetime import datetime, UTC
from abc import ABC, abstractmethod

from config import API_KEY


logger = logging.getLogger(__name__)

class APIService(ABC):

    @abstractmethod
    def get_historical_data(self, days: int) -> None:
        pass
    
    @abstractmethod
    def get_snapshot(self) -> None:
        pass

    @abstractmethod
    def get_metada(self) -> None:
        pass


class APIGeckoCoin(APIService):

    def __init__(self):
        self.DEMO_API = "https://api.coingecko.com/api/v3"
        self.COINS = [
            {"id": "avalanche-2", "symbol": "avax", "name": "Avalanche"},
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
            {"id": "cardano", "symbol": "ada", "name": "Cardano"},
            {"id": "dogecoin", "symbol": "doge", "name": "Dogecoin"},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum"},
            {"id": "ripple", "symbol": "xrp", "name": "XRP"},
            {"id": "solana", "symbol": "sol", "name": "Solana"},
            {"id": "tether", "symbol": "usdt", "name": "Tether"}
        ]

    def get_historical_data(self, days: int) -> None:
        logger.info("Getting historical data!")
        self.rows_historical = []
        for coin in self.COINS:
            coin_id = coin.get("id")

            endpoint = f"{self.DEMO_API}/coins/{coin_id}/market_chart"
            params = {
                "vs_currency": "usd",
                "days": days,  # If 0 = current day. Max days available is 365 to the past from current day
                "x_cg_demo_api_key": API_KEY
            }

            response = requests.get(url=endpoint, params=params)

            if response.status_code == 200:
                data = response.json()
                prices = data["prices"]
                market_caps = data["market_caps"]
                total_volumes = data["total_volumes"]

                for i in range(len(prices)):
                    ts = prices[i][0]  # timestamp en ms
                    date = datetime.fromtimestamp(ts / 1000, UTC).isoformat() 
                    ingestion_date = datetime.now(UTC).date().isoformat()

                    row = {
                        "coin_id": coin_id,
                        "event_timestamp": date,
                        "price_usd": prices[i][1],
                        "market_cap_usd": market_caps[i][1],
                        "volume_usd": total_volumes[i][1],
                        "ingestion_date": ingestion_date
                    }
                    self.rows_historical.append(row)

    def get_snapshot(self) -> None:
        logger.info("Getting snapshot!")
        self.rows_snapshot = []
        endpoint = f"{self.DEMO_API}/coins/markets"
        params = {
            "vs_currency": "usd",
            "x_cg_demo_api_key": API_KEY
        }

        response = requests.get(url=endpoint, params=params)

        if response.status_code == 200:
            data = response.json()

            for i in range(len(data)):
                ingestion_date = datetime.now(UTC).date().isoformat()

                row = {
                    "id": data[i]["id"],
                    "current_price": data[i]["current_price"],
                    "market_cap": data[i]["market_cap"],
                    "market_cap_rank": data[i]["market_cap_rank"],
                    "fully_diluted_valuation": data[i]["fully_diluted_valuation"],
                    "total_volume": data[i]["total_volume"],
                    "high_24h": data[i]["high_24h"],
                    "low_24h": data[i]["low_24h"],
                    "price_change_24h": data[i]["price_change_24h"],
                    "price_change_percentage_24h": data[i]["price_change_percentage_24h"],
                    "market_cap_change_24h": data[i]["market_cap_change_24h"],
                    "market_cap_change_percentage_24h": data[i]["market_cap_change_percentage_24h"],
                    "circulating_supply": data[i]["circulating_supply"],
                    "total_supply": data[i]["total_supply"],
                    "max_supply": data[i]["max_supply"],
                    "ath": data[i]["ath"],
                    "ath_change_percentage": data[i]["ath_change_percentage"],
                    "ath_date": data[i]["ath_date"],
                    "atl": data[i]["atl"],
                    "atl_change_percentage": data[i]["atl_change_percentage"],
                    "atl_date": data[i]["atl_date"],
                    "roi": data[i]["roi"],
                    "last_updated": data[i]["last_updated"],
                    "ingestion_date": ingestion_date
                }
                self.rows_snapshot.append(row)

    def get_metada(self) -> None:
        logger.info("Getting metadata!")
        self.rows_metadata = []
        for currency in self.COINS:
            coin_id = currency.get("id")

            endpoint = f"{self.DEMO_API}/coins/{coin_id}"
            params = {
                "localization": "false",
                "tickers": "false",
                "market_data": "false",
                "community_data": "false",
                "developer_data": "false",
                "sparkline": "false",
                "x_cg_demo_api_key": API_KEY
            }

            response = requests.get(url=endpoint, params=params)

            if response.status_code == 200:
                data = response.json()
                
                row = {
                    "coin_id": data["id"],
                    "symbol": data["symbol"],
                    "name": data["name"],
                    "genesis_date": data.get("genesis_date"),
                    "hashing_algorithm": data.get("hashing_algorithm"),
                    "country_origin": data.get("country_origin"),
                    "categories": ", ".join(data.get("categories", [])),
                    "homepage": data["links"]["homepage"][0] if data["links"]["homepage"] else None
                }
                self.rows_metadata.append(row)