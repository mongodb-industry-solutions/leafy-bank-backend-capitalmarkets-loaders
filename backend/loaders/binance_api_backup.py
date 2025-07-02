from datetime import datetime, timedelta
import pytz
from loaders.db.mdb import MongoDBConnector
from bson import json_util
import logging
from config.config_loader import ConfigLoader

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BinanceAPIBackup(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, collection_name: str = os.getenv("BINANCE_TIMESERIES_COLLECTION")):
        """
        Backup utility class for retrieving and storing day-specific data.
        
        Args:
            uri (str, optional): MongoDB URI.
            database_name (str, optional): Database name.
            appname (str, optional): Application name.
            collection_name (str, optional): Collection name. Defaults to "binanceCryptoData".
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name
        logger.info("BinanceAPIBackup initialized")
    
    def backup_day_data(self, symbols: list, backup_date: str):
        """
        Retrieves data for all given symbols for the specified backup_date 
        and stores the result in a JSON file named bkp_day_data_(symbol).json.
        It also logs how many documents were backed up.
        
        Args:
            symbols (list): List of symbol strings.
            backup_date (str): The backup date in "YYYY-MM-DD" format.
        """
        # Parse backup_date into a datetime object with UTC tzinfo
        start_day = datetime.strptime(backup_date, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
        end_day = start_day + timedelta(days=1)
        
        # Ensure the backup directory exists
        backup_dir = "./backend/loaders/backup"
        os.makedirs(backup_dir, exist_ok=True)
        
        for symbol in symbols:
            query = {
                "symbol": symbol,
                "timestamp": {
                    "$gte": start_day,
                    "$lt": end_day
                }
            }
            cursor = self.db[self.collection_name].find(query)
            records = list(cursor)
            count = len(records)
            
            # Remove the _id field from each document
            for record in records:
                if '_id' in record:
                    del record['_id']
            
            filename = os.path.join(backup_dir, f"bkp_day_data_{symbol}.json")
            
            # Remove existing file if it exists
            if os.path.exists(filename):
                os.remove(filename)
                logger.info(f"Previous backup file {filename} deleted.")
            
            with open(filename, "w") as f:
                # Dump MongoDB data to JSON using bson.json_util to handle special types
                f.write(json_util.dumps(records, indent=4))
            logger.info(f"Backup for symbol {symbol} on {backup_date} written to {filename} with {count} documents.")

if __name__ == "__main__":
    config_loader = ConfigLoader()
    crypto = config_loader.get("BINANCE_ASSETS") # e.g.: "BTCUSDT ETHUSDT USDCUSDT XRPUSDT SOLUSDT DOGEUSDT ADAUSDT"
    
    # Combine tickers from all asset types (assuming they are space-separated).
    tickers_list = set()
    for tickers in [crypto]:
        if tickers:
            for ticker in tickers.split():
                tickers_list.add(ticker)

    # Helper function to normalize symbols:
    def normalize_symbol(ticker: str) -> str:
        # Remove 'USDT' suffix if present
        if ticker.endswith("USDT"):
            return ticker[:-4]  # Remove last 4 characters (USDT)
        return ticker

    # Normalize all symbols.
    symbols = [normalize_symbol(ticker) for ticker in tickers_list]
    
    backup_date = "2025-06-24"  # Format: YYYY-MM-DD
    backup_instance = BinanceAPIBackup()
    backup_instance.backup_day_data(symbols, backup_date)