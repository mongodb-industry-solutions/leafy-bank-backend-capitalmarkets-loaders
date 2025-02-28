from datetime import datetime, timedelta
import pytz
from db.mongo_db import MongoDBConnector
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

class YFinanceTickersBackup(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, collection_name: str = os.getenv("YFINANCE_TIMESERIES_COLLECTION")):
        """
        Backup utility class for retrieving and storing day-specific data.
        
        Args:
            uri (str, optional): MongoDB URI.
            database_name (str, optional): Database name.
            appname (str, optional): Application name.
            collection_name (str, optional): Collection name. Defaults to "yfinanceMarketData".
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name
        logger.info("YFinanceTickersBackup initialized")
    
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
    equities         = config_loader.get("EQUITIES")          # e.g.: "SPY QQQ EEM XLE"
    bonds            = config_loader.get("BONDS")             # e.g.: "TLT LQD HYG"
    commodities      = config_loader.get("COMMODITIES")       # e.g.: "GLD USO"
    market_volatility= config_loader.get("MARKET_VOLATILITY") # e.g.: "^VIX"
    
    # Combine tickers from all asset types (assuming they are space-separated).
    tickers_list = set()
    for tickers in [equities, bonds, commodities, market_volatility]:
        if tickers:
            for ticker in tickers.split():
                tickers_list.add(ticker)
    
    # Helper function to normalize symbols:
    def normalize_symbol(ticker: str) -> str:
        # Remove leading '^' if present.
        if ticker.startswith("^"):
            ticker = ticker.lstrip("^")
        return ticker

    # Normalize all symbols.
    symbols = [normalize_symbol(ticker) for ticker in tickers_list]
    
    backup_date = "2025-02-26"  # Format: YYYY-MM-DD
    backup_instance = YFinanceTickersBackup()
    backup_instance.backup_day_data(symbols, backup_date)