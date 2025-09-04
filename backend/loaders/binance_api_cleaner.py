import os
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from loaders.db.mdb import MongoDBConnector
from loaders.config.config_loader import ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class BinanceAPICleaner(MongoDBConnector):
    def __init__(self, uri=None, database_name=None, appname=None, collection_name=None):
        """
        Initializes the BinanceAPICleaner.
        """
        collection_name = collection_name or os.getenv("BINANCE_TIMESERIES_COLLECTION", "binanceCryptoData")
        super().__init__(uri, database_name, collection_name, appname)
        self.collection_name = collection_name
        logger.info("BinanceAPICleaner initialized using collection: %s", self.collection_name)

    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalizes the given Binance symbol by removing the USDT suffix if present.

        Parameters:
            - symbol (str): The Binance symbol to be normalized.
        
        Returns:
            - str: Normalized symbol without USDT suffix.
        """
        # Remove 'USDT' suffix if present
        if symbol.endswith("USDT"):
            return symbol[:-4]  # Remove last 4 characters (USDT)
        return symbol

    def get_tickers(self):
        """
        Loads tickers from the configuration file and normalizes them.
        """
        logging.info("Loading tickers from configuration")
        config_loader = ConfigLoader()

        # Load configurations
        crypto = config_loader.get("BINANCE_ASSETS").split()

        # Combine all tickers into a single list
        tickers = crypto

        # Normalize tickers
        normalized_tickers = [self.normalize_symbol(ticker) for ticker in tickers]

        return normalized_tickers

    def clean_up_data(self):
        """
        Cleans up the binanceCryptoData collection by retaining only the last 60 days of data.
        """
        collection = self.db[self.collection_name]
        tickers = self.get_tickers()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=60)

        for ticker in tickers:
            logger.info(f"Cleaning up data for ticker: {ticker}")

            # Find documents older than 60 days
            old_documents_count = collection.count_documents({"symbol": ticker, "timestamp": {"$lt": cutoff_date}})
            if old_documents_count > 0:
                # Delete documents older than 60 days
                result = collection.delete_many({"symbol": ticker, "timestamp": {"$lt": cutoff_date}})
                logger.info(f"Deleted {result.deleted_count} old documents for ticker {ticker}")
            else:
                logger.info(f"No documents older than 60 days for ticker {ticker}")

    def run(self):
        """
        Runs the Binance API tickers cleaner process.
        """
        self.clean_up_data()

if __name__ == "__main__":
    cleaner = BinanceAPICleaner()
    cleaner.run()