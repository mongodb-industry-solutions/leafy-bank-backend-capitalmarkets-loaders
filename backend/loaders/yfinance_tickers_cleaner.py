import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loaders.db.mongo_db import MongoDBConnector
from loaders.config.config_loader import ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class YFinanceTickersCleaner(MongoDBConnector):
    def __init__(self, uri=None, database_name=None, appname=None, collection_name=None):
        """
        Initializes the YFinanceTickersCleaner.
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name or os.getenv("YFINANCE_MARKET_DATA_COLLECTION", "yfinanceMarketData")
        logger.info("YFinanceTickersCleaner initialized using collection: %s", self.collection_name)

    def get_tickers(self):
        """
        Loads tickers from the configuration file and normalizes them.
        """
        logging.info("Loading tickers from configuration")
        config_loader = ConfigLoader()

        # Load configurations
        equities = config_loader.get("EQUITIES").split()
        bonds = config_loader.get("BONDS").split()
        commodities = config_loader.get("COMMODITIES").split()
        market_volatility = config_loader.get("MARKET_VOLATILITY").split()

        # Combine all tickers into a single list
        tickers = equities + bonds + commodities + market_volatility

        # Normalize tickers (remove special characters like ^)
        normalized_tickers = [ticker.replace("^", "") for ticker in tickers]

        return normalized_tickers

    def clean_up_data(self):
        """
        Cleans up the yfinanceMarketData collection by retaining only the last 60 days of data.
        """
        collection = self.db[self.collection_name]
        tickers = self.get_tickers()
        cutoff_date = datetime.utcnow() - timedelta(days=60)

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
        Runs the YFinance tickers cleaner process.
        """
        self.clean_up_data()

if __name__ == "__main__":
    cleaner = YFinanceTickersCleaner()
    cleaner.run()