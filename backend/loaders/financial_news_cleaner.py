import os
import logging
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

class FinancialNewsCleaner(MongoDBConnector):
    def __init__(self, uri=None, database_name=None, appname=None, collection_name=None):
        """
        Initializes the FinancialNewsCleaner.
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name or os.getenv("NEWS_COLLECTION", "financial_news")
        logger.info("FinancialNewsCleaner initialized using collection: %s", self.collection_name)

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

    def clean_up_articles(self):
        """
        Cleans up the financial news collection by keeping only the most recent 100 articles for each ticker.
        """
        collection = self.db[self.collection_name]
        tickers = self.get_tickers()

        for ticker in tickers:
            # Find the number of articles for the ticker
            article_count = collection.count_documents({"ticker": ticker})
            if article_count > 100:
                logger.info(f"Ticker {ticker} has {article_count} articles. Cleaning up...")

                # Find the articles to delete, keeping the most recent 100
                articles_to_delete = collection.find({"ticker": ticker}).sort("extraction_timestamp_utc", 1).limit(article_count - 100)
                article_ids_to_delete = [article["_id"] for article in articles_to_delete]

                # Delete the old articles
                result = collection.delete_many({"_id": {"$in": article_ids_to_delete}})
                logger.info(f"Deleted {result.deleted_count} old articles for ticker {ticker}")

            else:
                logger.info(f"Ticker {ticker} has {article_count} articles. No clean up needed.")

    def run(self):
        """
        Runs the financial news cleaner process.
        """
        self.clean_up_articles()

if __name__ == "__main__":
    cleaner = FinancialNewsCleaner()
    cleaner.run()