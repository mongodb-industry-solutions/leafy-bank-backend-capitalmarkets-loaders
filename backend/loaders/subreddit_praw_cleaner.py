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

class SubredditPrawCleaner(MongoDBConnector):
    def __init__(self, uri=None, database_name=None, appname=None, collection_name=None):
        """
        Initializes the SubredditPrawCleaner.
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name or os.getenv("SUBREDDIT_SUBMISSIONS_COLLECTION", "subredditSubmissions")
        logger.info("SubredditPrawCleaner initialized using collection: %s", self.collection_name)
        self.min_documents_per_asset = 40

    def get_binance_assets(self):
        """
        Loads Binance assets from the configuration file.
        """
        logging.info("Loading Binance assets from configuration")
        config_loader = ConfigLoader()

        # Load Binance assets from config
        binance_assets = config_loader.get("BINANCE_ASSETS").split()
        logger.info(f"Found {len(binance_assets)} Binance assets in configuration")
        
        return binance_assets

    def clean_up_data(self, retention_days=60):
        """
        Cleans up the subredditSubmissions collection by retaining only the last 60 days of data,
        while ensuring at least 40 documents per binance_asset are preserved.
        """
        collection = self.db[self.collection_name]
        binance_assets = self.get_binance_assets()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        
        # Format date to be more friendly (just date, no time)
        friendly_date = cutoff_date.strftime('%Y-%m-%d')
        logger.info(f"Cleaning up subreddit submissions older than {friendly_date}")
        
        # Process each Binance asset
        for asset in binance_assets:
            logger.info(f"Processing data for Binance asset: {asset}")
            
            # Count total documents for this asset
            total_docs = collection.count_documents({"binance_asset": asset})
            logger.info(f"Found {total_docs} total documents for {asset}")
            
            # Count documents older than the retention period
            old_docs_count = collection.count_documents({
                "binance_asset": asset, 
                "created_at_utc": {"$lt": cutoff_date}
            })
            
            if old_docs_count == 0:
                logger.info(f"No documents older than {retention_days} days for {asset}")
                continue
                
            # Calculate how many old documents we can safely remove
            # while preserving at least min_documents_per_asset
            docs_to_preserve = max(0, self.min_documents_per_asset - (total_docs - old_docs_count))
            docs_to_delete = old_docs_count - docs_to_preserve
            
            if docs_to_delete <= 0:
                logger.info(f"Not enough documents for {asset}. Preserving all to maintain minimum of {self.min_documents_per_asset}")
                continue
                
            logger.info(f"Will delete {docs_to_delete} of {old_docs_count} old documents for {asset} (preserving {docs_to_preserve})")
            
            if docs_to_preserve > 0:
                # If we need to preserve some old documents, find the newest ones among the old documents
                oldest_to_keep = collection.find(
                    {"binance_asset": asset, "created_at_utc": {"$lt": cutoff_date}}
                ).sort("created_at_utc", -1).limit(docs_to_preserve)
                
                # Get their IDs to exclude from deletion
                preserve_ids = [doc["_id"] for doc in oldest_to_keep]
                
                # Delete old documents except the ones we want to preserve
                result = collection.delete_many({
                    "binance_asset": asset,
                    "created_at_utc": {"$lt": cutoff_date},
                    "_id": {"$nin": preserve_ids}
                })
            else:
                # If we don't need to preserve any old documents, delete them all
                result = collection.delete_many({
                    "binance_asset": asset,
                    "created_at_utc": {"$lt": cutoff_date}
                })
                
            logger.info(f"Deleted {result.deleted_count} old documents for {asset}")
        
        # Also clean up documents with no binance_asset field or null binance_asset
        # Only if they're older than the retention period
        null_asset_result = collection.delete_many({
            "$or": [
                {"binance_asset": {"$exists": False}},
                {"binance_asset": None}
            ],
            "created_at_utc": {"$lt": cutoff_date}
        })
        
        if null_asset_result.deleted_count > 0:
            logger.info(f"Deleted {null_asset_result.deleted_count} old documents with no binance_asset")

    def run(self, retention_days=60):
        """
        Runs the SubredditPraw cleaner process.
        
        Args:
            retention_days (int): Number of days to retain data for (default: 60)
        """
        logger.info(f"Starting SubredditPrawCleaner with {retention_days} days retention period")
        self.clean_up_data(retention_days)
        logger.info("SubredditPrawCleaner process completed")

if __name__ == "__main__":
    cleaner = SubredditPrawCleaner()
    cleaner.run()