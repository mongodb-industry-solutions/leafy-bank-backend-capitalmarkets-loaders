import os
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from loaders.db.mdb import MongoDBConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class SubredditPrawCleaner(MongoDBConnector):
    def __init__(self, uri=None, database_name=None, appname=None, 
                 submissions_collection_name=None, mappings_collection_name=None):
        """
        Initializes the SubredditPrawCleaner.
        """
        super().__init__(uri, database_name, None, appname)
        self.submissions_collection_name = submissions_collection_name or os.getenv("SUBREDDIT_SUBMISSIONS_COLLECTION", "subredditSubmissions")
        self.mappings_collection_name = mappings_collection_name or os.getenv("ASSET_MAPPINGS_COLLECTION", "assetMappings")
        logger.info("SubredditPrawCleaner initialized using submissions collection: %s", self.submissions_collection_name)
        logger.info("SubredditPrawCleaner initialized using mappings collection: %s", self.mappings_collection_name)
        self.min_documents_per_asset = 40

    def get_all_asset_ids(self):
        """
        Gets all asset_ids from the assetMappings collection.
        """
        logger.info("Loading asset IDs from asset mappings collection")
        
        try:
            mappings_collection = self.db[self.mappings_collection_name]
            asset_ids = mappings_collection.distinct("asset_id")
            logger.info(f"Found {len(asset_ids)} assets in mappings collection: {asset_ids}")
            return asset_ids
        except Exception as e:
            logger.error(f"Error retrieving asset IDs from mappings collection: {e}")
            return []

    def clean_up_data(self, retention_days=60):
        """
        Cleans up the subredditSubmissions collection by retaining only the last 60 days of data,
        while ensuring at least 40 documents per asset_id are preserved.
        """
        submissions_collection = self.db[self.submissions_collection_name]
        asset_ids = self.get_all_asset_ids()
        
        if not asset_ids:
            logger.warning("No asset IDs found in mappings collection. Skipping cleanup.")
            return
            
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        
        # Format date to be more friendly (just date, no time)
        friendly_date = cutoff_date.strftime('%Y-%m-%d')
        logger.info(f"Cleaning up subreddit submissions older than {friendly_date}")
        
        # Process each asset
        for asset_id in asset_ids:
            logger.info(f"Processing data for asset: {asset_id}")
            
            # Count total documents for this asset
            total_docs = submissions_collection.count_documents({"asset_id": asset_id})
            logger.info(f"Found {total_docs} total documents for {asset_id}")
            
            # Count documents older than the retention period
            old_docs_count = submissions_collection.count_documents({
                "asset_id": asset_id, 
                "created_at_utc": {"$lt": cutoff_date}
            })
            
            if old_docs_count == 0:
                logger.info(f"No documents older than {retention_days} days for {asset_id}")
                continue
                
            # Calculate how many old documents we can safely remove
            # while preserving at least min_documents_per_asset
            docs_to_preserve = max(0, self.min_documents_per_asset - (total_docs - old_docs_count))
            docs_to_delete = old_docs_count - docs_to_preserve
            
            if docs_to_delete <= 0:
                logger.info(f"Not enough documents for {asset_id}. Preserving all to maintain minimum of {self.min_documents_per_asset}")
                continue
                
            logger.info(f"Will delete {docs_to_delete} of {old_docs_count} old documents for {asset_id} (preserving {docs_to_preserve})")
            
            if docs_to_preserve > 0:
                # If we need to preserve some old documents, find the newest ones among the old documents
                oldest_to_keep = submissions_collection.find(
                    {"asset_id": asset_id, "created_at_utc": {"$lt": cutoff_date}}
                ).sort("created_at_utc", -1).limit(docs_to_preserve)
                
                # Get their IDs to exclude from deletion
                preserve_ids = [doc["_id"] for doc in oldest_to_keep]
                
                # Delete old documents except the ones we want to preserve
                result = submissions_collection.delete_many({
                    "asset_id": asset_id,
                    "created_at_utc": {"$lt": cutoff_date},
                    "_id": {"$nin": preserve_ids}
                })
            else:
                # If we don't need to preserve any old documents, delete them all
                result = submissions_collection.delete_many({
                    "asset_id": asset_id,
                    "created_at_utc": {"$lt": cutoff_date}
                })
                
            logger.info(f"Deleted {result.deleted_count} old documents for {asset_id}")
        
        # Also clean up documents with no asset_id field or null asset_id
        # Only if they're older than the retention period
        null_asset_result = submissions_collection.delete_many({
            "$or": [
                {"asset_id": {"$exists": False}},
                {"asset_id": None}
            ],
            "created_at_utc": {"$lt": cutoff_date}
        })
        
        if null_asset_result.deleted_count > 0:
            logger.info(f"Deleted {null_asset_result.deleted_count} old documents with no asset_id")

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