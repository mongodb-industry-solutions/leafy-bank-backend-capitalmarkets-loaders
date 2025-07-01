import os
from dotenv import load_dotenv
import praw
from praw.exceptions import PRAWException
from praw.exceptions import RedditAPIException
from datetime import datetime, timezone
import logging
import time
from db.mdb import MongoDBConnector
from config.config_loader import ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SubredditPrawWrapper(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, 
                 mappings_collection_name: str = os.getenv("SUBREDDIT_MAPPINGS_COLLECTION", "subredditMappings"),
                 submissions_collection_name: str = os.getenv("SUBREDDIT_SUBMISSIONS_COLLECTION", "subredditSubmissions")):
        """
        Reddit Subreddit data extractor using mappings from MongoDB.
        Retrieves, augments, and stores data in MongoDB.
        """
        # Load environment variables
        load_dotenv()
        
        # Initialize MongoDB connection
        super().__init__(uri, database_name, appname)
        self.mappings_collection_name = mappings_collection_name
        self.submissions_collection_name = submissions_collection_name
        logger.info(f"Using MongoDB collection '{self.mappings_collection_name}' for subreddit mappings")
        logger.info(f"Using MongoDB collection '{self.submissions_collection_name}' for subreddit submissions")
        
        # Load configuration
        self.config = ConfigLoader()
        self.binance_assets = self.config.get("BINANCE_ASSETS", "").split()
        logger.info(f"Loaded {len(self.binance_assets)} Binance assets from config")
        
        # Create indexes for efficient queries
        self._ensure_indexes()
        
        # Initialize Reddit API client
        self._initialize_reddit_client()
    
    def _ensure_indexes(self):
        """Ensure necessary indexes exist on the collections."""
        try:
            # Create index on asset_binance field if it doesn't exist
            self.db[self.mappings_collection_name].create_index("asset_binance")
            logger.info("Created index on asset_binance field in mappings collection")
            
            # Create indexes on submissions collection
            # Use compound index on url + asset_id to allow same URL for different assets
            self.db[self.submissions_collection_name].create_index(
                [("url", 1), ("asset_id", 1)], 
                unique=True
            )
            self.db[self.submissions_collection_name].create_index("asset_id")
            self.db[self.submissions_collection_name].create_index("subreddit")
            self.db[self.submissions_collection_name].create_index("created_at_utc")
            self.db[self.submissions_collection_name].create_index("extraction_timestamp_utc")
            logger.info("Created indexes on submissions collection")
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")
        
    def _initialize_reddit_client(self):
        """Initialize the Reddit API client with credentials from environment variables."""
        try:
            self.reddit = praw.Reddit(
                client_id=os.getenv("REDDIT_CLIENT_ID"),
                client_secret=os.getenv("REDDIT_SECRET"),
                username=os.getenv("REDDIT_USERNAME"),
                password=os.getenv("REDDIT_PASSWORD"),
                user_agent=os.getenv("REDDIT_USER_AGENT")
            )
            self.reddit.read_only = True
            logger.info("Reddit API client initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Reddit API client: {e}")
            raise
    
    def get_mapping(self, binance_asset: str) -> dict:
        """Get the subreddit mapping for a specific Binance asset from MongoDB."""
        try:
            mapping = self.db[self.mappings_collection_name].find_one({"asset_binance": binance_asset})
            if mapping:
                logger.info(f"Found mapping for {binance_asset}: {len(mapping['subreddits'])} subreddits")
            else:
                logger.warning(f"No mapping found for {binance_asset}")
            return mapping
        except Exception as e:
            logger.error(f"Error retrieving mapping for {binance_asset}: {e}")
            return None
    
    def get_all_mappings(self, only_configured_assets: bool = False) -> list:
        """Get all subreddit mappings from MongoDB, optionally limited to configured Binance assets."""
        try:
            query = {}
            
            if only_configured_assets and self.binance_assets:
                query["asset_binance"] = {"$in": self.binance_assets}
                filter_str = " (filtered to configured Binance assets only)"
            else:
                filter_str = ""
            
            mappings = list(self.db[self.mappings_collection_name].find(query))
            logger.info(f"Retrieved {len(mappings)} asset mappings{filter_str}")
            
            return mappings
        except Exception as e:
            logger.error(f"Error retrieving asset mappings: {e}")
            return []
    
    def search_subreddit(self, subreddit_name: str, query: str, asset_id: str = None,
                        binance_asset: str = None, sort: str = "new", 
                        time_filter: str = "day", limit: int = 10) -> list:
        """
        Search a specific subreddit for submissions matching the query and store results.
        Handles cases where subreddits might be unavailable or deleted.
        """
        logger.info(f"Searching subreddit r/{subreddit_name} for query: '{query}'")
        
        submissions_data = []
        
        for attempt in range(3):
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Try to access subreddit properties and perform search
                try:
                    # This will trigger RedditAPIException for banned/non-existent subreddits
                    submissions_list = list(subreddit.search(
                        query=query,
                        sort=sort,
                        time_filter=time_filter,
                        limit=limit
                    ))
                    
                    logger.info(f"Found {len(submissions_list)} submissions in r/{subreddit_name} for query '{query}'")
                    
                    for submission in submissions_list:
                        submission_data = self._process_submission(
                            submission, 
                            subreddit_name=subreddit_name, 
                            query=query,
                            asset_id=asset_id,
                            binance_asset=binance_asset
                        )
                        submissions_data.append(submission_data)
                    
                    # Store results in MongoDB
                    if submissions_data:
                        self.store_submissions(submissions_data)
                    
                    return submissions_data
                    
                except RedditAPIException as api_exception:
                    # Handle specific Reddit API errors with more detailed messages
                    for error_item in api_exception.items:
                        if error_item.error_type == "private":
                            logger.warning(f"WARNING: Subreddit r/{subreddit_name} is private and cannot be accessed")
                        elif error_item.error_type == "banned":
                            logger.warning(f"WARNING: Subreddit r/{subreddit_name} has been banned")
                        elif error_item.error_type == "not_found" or error_item.error_type == "404":
                            logger.warning(f"WARNING: Subreddit r/{subreddit_name} does not exist")
                        else:
                            logger.warning(f"WARNING: Reddit API error when accessing r/{subreddit_name}: {error_item.error_message}")
                    
                    # Don't retry for these specific errors
                    return submissions_data
                    
                except PRAWException as praw_exception:
                    # Handle other PRAW-specific exceptions
                    logger.warning(f"WARNING: PRAW exception when accessing r/{subreddit_name}: {praw_exception}")
                    
                    # For general PRAW exceptions, we might want to retry
                    if attempt < 2:
                        logger.info(f"Retrying PRAW operation... ({attempt + 1}/3)")
                        time.sleep(2)
                    else:
                        logger.warning(f"WARNING: Failed to access r/{subreddit_name} after 3 attempts. Continuing with other subreddits.")
                        return submissions_data
                    
            except Exception as e:
                # General error handling for network issues or other problems
                logger.error(f"Error searching subreddit {subreddit_name}: {e}")
                if attempt < 2:
                    logger.info(f"Retrying... ({attempt + 1}/3)")
                    time.sleep(2)
                else:
                    logger.warning(f"WARNING: Failed to search subreddit r/{subreddit_name} after 3 attempts. Continuing with other subreddits.")
        
        return submissions_data
    
    def _process_submission(self, submission, subreddit_name: str, query: str, 
                       asset_id: str = None, binance_asset: str = None) -> dict:
        """
        Process a submission and extract relevant data with enhanced fields for semantic search.
        Also captures up to 3 most recent comments, if available.
        """
        # Extract timestamps as datetime objects for MongoDB
        created_date = datetime.fromtimestamp(submission.created)
        created_utc_date = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
        extraction_timestamp = datetime.now(timezone.utc)
        
        # Limit selftext to 1000 characters
        limited_selftext = submission.selftext[:1000] if submission.selftext and len(submission.selftext) > 1000 else submission.selftext
        
        # Extract top comments if available
        comments_data = []
        if submission.num_comments > 0:
            try:
                # Ensure comment tree is loaded and get the comments
                submission.comments.replace_more(limit=0)  # Don't fetch MoreComments
                
                # Get all comments as a flattened list
                all_comments = submission.comments.list()
                
                # Sort by created_utc to get the most recent ones
                # Reverse=True means newest first
                all_comments.sort(key=lambda comment: comment.created_utc, reverse=True)
                
                # Take the 3 most recent comments
                recent_comments = all_comments[:3]
                
                for comment in recent_comments:
                    # Extract comment data with proper timestamp
                    comment_created_date = datetime.fromtimestamp(comment.created_utc, tz=timezone.utc)
                    # Limit comment body to 250 characters
                    comment_text = comment.body[:250] if comment.body and len(comment.body) > 250 else comment.body
                    
                    comment_data = {
                        "id": comment.id,
                        "author": str(comment.author) if comment.author else "[deleted]",
                        "body": comment_text,  # Limited to 250 chars
                        "score": comment.score,
                        "created_at_utc": comment_created_date
                    }
                    comments_data.append(comment_data)
                
                logger.debug(f"Extracted {len(comments_data)} comments for submission: {submission.title}")
            except Exception as e:
                logger.warning(f"Error extracting comments for submission {submission.id}: {e}")
        
        # Create structured dictionary for semantic search instead of string
        submission_dict = {
            "title": submission.title,
            "selftext": limited_selftext,
            "subreddit": f"r/{subreddit_name}",
            "url": submission.url,
            "asset_id": asset_id or "Unknown",
            "query": query,
            "comments": [
                {
                    "author": comment["author"],
                    "body": comment["body"],
                    "created_at_utc": comment["created_at_utc"]
                } 
                for comment in comments_data
            ]
        }
        
        submission_data = {
            "subreddit": str(submission.subreddit),
            "title": submission.title,
            "author": str(submission.author) if submission.author else None,
            "author_fullname": submission.author_fullname if hasattr(submission, 'author_fullname') else None,
            "author_premium": submission.author_premium if hasattr(submission, 'author_premium') else None,
            "author_is_blocked": submission.author_is_blocked if hasattr(submission, 'author_is_blocked') else None,
            "created_at": created_date,  # Store as datetime object
            "created_at_utc": created_utc_date,  # Store as datetime object with timezone
            "domain": submission.domain,
            "name": submission.name,
            "score": submission.score,
            "url": submission.url,
            "selftext": submission.selftext,
            "num_comments": submission.num_comments,
            "ups": submission.ups,
            "downs": submission.downs,
            # Enhanced fields for semantic search
            "submission_dict": submission_dict,  # Store structured data instead of string
            "extraction_timestamp_utc": extraction_timestamp,
            "asset_id": asset_id,
            "binance_asset": binance_asset,
            "query": query,
            # Add the extracted comments
            "comments": comments_data
        }
        
        logger.debug(f"Processed submission: {submission.title}")
        return submission_data
    
    def store_submissions(self, submissions: list) -> int:
        """
        Store submission data in MongoDB.
        
        Args:
            submissions (list): List of submission data dictionaries
            
        Returns:
            int: Number of submissions stored
        """
        if not submissions:
            logger.info("No submissions to store")
            return 0
        
        stored_count = 0
        updated_count = 0
        
        for submission in submissions:
            try:
                # Use URL + asset_id as a compound unique identifier
                # This allows the same URL to exist for different assets
                query = {
                    "url": submission["url"],
                    "asset_id": submission["asset_id"]
                }
                update = {"$set": submission}
                result = self.db[self.submissions_collection_name].update_one(
                    query, update, upsert=True
                )
                
                if result.upserted_id:
                    stored_count += 1
                    logger.debug(f"Stored new submission for {submission['asset_id']}: {submission['title']}")
                elif result.modified_count > 0:
                    updated_count += 1
                    logger.debug(f"Updated existing submission for {submission['asset_id']}: {submission['title']}")
                
            except Exception as e:
                logger.error(f"Error storing submission {submission.get('title', 'Unknown')}: {e}")
        
        logger.info(f"Stored {stored_count} new submissions and updated {updated_count} existing submissions in MongoDB")
        return stored_count
    
    def search_for_asset(self, binance_asset: str, sort: str = "new", 
                        time_filter: str = "day", limit: int = 10) -> dict:
        """
        Search for content related to a specific Binance asset across its mapped subreddits
        and store results in MongoDB.
        """
        mapping = self.get_mapping(binance_asset)
        if not mapping:
            logger.warning(f"No subreddit mapping found for {binance_asset}")
            return {}
        
        subreddits = mapping.get("subreddits", [])
        query = mapping.get("query", binance_asset)
        asset_id = mapping.get("asset_id")
        
        logger.info(f"Searching for {binance_asset} (asset_id: {asset_id}) with query '{query}' across {len(subreddits)} subreddits")
        
        results = {}
        for subreddit in subreddits:
            submissions = self.search_subreddit(
                subreddit_name=subreddit,
                query=query,
                asset_id=asset_id,
                binance_asset=binance_asset,
                sort=sort,
                time_filter=time_filter,
                limit=limit
            )
            if submissions:
                results[subreddit] = submissions
        
        total_submissions = sum(len(subs) for subs in results.values())
        logger.info(f"Found {total_submissions} submissions across {len(results)} subreddits for {binance_asset}")
        
        return results
    
    def search_all_assets(self, sort: str = "new", time_filter: str = "day", 
                         limit: int = 10, only_configured_assets: bool = True) -> dict:
        """
        Search for content related to all assets in the database, optionally limited to configured assets,
        and store results in MongoDB.
        """
        # If only searching configured assets, we can use the list directly
        if only_configured_assets:
            assets_to_search = self.binance_assets
            logger.info(f"Searching for content related to {len(assets_to_search)} configured Binance assets")
        else:
            # Otherwise, get all mappings from MongoDB
            mappings = self.get_all_mappings()
            assets_to_search = [mapping["asset_binance"] for mapping in mappings]
            logger.info(f"Searching for content related to {len(assets_to_search)} assets from database")
        
        if not assets_to_search:
            logger.warning("No assets found to search")
            return {}
        
        results = {}
        for binance_asset in assets_to_search:
            asset_results = self.search_for_asset(
                binance_asset=binance_asset,
                sort=sort,
                time_filter=time_filter,
                limit=limit
            )
            if asset_results:
                results[binance_asset] = asset_results
        
        return results


if __name__ == "__main__":
    # Initialize the wrapper
    reddit_wrapper = SubredditPrawWrapper()
    
    # Execute data extraction and storage for all configured Binance assets
    logger.info("Starting Reddit data extraction and storage for configured Binance assets")
    
    # Search all assets and store results (this will automatically store in MongoDB)
    results = reddit_wrapper.search_all_assets(
        sort="new",
        time_filter="day",
        limit=10,  # Fetch up to 10 submissions per subreddit
        only_configured_assets=True
    )
    
    # Log summary information
    total_assets = len(results)
    total_submissions = sum(sum(len(subs) for subs in asset_results.values()) for asset_results in results.values())
    
    logger.info(f"Data extraction complete.")
    logger.info(f"Processed {total_assets} assets with a total of {total_submissions} submissions.")
    logger.info(f"All data has been stored in the '{reddit_wrapper.submissions_collection_name}' MongoDB collection.")