import os
from dotenv import load_dotenv
import praw
from datetime import datetime
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
                 collection_name: str = os.getenv("SUBREDDIT_MAPPINGS_COLLECTION", "subredditMappings")):
        """
        Reddit Subreddit data extractor using mappings from MongoDB.

        Args:
            uri (str, optional): MongoDB URI. Defaults to None.
            database_name (str, optional): Database name. Defaults to None.
            appname (str, optional): Application name. Defaults to None.
            collection_name (str, optional): Collection name for subreddit mappings. 
                                             Defaults to value from SUBREDDIT_MAPPINGS_COLLECTION env var.
        """
        # Load environment variables
        load_dotenv()
        
        # Initialize MongoDB connection
        super().__init__(uri, database_name, appname)
        self.mappings_collection_name = collection_name
        logger.info(f"Using MongoDB collection '{self.mappings_collection_name}' for subreddit mappings")
        
        # Load configuration
        self.config = ConfigLoader()
        self.binance_assets = self.config.get("BINANCE_ASSETS", "").split()
        logger.info(f"Loaded {len(self.binance_assets)} Binance assets from config")
        
        # Create index on asset_binance field for faster lookups
        self._ensure_indexes()
        
        # Initialize Reddit API client
        self._initialize_reddit_client()
    
    def _ensure_indexes(self):
        """Ensure necessary indexes exist on the mappings collection."""
        try:
            # Create index on asset_binance field if it doesn't exist
            self.db[self.mappings_collection_name].create_index("asset_binance")
            logger.info("Created index on asset_binance field")
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
        """
        Get the subreddit mapping for a specific Binance asset from MongoDB.
        
        Args:
            binance_asset (str): The Binance asset identifier (e.g., "BTCUSDT")
            
        Returns:
            dict: The mapping document or None if not found
        """
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
        """
        Get all subreddit mappings from MongoDB, optionally limited to configured Binance assets.
        
        Args:
            only_configured_assets (bool, optional): If True, only return mappings for assets in config.BINANCE_ASSETS.
                                                    Defaults to False.
            
        Returns:
            list: List of mapping documents
        """
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
    
    def search_subreddit(self, subreddit_name: str, query: str, 
                        sort: str = "new", time_filter: str = "day", 
                        limit: int = 10) -> list:
        """
        Search a specific subreddit for submissions matching the query.
        
        Args:
            subreddit_name (str): Name of the subreddit to search.
            query (str): Search query.
            sort (str, optional): Sort order for results. Defaults to "new".
            time_filter (str, optional): Time filter for search. Defaults to "day".
            limit (int, optional): Maximum number of submissions to fetch. Defaults to 10.
            
        Returns:
            list: List of submission data dictionaries.
        """
        logger.info(f"Searching subreddit r/{subreddit_name} for query: '{query}'")
        
        submissions_data = []
        
        for attempt in range(3):
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                submissions_list = list(subreddit.search(
                    query=query,
                    sort=sort,
                    time_filter=time_filter,
                    limit=limit
                ))
                
                logger.info(f"Found {len(submissions_list)} submissions in r/{subreddit_name} for query '{query}'")
                
                for submission in submissions_list:
                    submission_data = self._process_submission(submission)
                    submissions_data.append(submission_data)
                
                return submissions_data
                
            except Exception as e:
                logger.error(f"Error searching subreddit {subreddit_name}: {e}")
                if attempt < 2:
                    logger.info(f"Retrying... ({attempt + 1}/3)")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to search subreddit {subreddit_name} after 3 attempts.")
        
        return submissions_data
    
    def _process_submission(self, submission) -> dict:
        """
        Process a submission and extract relevant data.
        
        Args:
            submission: PRAW Submission object
            
        Returns:
            dict: Dictionary containing submission data
        """
        created_date = datetime.fromtimestamp(submission.created)
        created_utc_date = datetime.fromtimestamp(submission.created_utc)
        
        submission_data = {
            "subreddit": str(submission.subreddit),
            "title": submission.title,
            "author": str(submission.author) if submission.author else None,
            "author_fullname": submission.author_fullname if hasattr(submission, 'author_fullname') else None,
            "author_premium": submission.author_premium if hasattr(submission, 'author_premium') else None,
            "author_is_blocked": submission.author_is_blocked if hasattr(submission, 'author_is_blocked') else None,
            "created_at": created_date.strftime('%Y-%m-%d %H:%M:%S'),
            "created_at_utc": created_utc_date.strftime('%Y-%m-%d %H:%M:%S'),
            "domain": submission.domain,
            "name": submission.name,
            "score": submission.score,
            "url": submission.url,
            "selftext": submission.selftext,
            "num_comments": submission.num_comments,
            "ups": submission.ups,
            "downs": submission.downs
        }
        
        logger.debug(f"Processed submission: {submission.title}")
        return submission_data
    
    def search_for_asset(self, binance_asset: str, sort: str = "new", 
                        time_filter: str = "day", limit: int = 10) -> dict:
        """
        Search for content related to a specific Binance asset across its mapped subreddits.
        
        Args:
            binance_asset (str): The Binance asset identifier (e.g., "BTCUSDT").
            sort (str, optional): Sort order for results. Defaults to "new".
            time_filter (str, optional): Time filter for search. Defaults to "day".
            limit (int, optional): Maximum number of submissions per subreddit. Defaults to 10.
            
        Returns:
            dict: Dictionary with subreddit names as keys and lists of submission data as values.
        """
        mapping = self.get_mapping(binance_asset)
        if not mapping:
            logger.warning(f"No subreddit mapping found for {binance_asset}")
            return {}
        
        subreddits = mapping.get("subreddits", [])
        query = mapping.get("query", binance_asset)
        
        logger.info(f"Searching for {binance_asset} with query '{query}' across {len(subreddits)} subreddits")
        
        results = {}
        for subreddit in subreddits:
            submissions = self.search_subreddit(
                subreddit_name=subreddit,
                query=query,
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
        Search for content related to all assets in the database, optionally limited to configured assets.
        
        Args:
            sort (str, optional): Sort order for results. Defaults to "new".
            time_filter (str, optional): Time filter for search. Defaults to "day".
            limit (int, optional): Maximum number of submissions per subreddit. Defaults to 10.
            only_configured_assets (bool, optional): If True, only search for assets in config.BINANCE_ASSETS.
                                                    Defaults to True.
            
        Returns:
            dict: Dictionary with Binance asset identifiers as keys and search results as values.
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
    
    def print_submission_details(self, submission_data: dict):
        """
        Print details of a submission in a formatted way.
        
        Args:
            submission_data (dict): Submission data dictionary.
        """
        print("####### Submission Details ###########")
        print("#######################################")
        print(f"Subreddit: {submission_data['subreddit']}")
        print(f"Title: {submission_data['title']}")
        print(f"Author: {submission_data['author']}")
        print(f"Author Full Name: {submission_data['author_fullname']}")
        print(f"Author is Premium?: {submission_data['author_premium']}")
        print(f"Author is Blocked?: {submission_data['author_is_blocked']}")
        print(f"Created At: {submission_data['created_at']}")
        print(f"Created At (UTC): {submission_data['created_at_utc']}")
        print(f"Domain: {submission_data['domain']}")
        print(f"Name: {submission_data['name']}")
        print(f"Score: {submission_data['score']}")
        print(f"URL: {submission_data['url']}")
        print(f"Selftext: {submission_data['selftext']}")
        print("-" * 80)
        print(f"Comments: {submission_data['num_comments']}")
        print(f"Ups: {submission_data['ups']}")
        print(f"Downs: {submission_data['downs']}")


if __name__ == "__main__":
    # Example usage
    reddit_wrapper = SubredditPrawWrapper()
    
    # Example 3: Search all configured Binance assets
    print("\n===== Example 3: Summary of all configured Binance asset searches =====")
    all_results = reddit_wrapper.search_all_assets(limit=3)
    for binance_asset, results in all_results.items():
        total_subs = sum(len(subs) for subs in results.values())
        print(f"{binance_asset}: Found {total_subs} submissions across {len(results)} subreddits")