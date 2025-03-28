import logging
from pymongo import UpdateOne
from tqdm import tqdm
from db.mdb import MongoDBConnector
from embeddings.vogayeai.vogaye_ai_embeddings import VogayeAIEmbeddings
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class FinancialNewsRecreateEmbeddings:
    def __init__(self, collection_name: str):
        """
        Initialize the FinancialNewsRecreateEmbeddings class.
        This class is responsible for dropping and recreating the `article_embedding` attribute
        for financial news articles in a MongoDB collection.
        It uses the Voyage AI API to generate embeddings for the articles.
        It requires the `VOYAGE_API_KEY` environment variable to be set.
        It also requires the `NEWS_COLLECTION` environment variable to be set, which specifies the name of the MongoDB collection to work with.
        The class uses the MongoDBConnector to connect to the database and perform operations on the collection.
        It uses the VogayeAIEmbeddings class to generate embeddings for the articles.
        The class uses the tqdm library to provide a progress bar for the processing of articles.
        It uses the logging library to log the progress and results of the operations.

        Args:
            collection_name (str): The name of the MongoDB collection containing the financial news articles.
        """
        self.collection_name = collection_name
        self.mongo_client = MongoDBConnector()
        self.collection = self.mongo_client.get_collection(collection_name=collection_name)
        self.vogaye_embeddings = VogayeAIEmbeddings(api_key=os.getenv("VOYAGE_API_KEY"))
        self.vogaye_model_id = "voyage-finance-2"

    def drop_article_embeddings(self):
        """
        Drops the `article_embedding` attribute from all documents in the collection.
        """
        logger.info("Dropping `article_embedding` attribute from all documents...")
        result = self.collection.update_many(
            {"article_embedding": {"$exists": True}},
            {"$unset": {"article_embedding": ""}}
        )
        logger.info(f"Dropped `article_embedding` from {result.modified_count} documents.")

    def recreate_article_embeddings(self):
        """
        Recreates the `article_embedding` attribute for all documents in the collection.
        """
        logger.info("Recreating `article_embedding` attribute for all documents...")

        # Find all documents that do not have the `article_embedding` attribute
        articles = list(self.collection.find({"article_embedding": {"$exists": False}}))
        logger.info(f"Found {len(articles)} articles to process.")

        if not articles:
            logger.info("No articles to process.")
            return

        operations = []
        for article in tqdm(articles, desc="Processing articles"):
            article_string = article.get("article_string", "")
            if not article_string:
                logger.warning(f"Skipping article with _id: {article.get('_id')} due to missing `article_string`.")
                continue

            # Generate embeddings using Voyage AI
            embedding = self.vogaye_embeddings.get_embeddings(model_id=self.vogaye_model_id, text=article_string)
            if embedding is None:
                logger.error(f"Skipping article with _id: {article.get('_id')} due to embedding generation error.")
                continue

            # Prepare the update operation
            update = {
                "$set": {
                    "article_embedding": embedding
                }
            }
            operations.append(UpdateOne({"_id": article["_id"]}, update))

        # Perform bulk update
        if operations:
            result = self.collection.bulk_write(operations)
            logger.info(f"Bulk update result: Matched: {result.matched_count}, Modified: {result.modified_count}")
        else:
            logger.info("No articles were updated.")

    def run(self):
        """
        Executes the process of dropping and recreating article embeddings.
        """
        logger.info("Starting the process of recreating article embeddings...")
        self.drop_article_embeddings()
        self.recreate_article_embeddings()
        logger.info("Process of recreating article embeddings completed.")

# Example usage
if __name__ == "__main__":
    # Initialize the process
    collection_name = os.getenv("NEWS_COLLECTION", "financial_news")
    recreate_embeddings = FinancialNewsRecreateEmbeddings(collection_name=collection_name)

    # Run the process
    recreate_embeddings.run()