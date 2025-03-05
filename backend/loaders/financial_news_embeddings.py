import os
import logging
from dotenv import load_dotenv
from pymongo import UpdateOne
from loaders.db.mongo_db import MongoDBConnector
from loaders.embeddings.bedrock.cohere_embeddings import BedrockCohereEnglishEmbeddings
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class FinancialNewsEmbeddings(MongoDBConnector):
    def __init__(self, uri=None, database_name=None, appname=None, collection_name=None):
        """
        Initializes the FinancialNewsEmbeddings processor.
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name or os.getenv("NEWS_COLLECTION", "financial_news")
        logger.info("FinancialNewsEmbeddings initialized using collection: %s", self.collection_name)
        # Instantiate the embeddings model
        self.embeddings_model = BedrockCohereEnglishEmbeddings(
            region_name=os.getenv("AWS_REGION"),
            aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

    def create_article_string(self, article: dict) -> str:
        """
        Creates a string representation of an article.
        
        The string is a concatenation of headline, description, source, ticker, and link.
        """
        headline = article.get("headline", "")
        description = article.get("description", "")
        source = article.get("source", "")
        ticker = article.get("ticker", "")
        link = article.get("link", "")
        article_str = (
            f"Headline: {headline}\n"
            f"/n Description: {description}\n"
            f"/n Source: {source}\n"
            f"/n Ticker: {ticker}\n"
            f"/n Link: {link}"
        )
        return article_str

    def get_article_embedding(self, text: str):
        """
        Generates an embedding for the given text using BedrockCohereEnglishEmbeddings.
        """
        if not text or not isinstance(text, str):
            logger.error("Invalid input text for embedding.")
            return None
        try:
            embedding = self.embeddings_model.predict(text)
            return embedding
        except Exception as e:
            logger.error("Error generating embedding: %s", e)
            return None

    def process_articles(self):
        """
        Processes financial news articles by:
          - Creating an article_string from the article fields.
          - Generating an article_embedding from the article_string.
          - Updating the document with the new attributes.
        """
        collection = self.db[self.collection_name]
        # Filter to find articles that do not have article_string and article_embedding attributes
        articles = list(collection.find({"article_string": {"$exists": False}, "article_embedding": {"$exists": False}}))
        logger.info("Found %d unprocessed articles.", len(articles))
        
        if not articles:
            logger.info("No articles to process.")
            return

        operations = []
        for article in tqdm(articles, desc="Processing articles"):
            article_string = self.create_article_string(article)
            embedding = self.get_article_embedding(article_string)
            if embedding is None:
                logger.error("Skipping article with _id: %s due to embedding error.", article.get("_id"))
                continue
            update = {
                "$set": {
                    "article_string": article_string,
                    "article_embedding": embedding
                }
            }
            operations.append(UpdateOne({"_id": article["_id"]}, update))
            logger.info("Processed article with _id: %s", article.get("_id"))
        
        if operations:
            result = collection.bulk_write(operations)
            logger.info("Bulk update result: Matched: %d, Modified: %d", result.matched_count, result.modified_count)
        else:
            logger.info("No articles were updated.")

    def run(self):
        """
        Runs the financial news embeddings process.
        """
        self.process_articles()

if __name__ == "__main__":
    processor = FinancialNewsEmbeddings()
    processor.run()