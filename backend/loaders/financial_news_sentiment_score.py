import os
import logging
from dotenv import load_dotenv
from pymongo import UpdateOne
from loaders.db.mongo_db import MongoDBConnector
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class FinancialNewsSentimentScore(MongoDBConnector):
    def __init__(self, uri=None, database_name=None, appname=None, collection_name=None):
        """
        Initializes the FinancialNewsSentimentScore.
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name or os.getenv("NEWS_COLLECTION", "financial_news")
        logger.info("FinancialNewsSentimentScore initialized using collection: %s", self.collection_name)
        # Load the FinBERT model and tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        # Initialize the sentiment analysis pipeline
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=self.model, tokenizer=self.tokenizer, return_all_scores=True)

    def get_sentiment_scores(self, text: str) -> dict:
        """
        Computes the sentiment scores for the given text using FinBERT.
        """
        if not text or not isinstance(text, str):
            logger.error("Invalid input text for sentiment analysis.")
            return None
        try:
            results = self.sentiment_pipeline(text)[0]
            scores = {result['label'].lower(): result['score'] for result in results}
            return scores
        except Exception as e:
            logger.error("Error computing sentiment scores: %s", e)
            return None

    def process_articles(self):
        """
        Processes financial news articles by:
          - Computing the sentiment scores for the article_string.
          - Updating the document with the new sentiment_score attribute.
        """
        collection = self.db[self.collection_name]
        # Filter to find articles that do not have sentiment_score attribute
        articles = list(collection.find({"sentiment_score": {"$exists": False}}))
        logger.info("Found %d unprocessed articles.", len(articles))
        
        if not articles:
            logger.info("No articles to process.")
            return

        operations = []
        for article in tqdm(articles, desc="Processing articles"):
            article_string = article.get("article_string", "")
            sentiment_scores = self.get_sentiment_scores(article_string)
            if sentiment_scores is None:
                logger.error("Skipping article with _id: %s due to sentiment analysis error.", article.get("_id"))
                continue
            update = {
                "$set": {
                    "sentiment_score": sentiment_scores
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
        Runs the financial news sentiment score process.
        """
        self.process_articles()

if __name__ == "__main__":
    sentiment_creator = FinancialNewsSentimentScore()
    sentiment_creator.run()