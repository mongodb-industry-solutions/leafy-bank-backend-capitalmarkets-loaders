import logging
import re
import requests
from time import sleep
from bs4 import BeautifulSoup
from loaders.db.mdb import MongoDBConnector
from loaders.generic_scraper import GenericScraper
from datetime import datetime, timezone

from tqdm import tqdm
from pymongo import UpdateOne
from loaders.embeddings.vogayeai.vogaye_ai_embeddings import VogayeAIEmbeddings

from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FinancialNewsScraper(GenericScraper):
    def __init__(self, collection_name, scrape_num_articles=1):
        """
        Initialize the FinancialNewsScraper with necessary parameters.

        Args:
            collection_name: str
            scrape_num_articles: int
        """
        self.headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'referer': 'https://www.google.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36 Edg/85.0.564.44'
        }

        self.collection_name = collection_name
        self.scrape_num_articles = scrape_num_articles
        self.mongo_client = MongoDBConnector()

        # Get the asset mappings collection name from environment variables
        asset_mappings_collection = os.getenv("ASSET_MAPPINGS_COLLECTION", "assetMappings")
        self.mappings_coll = self.mongo_client.get_collection(collection_name=asset_mappings_collection)
        self.assets_metadata = self._load_asset_metadata()

        # Get the database collection for financial news
        self.collection = self.mongo_client.get_collection(collection_name=collection_name)
        
        # == EMBEDDINGS ==
        # Instantiate the VogayeAIEmbeddings class
        self.vogaye_embeddings = VogayeAIEmbeddings(api_key=os.getenv("VOYAGE_API_KEY"))
        # https://blog.voyageai.com/2024/06/03/domain-specific-embeddings-finance-edition-voyage-finance-2/
        self.vogate_model_id = "voyage-finance-2"
        
        # == SENTIMENT SCORE ==
        # Initialize FinBERT model for sentiment analysis
        logger.info("Loading FinBERT model for sentiment analysis...")
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=self.model, tokenizer=self.tokenizer, top_k=None)
        logger.info("FinBERT model loaded successfully")

    @staticmethod
    def extract_article(card, ticker):
        """
        Extract article information from the raw HTML.

        Args:
            card: BeautifulSoup object
            ticker: str

        Returns:
            dict: Extracted article information
        """
        extraction_timestamp = datetime.now(timezone.utc)

        headline = card.find('h4', 's-title').text
        source = card.find("span", 's-source').text
        posted = card.find('span', 's-time').text.replace('·', '').strip()
        description = card.find('p', 's-desc').text.strip()
        raw_link = card.find('a').get('href')
        unquoted_link = requests.utils.unquote(raw_link)
        pattern = re.compile(r'RU=(.+)\/RK')
        clean_link = re.search(pattern, unquoted_link).group(1)

        return {
            'headline': headline,
            'source': source,
            'posted': posted,
            'description': description,
            'link': clean_link,
            'synced': False,
            'extraction_timestamp_utc': extraction_timestamp,
            'ticker': ticker
        }

    def _load_asset_metadata(self):
        """
        Load asset metadata from the assetMappings collection.
        This includes both asset_id (ticker) and query string.

        Returns:
            A list of dicts with "ticker" and "query" keys.
        """
        mappings = self.mappings_coll.find({}, {"asset_id": 1, "query": 1})
        return [
            {"ticker": doc["asset_id"], "query": doc["query"]}
            for doc in mappings
            if "asset_id" in doc and "query" in doc
        ]

    def scrape_articles(self, search_query, ticker):
        """
        Scrape news articles for a specific search query.

        Args:
            search_query: str

        Returns:
            list: List of extracted
        """
        template = 'https://news.search.yahoo.com/search?p={}'
        url = template.format(search_query)
        articles = []
        links = set()
        num_search = self.scrape_num_articles

        while num_search:
            num_search -= 1
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            cards = soup.find_all('div', 'NewsArticle')

            # Extract articles from the page
            for card in cards:
                article = self.extract_article(card, ticker)
                link = article['link']
                if link not in links:
                    links.add(link)
                    articles.append(article)

            # Find the next page
            try:
                url = soup.find('a', 'next').get('href')
                sleep(1)
            except AttributeError:
                break

        # Insert articles into MongoDB
        if articles:
            self.mongo_client.insert_many(self.collection_name, articles)
            logger.info(f"Inserted {len(articles)} articles into MongoDB.")

        return articles

    def scrape_all_tickers(self):
        """
        Scrape news articles for all tickers using their associated queries.
        """
        logger.info(f"Scraping news for tickers: {[a['ticker'] for a in self.assets_metadata]}")

        for asset in self.assets_metadata:
            ticker = asset["ticker"]
            query = asset["query"]
            logger.info(f"Scraping news for ticker: {ticker}")
            logger.info(f"Using query: {query}")
            try:
                self.scrape_articles(search_query=query, ticker=ticker)
            except Exception as e:
                logger.error(f"Error while scraping news for {ticker}: {e}")

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

    def get_article_embedding(self, text: str) -> list:
        """
        Generates an embedding for the given text.
        """
        if not text or not isinstance(text, str):
            logger.error("Invalid input text for embedding.")
            return None
        try:
            embedding = self.vogaye_embeddings.get_embeddings(model_id=self.vogate_model_id, text=text)
            return embedding
        except Exception as e:
            logger.error("Error generating embedding: %s", e)
            return None

    def process_articles_embeddings(self):
        """
        Processes financial news articles by:
          - Creating an article_string from the article fields.
          - Generating an article_embedding from the article_string.
          - Updating the document with the new attributes.
        """
        collection = self.collection
        # Filter to find articles that do not have article_string and article_embedding attributes
        articles = list(collection.find(
            {"article_string": {"$exists": False}, "article_embedding": {"$exists": False}}))
        logger.info("Found %d unprocessed articles.", len(articles))

        if not articles:
            logger.info("No articles to process.")
            return

        operations = []
        for article in tqdm(articles, desc="Processing articles"):
            article_string = self.create_article_string(article)
            embedding = self.get_article_embedding(article_string)
            if embedding is None:
                logger.error(
                    "Skipping article with _id: %s due to embedding error.", article.get("_id"))
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
            logger.info("Bulk update result: Matched: %d, Modified: %d",
                        result.matched_count, result.modified_count)
        else:
            logger.info("No articles were updated.")

    def get_sentiment_scores(self, text: str) -> dict:
        """
        Computes the sentiment scores for the given text using FinBERT.
        """
        if not text or not isinstance(text, str):
            logger.error("Invalid input text for sentiment analysis.")
            return None
        try:
            results = self.sentiment_pipeline(text)[0]
            scores = {result['label'].lower(): result['score']
                      for result in results}
            return scores
        except Exception as e:
            logger.error("Error computing sentiment scores: %s", e)
            return None

    def process_articles_sentiment_scores(self):
        """
        Processes financial news articles by:
        - Computing the sentiment scores for the article_string.
        - Updating the document with the new sentiment_score attribute.
        """
        # Find articles without sentiment_score
        articles = list(self.collection.find({"sentiment_score": {"$exists": False}}))
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
            result = self.collection.bulk_write(operations)
            logger.info("Bulk update result: Matched: %d, Modified: %d", result.matched_count, result.modified_count)
        else:
            logger.info("No articles were updated.")

    def remove_duplicates(self):
        """
        Removes duplicate articles from the collection based on ticker and link.
        Keeps only one article for each unique ticker+link combination.
        """
        logger.info("Starting duplicate removal process...")
        
        # Aggregation pipeline to find duplicates
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "ticker": "$ticker",
                        "link": "$link"
                    },
                    "count": {"$sum": 1},
                    "docs": {"$push": "$_id"}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            }
        ]
        
        duplicates = list(self.collection.aggregate(pipeline))
        
        if not duplicates:
            logger.info("No duplicate articles found.")
            return
        
        logger.info(f"Found {len(duplicates)} groups of duplicate articles.")
        
        total_deleted = 0
        
        for duplicate_group in tqdm(duplicates, desc="Removing duplicates"):
            ticker = duplicate_group["_id"]["ticker"]
            link = duplicate_group["_id"]["link"]
            count = duplicate_group["count"]
            doc_ids = duplicate_group["docs"]
            
            # Keep the first document and delete the rest
            ids_to_delete = doc_ids[1:]  # Skip the first one
            
            if ids_to_delete:
                result = self.collection.delete_many({"_id": {"$in": ids_to_delete}})
                deleted_count = result.deleted_count
                total_deleted += deleted_count
                
                logger.info(f"Removed {deleted_count} duplicates for ticker '{ticker}' with link: {link[:50]}...")
        
        logger.info(f"Duplicate removal completed. Total articles deleted: {total_deleted}")

    def clean_up_articles(self):
        """
        Cleans up the financial news collection by keeping only the most recent 100 articles for each ticker.
        """
        for asset in self.assets_metadata:
            ticker = asset["ticker"]

            article_count = self.collection.count_documents({"ticker": ticker})
            if article_count > 100:
                logger.info(f"Ticker {ticker} has {article_count} articles. Cleaning up...")

                articles_to_delete = self.collection.find({"ticker": ticker}) \
                                                    .sort("extraction_timestamp_utc", 1) \
                                                    .limit(article_count - 100)

                article_ids_to_delete = [article["_id"] for article in articles_to_delete]

                result = self.collection.delete_many({"_id": {"$in": article_ids_to_delete}})
                logger.info(f"Deleted {result.deleted_count} old articles for ticker {ticker}")
            else:
                logger.info(f"Ticker {ticker} has {article_count} articles. No clean up needed.")

    def run(self):
        """
        Runs the financial news scraper process to scrape news articles, create embeddings, sentiment scores, and clean up old articles.
        """
        # Scrape news articles
        logger.info("Scrape all tickers process started!")
        self.scrape_all_tickers()
        logger.info("Scrape all tickers process completed!")

        # Remove duplicates
        logger.info("Remove duplicates process started!")
        self.remove_duplicates()
        logger.info("Remove duplicates process completed!")

        # Embeddings
        logger.info("Process articles embeddings started!")
        self.process_articles_embeddings()
        logger.info("Process articles embeddings completed!")

        # Sentiment Score
        logger.info("Process articles sentiment scores started!")
        self.process_articles_sentiment_scores()
        logger.info("Process articles sentiment scores completed!")

        # Clean up articles older than 100 for each ticker
        logger.info("Clean up articles process started!")
        self.clean_up_articles()
        logger.info("Clean up articles process completed!")


# Example usage
if __name__ == "__main__":

    # Initialize the scraper
    scraper = FinancialNewsScraper(
        collection_name=os.getenv("NEWS_COLLECTION", "financial_news"),
        scrape_num_articles=int(os.getenv("SCRAPE_NUM_ARTICLES", 1))
    )
    scraper.run()
