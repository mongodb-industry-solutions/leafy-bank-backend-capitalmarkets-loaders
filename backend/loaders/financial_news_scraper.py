import logging
import re
import requests
from time import sleep
from bs4 import BeautifulSoup
from loaders.config.config_loader import ConfigLoader
from loaders.db.mongo_db import MongoDBConnector
from loaders.generic_scraper import GenericScraper
from datetime import datetime, timezone

from tqdm import tqdm
from pymongo import UpdateOne
from loaders.embeddings.bedrock.cohere_embeddings import BedrockCohereEnglishEmbeddings

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
        # Get the database collection
        self.collection = self.mongo_client.get_collection(collection_name=collection_name)
        # Instantiate the embeddings model
        self.embeddings_model = BedrockCohereEnglishEmbeddings(
            region_name=os.getenv("AWS_REGION"),
            aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        # Load the FinBERT model and tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        # Initialize the sentiment analysis pipeline
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=self.model, tokenizer=self.tokenizer, return_all_scores=True)

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

    def get_tickers(self):
        """
        Loads tickers from the configuration file and normalizes them.
        """
        logger.info("Loading tickers from configuration")
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

    def scrape_articles(self, search_query):
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
                article = self.extract_article(card, search_query)
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
        Scrape news articles for a list of tickers.

        Args:
            tickers: list
        """
        tickers = self.get_tickers()
        logger.info(f"Scraping news for tickers: {tickers}")

        for ticker in tickers:
            logger.info(f"Scraping news for ticker: {ticker}")
            try:
                self.scrape_articles(ticker)
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
        collection = self.collection
        # Filter to find articles that do not have sentiment_score attribute
        articles = list(collection.find(
            {"sentiment_score": {"$exists": False}}))
        logger.info("Found %d unprocessed articles.", len(articles))

        if not articles:
            logger.info("No articles to process.")
            return

        operations = []
        for article in tqdm(articles, desc="Processing articles"):
            article_string = article.get("article_string", "")
            sentiment_scores = self.get_sentiment_scores(article_string)
            if sentiment_scores is None:
                logger.error(
                    "Skipping article with _id: %s due to sentiment analysis error.", article.get("_id"))
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
            logger.info("Bulk update result: Matched: %d, Modified: %d",
                        result.matched_count, result.modified_count)
        else:
            logger.info("No articles were updated.")

    def clean_up_articles(self):
        """
        Cleans up the financial news collection by keeping only the most recent 100 articles for each ticker.
        """
        collection = self.collection
        tickers = self.get_tickers()

        for ticker in tickers:
            # Find the number of articles for the ticker
            article_count = collection.count_documents({"ticker": ticker})
            if article_count > 100:
                logger.info(
                    f"Ticker {ticker} has {article_count} articles. Cleaning up...")

                # Find the articles to delete, keeping the most recent 100
                articles_to_delete = collection.find({"ticker": ticker}).sort(
                    "extraction_timestamp_utc", 1).limit(article_count - 100)
                article_ids_to_delete = [article["_id"]
                                         for article in articles_to_delete]

                # Delete the old articles
                result = collection.delete_many(
                    {"_id": {"$in": article_ids_to_delete}})
                logger.info(
                    f"Deleted {result.deleted_count} old articles for ticker {ticker}")

            else:
                logger.info(
                    f"Ticker {ticker} has {article_count} articles. No clean up needed.")

    def run(self):
        """
        Runs the financial news scraper process to scrape news articles, create embeddings, sentiment scores, and clean up old articles.
        """
        # Scrape news articles
        logger.info("Scrape all tickers process started!")
        self.scrape_all_tickers()
        logger.info("Scrape all tickers process completed!")

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
