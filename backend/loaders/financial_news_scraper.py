import re
import requests
from time import sleep
from bs4 import BeautifulSoup
from loaders.config.config_loader import ConfigLoader
from loaders.db.mongo_db import MongoDBConnector
from loaders.generic_scraper import GenericScraper
from datetime import datetime, timezone

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


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
            logging.info(f"Inserted {len(articles)} articles into MongoDB.")

        return articles
    
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

    def scrape_all_tickers(self):
        """
        Scrape news articles for a list of tickers.

        Args:
            tickers: list
        """
        tickers = self.get_tickers()
        logging.info(f"Scraping news for tickers: {tickers}")

        for ticker in tickers:
            logging.info(f"Scraping news for ticker: {ticker}")
            try:
                self.scrape_articles(ticker)
            except Exception as e:
                logging.error(f"Error while scraping news for {ticker}: {e}")


# Example usage
if __name__ == "__main__":

    # Initialize the scraper
    scraper = FinancialNewsScraper(
        collection_name=os.getenv("NEWS_COLLECTION", "financial_news"),
        scrape_num_articles=int(os.getenv("SCRAPE_NUM_ARTICLES", 1))
    )

    scraper.scrape_all_tickers()