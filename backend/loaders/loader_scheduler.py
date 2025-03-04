import time
import logging
import datetime as dt
from datetime import datetime, timedelta, timezone

from yfinance_tickers_extract import YFinanceTickersExtract
from yfinance_tickers_transform import YFinanceTickersTransform
from yfinance_tickers_load import YFinanceTickersLoad

from financial_news_scraper import FinancialNewsScraper
from financial_news_embeddings import FinancialNewsEmbeddings
from financial_news_sentiment_score import FinancialNewsSentimentScore

from config.config_loader import ConfigLoader

from scheduler import Scheduler
import scheduler.trigger as trigger
import pytz

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

class LoaderScheduler:
    def __init__(self):
        """
        Scheduler for Yahoo Finance tickers ETL process and Financial News processing.
        """
        self.config_loader = ConfigLoader()
        self.utc = pytz.UTC
        self.scheduler = Scheduler(tzinfo=timezone.utc)
        logger.info("LoaderScheduler initialized")

    def run_etl(self):
        """
        Runs the ETL process: Extract, Transform, Load.
        """
        logger.info("Starting ETL process")

        # Define date range for extraction
        end_date = datetime.now(self.utc)
        start_date = end_date - timedelta(days=1)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        logger.info(f"Extracting data from {start_date_str} to {end_date_str}")

        # Extract data
        extractor = YFinanceTickersExtract(start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract()

        # Transform data
        transformer = YFinanceTickersTransform()
        transformed_data = {}
        for asset_type, data in extracted_data.items():
            logger.info(f"Transforming data for asset type: {asset_type}")
            for symbol, df in data.items():
                transformed_data[symbol] = transformer.transform(symbol=symbol, df=df)

        # Load data
        loader = YFinanceTickersLoad()
        loader.load(transformed_data, start_date=start_date_str)

        logger.info("ETL process completed")

    def run_financial_news_processing(self):
        """
        Runs the Financial News processing: Scrape, Embeddings, Sentiment Score.
        """
        logger.info("Starting financial news processing")

        # Scraper
        news_scraper = FinancialNewsScraper(
            collection_name=os.getenv("NEWS_COLLECTION", "financial_news"),
            scrape_num_articles=int(os.getenv("SCRAPE_NUM_ARTICLES", 3))
        )
        news_scraper.scrape_all_tickers()

        # Embeddings
        news_embeddings = FinancialNewsEmbeddings()
        news_embeddings.process_articles()

        # Sentiment Score
        news_sentiment_creator = FinancialNewsSentimentScore()
        news_sentiment_creator.process_articles()

        logger.info("Financial News processing completed!")

    def schedule_jobs(self):
        """
        Schedules the ETL process and financial news processing to run from Tuesday to Saturday using UTC time.
        """
        test_time_etl = dt.time(hour=10, minute=5, tzinfo=timezone.utc)
        test_time_news = dt.time(hour=10, minute=7, tzinfo=timezone.utc)

        self.scheduler.once(trigger.Tuesday(test_time_etl), self.run_etl)
        self.scheduler.once(trigger.Tuesday(test_time_news), self.run_financial_news_processing)

        etl_time = dt.time(hour=4, minute=0, tzinfo=timezone.utc)
        self.scheduler.weekly(trigger.Tuesday(etl_time), self.run_etl)
        self.scheduler.weekly(trigger.Wednesday(etl_time), self.run_etl)
        self.scheduler.weekly(trigger.Thursday(etl_time), self.run_etl)
        self.scheduler.weekly(trigger.Friday(etl_time), self.run_etl)
        self.scheduler.weekly(trigger.Saturday(etl_time), self.run_etl)

        # Schedule financial news processing
        news_processing_time = dt.time(hour=4, minute=15, tzinfo=timezone.utc)
        self.scheduler.weekly(trigger.Tuesday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Wednesday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Thursday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Friday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Saturday(news_processing_time), self.run_financial_news_processing)

        logger.info("Scheduled ETL and financial news processing jobs from Tuesday to Saturday using UTC time")

    def start(self):
        """
        Starts the scheduler.
        """
        self.schedule_jobs()
        logger.info("Schedule Jobs overview:")
        logger.info(self.scheduler)
        while True:
            self.scheduler.exec_jobs()
            time.sleep(1)

if __name__ == "__main__":
    scheduler = LoaderScheduler()
    scheduler.start()