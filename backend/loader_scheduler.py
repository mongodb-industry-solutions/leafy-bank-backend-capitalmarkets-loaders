import time
import logging
import datetime as dt
from datetime import datetime, timedelta, timezone

from loaders.config.config_loader import ConfigLoader

from loaders.yfinance_tickers_extract import YFinanceTickersExtract
from loaders.yfinance_tickers_transform import YFinanceTickersTransform
from loaders.yfinance_tickers_load import YFinanceTickersLoad
from loaders.yfinance_tickers_cleaner import YFinanceTickersCleaner

from loaders.pyfredapi_macroindicators_extract import PyFredAPIExtract
from loaders.pyfredapi_macroindicators_transform import PyFredAPITransform
from loaders.pyfredapi_macroindicators_load import PyFredAPILoad

from loaders.financial_news_scraper import FinancialNewsScraper

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

    def run_yfinance_market_data_etl(self):
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

        # Extract Market Data
        extractor = YFinanceTickersExtract(start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract()

        # Transform Market Data
        transformer = YFinanceTickersTransform()
        transformed_data = {}
        for asset_type, data in extracted_data.items():
            logger.info(f"Transforming data for asset type: {asset_type}")
            for symbol, df in data.items():
                transformed_data[symbol] = transformer.transform(symbol=symbol, df=df)

        # Load Market Data
        loader = YFinanceTickersLoad()
        loader.load(transformed_data, start_date=start_date_str)

        # Clean up Market Data older than 60 days
        cleaner = YFinanceTickersCleaner()
        cleaner.run()

        logger.info("YFinanceTickers ETL process completed")

    def run_pyfredapi_macroeconomic_data_etl(self):
        """
        Runs the ETL process for PyFredAPI macroeconomic data: Extract, Transform, Load.
        """
        logger.info("Starting PyFredAPI ETL process")

        # Define date range for extraction
        start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y%m%d")
        end_date = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y%m%d")

        # Extract Macroeconomic Data
        extractor = PyFredAPIExtract(start_date=start_date, end_date=end_date)
        extracted_data = extractor.extract()

        # Transform Macroeconomic Data
        transformer = PyFredAPITransform()
        transformed_data = {}
        for series_id, df in extracted_data.items():
            transformed_data[series_id] = transformer.transform(series_id, df)

        # Load Macroeconomic Data
        loader = PyFredAPILoad()
        loader.load(transformed_data)

        logger.info("PyFredAPI ETL process completed")

    def run_financial_news_processing(self):
        """
        Runs the Financial News processing: Scrape, Embeddings, Sentiment Score.
        """
        logger.info("Starting financial news processing")

        # Scraper
        news_scraper = FinancialNewsScraper(
            collection_name=os.getenv("NEWS_COLLECTION", "financial_news"),
            scrape_num_articles=int(os.getenv("SCRAPE_NUM_ARTICLES", 1))
        )
        news_scraper.run()

        logger.info("Financial News processing completed!")

    def schedule_jobs(self):
        """
        Schedules the ETL process and financial news processing to run from Tuesday to Saturday using UTC time.
        """

        # Schedule Yahoo Finance tickers ETL process
        yfinance_market_data_etl_time = dt.time(hour=4, minute=0, tzinfo=timezone.utc)
        self.scheduler.weekly(trigger.Tuesday(yfinance_market_data_etl_time), self.run_yfinance_market_data_etl)
        self.scheduler.weekly(trigger.Wednesday(yfinance_market_data_etl_time), self.run_yfinance_market_data_etl)
        self.scheduler.weekly(trigger.Thursday(yfinance_market_data_etl_time), self.run_yfinance_market_data_etl)
        self.scheduler.weekly(trigger.Friday(yfinance_market_data_etl_time), self.run_yfinance_market_data_etl)
        self.scheduler.weekly(trigger.Saturday(yfinance_market_data_etl_time), self.run_yfinance_market_data_etl)

        # Schedule PyFredAPI ETL process
        run_pyfredapi_macroeconomic_data_etl_time = dt.time(hour=4, minute=5, tzinfo=timezone.utc)
        self.scheduler.daily(run_pyfredapi_macroeconomic_data_etl_time, self.run_pyfredapi_macroeconomic_data_etl)

        # Schedule financial news processing
        news_processing_time = dt.time(hour=4, minute=10, tzinfo=timezone.utc)
        self.scheduler.weekly(trigger.Tuesday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Wednesday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Thursday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Friday(news_processing_time), self.run_financial_news_processing)
        self.scheduler.weekly(trigger.Saturday(news_processing_time), self.run_financial_news_processing)
        
        logger.info("Scheduled jobs configured!")

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
