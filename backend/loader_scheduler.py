import time
import logging
import datetime as dt
from datetime import datetime, timedelta, timezone

from loaders.config.config_loader import ConfigLoader

from loaders.yfinance_tickers_extract import YFinanceTickersExtract
from loaders.yfinance_tickers_transform import YFinanceTickersTransform
from loaders.yfinance_tickers_load import YFinanceTickersLoad
from loaders.yfinance_tickers_cleaner import YFinanceTickersCleaner

from loaders.binance_api_extract import BinanceAPIExtract
from loaders.binance_api_transform import BinanceAPITransform
from loaders.binance_api_load import BinanceAPILoad
from loaders.binance_api_cleaner import BinanceAPICleaner

from loaders.pyfredapi_macroindicators_extract import PyFredAPIExtract
from loaders.pyfredapi_macroindicators_transform import PyFredAPITransform
from loaders.pyfredapi_macroindicators_load import PyFredAPILoad

from loaders.financial_news_scraper import FinancialNewsScraper

from loaders.portfolio_performance_load import PorfolioPerformanceLoad

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

    def run_binance_api_crypto_data_etl(self):
        """
        Runs the ETL process for Binance API crypto data: Extract, Transform, Load.
        """
        logger.info("Starting Binance API ETL process")

        # Define date range for extraction
        end_date = datetime.now(self.utc)
        start_date = end_date - timedelta(days=1)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        # Extract Crypto Data
        extractor = BinanceAPIExtract(start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract()

        # Transform Crypto Data
        transformer = BinanceAPITransform()
        transformed_data = {}
        
        # Access the 'crypto' key first to get the dictionary of symbols
        if 'crypto' in extracted_data and extracted_data['crypto']:
            for symbol, df in extracted_data['crypto'].items():
                try:
                    transformed_data[symbol] = transformer.transform(symbol=symbol, df=df)
                except Exception as e:
                    logger.error(f"Error transforming {symbol}: {e}")
        else:
            logger.warning("No crypto data was extracted")

        # Load Crypto Data
        if transformed_data:
            loader = BinanceAPILoad()
            loader.load(transformed_data, start_date=start_date_str)

            # Clean up Crypto Data older than 60 days
            cleaner = BinanceAPICleaner()
            cleaner.run()
        else:
            logger.warning("No data to load after transformation")

        logger.info("Binance API ETL process completed")

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

    def run_insert_portfolio_performance_yesterday_data(self):
        """
        Runs the daily portfolio performance data generation and insertion.
        Ensures yesterday's portfolio performance data is available.
        """
        logger.info("Starting portfolio performance data generation for yesterday")
        
        try:
            # Initialize Portfolio Performance Loader
            loader = PorfolioPerformanceLoad()
            
            # Insert yesterday's data if not already present
            result = loader.insert_portfolio_performance_yesterday_data()
            
            if result["status"] == "exists":
                logger.info("Yesterday's portfolio performance data already exists. No action needed.")
            else:
                logger.info(f"Generated and inserted portfolio performance data for yesterday: {result['date'].date()}")
            
            logger.info("Portfolio performance data processing completed!")
        except Exception as e:
            logger.error(f"Error during portfolio performance data processing: {str(e)}")

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

        # Schedule Binance API crypto data ETL process
        binance_api_crypto_data_etl_time = dt.time(hour=4, minute=3, tzinfo=timezone.utc)
        self.scheduler.daily(binance_api_crypto_data_etl_time, self.run_binance_api_crypto_data_etl)

        # Schedule PyFredAPI ETL process
        run_pyfredapi_macroeconomic_data_etl_time = dt.time(hour=4, minute=5, tzinfo=timezone.utc)
        self.scheduler.daily(run_pyfredapi_macroeconomic_data_etl_time, self.run_pyfredapi_macroeconomic_data_etl)

        # Schedule Portfolio Performance insert
        portfolio_performance_insert_time = dt.time(hour=4, minute=10, tzinfo=timezone.utc)
        self.scheduler.daily(portfolio_performance_insert_time, self.run_insert_portfolio_performance_yesterday_data)

        # Schedule financial news processing
        # NOTE: Financial news scraping process is disabled for now, a fixed dataset is used for this project.
        # The reasons are:
        # 1. To avoid unnecessary recurrent scraping of news articles during runtime.
        # 2. Reduce costs by minimizing Voyage AI requests for embeddings.
        # 3. Minimize delays on calculating sentiment scores.
        # 4. Have better control over user flow and experience.
        # In real-world scenarios, you will have your own dataset or data retrieval/ingestion process for financial news.
        ########################################################################
        # news_processing_time = dt.time(hour=4, minute=10, tzinfo=timezone.utc)
        # self.scheduler.weekly(trigger.Tuesday(news_processing_time), self.run_financial_news_processing)
        # self.scheduler.weekly(trigger.Wednesday(news_processing_time), self.run_financial_news_processing)
        # self.scheduler.weekly(trigger.Thursday(news_processing_time), self.run_financial_news_processing)
        # self.scheduler.weekly(trigger.Friday(news_processing_time), self.run_financial_news_processing)
        # self.scheduler.weekly(trigger.Saturday(news_processing_time), self.run_financial_news_processing)
        
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
