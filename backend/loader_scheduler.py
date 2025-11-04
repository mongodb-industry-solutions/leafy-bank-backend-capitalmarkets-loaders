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

from loaders.subreddit_praw_wrapper import SubredditPrawWrapper
from loaders.subreddit_praw_embedder import SubredditPrawEmbedder
from loaders.subreddit_praw_sentiment import SubredditPrawSentiment
from loaders.subreddit_praw_cleaner import SubredditPrawCleaner

from loaders.portfolio_performance_load import PorfolioPerformanceLoad

from loaders.coingecko_stablecoin_market_cap import CoingeckoStablecoinMarketCap

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

    def run_financial_news_extraction(self):
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

    def run_subreddit_praw_data_processing(self):
        """
        Run the Subreddit PRAW data processing pipeline:
        1. Wrapper: Fetches data from Reddit using PRAW.
        2. Embedder: Generates embeddings for the fetched data.
        3. Sentiment Analysis: Analyzes sentiment of the data.
        4. Cleaner: Cleans up data older than 60 days while ensuring at least 40 documents per asset are preserved.
        """
        logger.info("Starting Subreddit PRAW data processing")

        # Wrapper
        praw_wrapper = SubredditPrawWrapper()
        praw_wrapper.run()

        # Embedder
        praw_embedder = SubredditPrawEmbedder()
        praw_embedder.run()

        # Sentiment Analysis
        praw_sentiment = SubredditPrawSentiment()
        praw_sentiment.run()

        # Cleaner
        praw_cleaner = SubredditPrawCleaner()
        praw_cleaner.run()

        logger.info("Subreddit PRAW data processing completed!")

    def run_subreddit_praw_data_embedder_only(self):
        """
        Run only the Subreddit PRAW data embedder.
        This is useful for re-embedding existing data without fetching new data.
        """
        logger.info("Starting Subreddit PRAW data embedder only")

        # Embedder
        praw_embedder = SubredditPrawEmbedder()
        praw_embedder.run()

        logger.info("Subreddit PRAW data embedder completed!")

    def run_subreddit_praw_data_sentiment_only(self):
        """
        Run only the Subreddit PRAW data sentiment analysis.
        This is useful for re-analyzing sentiment of existing data without fetching new data.
        """
        logger.info("Starting Subreddit PRAW data sentiment analysis only")

        # Sentiment Analysis
        praw_sentiment = SubredditPrawSentiment()
        praw_sentiment.run()

        logger.info("Subreddit PRAW data sentiment analysis completed!")

    def run_subreddit_praw_data_cleaner_only(self):
        """
        Run only the Subreddit PRAW data cleaner.
        This is useful for cleaning up existing data without fetching new data.
        """
        logger.info("Starting Subreddit PRAW data cleaner only")

        # Cleaner
        praw_cleaner = SubredditPrawCleaner()
        praw_cleaner.run()

        logger.info("Subreddit PRAW data cleaner completed!")

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

    def run_coingecko_stablecoin_market_cap_data(self):
        """
        Runs the daily extraction of Coingecko Stablecoin Market Cap data.
        """
        logger.info("Starting Coingecko Stablecoin Market Cap data extraction")
        try:
            # Initialize Coingecko Stablecoin Market Cap Extractor
            extractor = CoingeckoStablecoinMarketCap()
            # Run the daily extraction
            result = extractor.run_daily_extraction()
            logger.info(f"Coingecko Stablecoin Market Cap message: {result}")
        except Exception as e:
            logger.error(f"Error during Coingecko Stablecoin Market Cap data extraction: {str(e)}")

    def schedule_jobs(self):
        """
        Schedules the ETL process and financial news processing to run from Tuesday to Saturday using UTC time.
        Jobs are only scheduled when NODE_ENV == "prod". For "dev" and "staging" environments, scheduling is skipped.
        """
        # Get NODE_ENV to determine if jobs should be scheduled
        node_env = os.getenv("NODE_ENV", "").lower()
        
        # Only schedule jobs in production environment
        if node_env != "prod":
            logger.info(f"Skipping job scheduling - NODE_ENV={node_env} (jobs only run in 'prod' environment)")
            logger.info("Scheduled jobs configured! (no jobs scheduled)")
            return
        
        logger.info(f"Scheduling jobs for production environment (NODE_ENV={node_env})")
        
        # Schedule Financial News processing on Mondays
        # NOTE: It runs only once a week on Mondays at 04:00 UTC. The reasons are:
        # 1. To avoid unnecessary recurrent scraping of news articles. For some assets, there may not be enough news articles to scrape daily.
        # 2. Reduce costs by minimizing Voyage AI requests for embeddings.
        # 3. Minimize delays on calculating sentiment scores.
        # 4. Have better control over user flow and experience.
        # In real-world scenarios, you will have your own dataset or data retrieval/ingestion process for financial news.
        financial_news_extraction_time = dt.time(hour=4, minute=0, tzinfo=timezone.utc)
        self.scheduler.weekly(trigger.Monday(financial_news_extraction_time), self.run_financial_news_extraction)

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

        # Schedule Portfolio Performance insert
        portfolio_performance_insert_time = dt.time(hour=4, minute=10, tzinfo=timezone.utc)
        self.scheduler.daily(portfolio_performance_insert_time, self.run_insert_portfolio_performance_yesterday_data)

        # Schedule Coingecko Stablecoin Market Cap data extraction
        coingecko_stablecoin_market_cap_time = dt.time(hour=4, minute=15, tzinfo=timezone.utc)
        self.scheduler.daily(coingecko_stablecoin_market_cap_time, self.run_coingecko_stablecoin_market_cap_data)

        # Schedule Subreddit PRAW data processing
        subreddit_praw_data_processing_time = dt.time(hour=4, minute=20, tzinfo=timezone.utc)
        self.scheduler.daily(subreddit_praw_data_processing_time, self.run_subreddit_praw_data_processing)

        # Schedule Subreddit PRAW data embedder only
        # This job is scheduled to ensure embeddings are generated for new data.
        subreddit_praw_data_embedder_only_time = dt.time(hour=4, minute=40, tzinfo=timezone.utc)
        self.scheduler.daily(subreddit_praw_data_embedder_only_time, self.run_subreddit_praw_data_embedder_only)

        # Schedule Subreddit PRAW data sentiment analysis only
        # This job is scheduled to ensure sentiment analysis is performed on the latest data.
        subreddit_praw_data_sentiment_only_time = dt.time(hour=4, minute=45, tzinfo=timezone.utc)
        self.scheduler.daily(subreddit_praw_data_sentiment_only_time, self.run_subreddit_praw_data_sentiment_only)

        # Schedule Subreddit PRAW data cleaner only
        # This job is scheduled to ensure that the data is cleaned up regularly.
        subreddit_praw_data_cleaner_only_time = dt.time(hour=5, minute=0, tzinfo=timezone.utc)
        self.scheduler.daily(subreddit_praw_data_cleaner_only_time, self.run_subreddit_praw_data_cleaner_only)

        ############################################################################
        # NOTE: Configuring this later than the previous jobs to ensure that the data is available on Binance API.
        # Schedule Binance API crypto data ETL process
        binance_api_crypto_data_etl_time = dt.time(hour=5, minute=10, tzinfo=timezone.utc)
        self.scheduler.daily(binance_api_crypto_data_etl_time, self.run_binance_api_crypto_data_etl)
        
        logger.info("Scheduled jobs configured!")

    def start(self):
        """
        Starts the scheduler.
        """
        self.schedule_jobs()
        
        # Get NODE_ENV to log appropriate message
        node_env = os.getenv("NODE_ENV", "").lower()
        
        if node_env == "prod":
            logger.info("Schedule Jobs overview:")
            logger.info(self.scheduler)
        else:
            logger.info(f"Scheduler started but no jobs scheduled (NODE_ENV={node_env}). API endpoints remain available for manual data loading.")
        
        while True:
            self.scheduler.exec_jobs()
            time.sleep(1)
