import logging
from datetime import datetime, timedelta, timezone

from loaders.yfinance_tickers_extract import YFinanceTickersExtract
from loaders.yfinance_tickers_transform import YFinanceTickersTransform
from loaders.yfinance_tickers_load import YFinanceTickersLoad
from loaders.yfinance_tickers_cleaner import YFinanceTickersCleaner

from loaders.binance_api_extract import BinanceAPIExtract
from loaders.binance_api_transform import BinanceAPITransform
from loaders.binance_api_load import BinanceAPILoad
from loaders.binance_api_cleaner import BinanceAPICleaner

from loaders.subreddit_praw_wrapper import SubredditPrawWrapper
from loaders.subreddit_praw_embedder import SubredditPrawEmbedder
from loaders.subreddit_praw_sentiment import SubredditPrawSentiment
from loaders.subreddit_praw_cleaner import SubredditPrawCleaner

from loaders.financial_news_scraper import FinancialNewsScraper

from loaders.pyfredapi_macroindicators_extract import PyFredAPIExtract
from loaders.pyfredapi_macroindicators_transform import PyFredAPITransform
from loaders.pyfredapi_macroindicators_load import PyFredAPILoad

from loaders.portfolio_performance_load import PorfolioPerformanceLoad

from loaders.config.config_loader import ConfigLoader

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

class LoaderService:
    def __init__(self):
        """
        Service for ADHOC loading requests of market data, macroeconomic data, and financial news data.
        This service provides methods for:
        1. Load Yahoo Finance market data for a given date. (load_yfinance_market_data)
        2. Load Yahoo Finance market data for a given date and symbol. (load_yfinance_market_data_by_symbol)
        3. Load Binance API crypto data for a given date. (load_binance_api_crypto_data)
        4. Load Binance API crypto data for a given date and symbol. (load_binance_api_crypto_data_by_symbol)
        5. Load PyFredAPI macroeconomic data for a given date. (load_pyfredapi_macroeconomic_data)
        6. Load PyFredAPI macroeconomic data for a given date and series id. (load_pyfredapi_macroeconomic_data_by_series)
        7. Load Portfolio Performance yesterday data. (insert_portfolio_performance_yesterday_data)
        8. Load Portfolio Performance data for a given date. (insert_portfolio_performance_data_for_date)
        9. Backfill Yahoo Finance market data for a given date range. (backfill_yfinance_market_data)
        10. Backfill Yahoo Finance market data for a given date range and symbol. (backfill_yfinance_market_data_by_symbol)
        11. Backfill Binance API crypto data for a given date range. (backfill_binance_api_crypto_data)
        12. Backfill Binance API crypto data for a given date range and symbol. (backfill_binance_api_crypto_data_by_symbol)
        13. Backfill PyFredAPI macroeconomic data for a given date range. (backfill_pyfredapi_macroeconomic_data)
        14. Backfill PyFredAPI macroeconomic data for a given date range and series id. (backfill_pyfredapi_macroeconomic_data_by_series)
        15. Backfill Portfolio Performance data for a given date range. (backfill_portfolio_performance_data)
        16. Load recent financial news data. (load_recent_financial_news)
        17. Load recent Subreddit PRAW data. (load_recent_subreddit_praw_data)
        """ 
        self.config_loader = ConfigLoader()
        self.utc = timezone.utc
        logger.info("LoaderService initialized")

    ####################################
    # YFINANCE
    ####################################

    def load_yfinance_market_data(self, date_str: str):
        """
        Loads Yahoo Finance market data for the given date.

        :param date_str: Date in "%Y%m%d" format.
        """
        logger.info("Starting Yahoo Finance market data loading process")

        # Validate the date is not current date or future date
        current_date = datetime.now(self.utc).strftime("%Y%m%d")
        if date_str >= current_date:
            raise ValueError("date_str cannot be the current date or a future date")

        # Date string for end date
        start_date_str = date_str

        # Define date range for extraction
        start_date = datetime.strptime(start_date_str, "%Y%m%d").replace(tzinfo=self.utc)
        end_date = start_date + timedelta(days=1)
        end_date_str = end_date.strftime("%Y%m%d")

        logger.info(f"Extracting data for {start_date_str}")

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

        logger.info("Yahoo Finance market data loading process completed")

    def load_yfinance_market_data_by_symbol(self, date_str: str, symbol: str):
        """
        Loads Yahoo Finance market data for the given date and symbol.

        :param date_str: Date in "%Y%m%d" format.
        :param symbol: Ticker symbol.
        """
        logger.info(f"Starting Yahoo Finance market data loading process for symbol: {symbol}")

        # Validate the date is not current date or future date
        current_date = datetime.now(self.utc).strftime("%Y%m%d")
        if date_str >= current_date:
            raise ValueError("date_str cannot be the current date or a future date")

        # Define date range for extraction
        start_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=self.utc)
        end_date = start_date + timedelta(days=1)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        # Extract Market Data for the specific symbol
        extractor = YFinanceTickersExtract(start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract_single_ticker(symbol)

        if not extracted_data:
            logger.warning(f"No data extracted for symbol: {symbol}")
            return

        # Transform Market Data
        transformer = YFinanceTickersTransform()
        transformed_data = transformer.transform(symbol=symbol, df=extracted_data[symbol])

        # Load Market Data
        loader = YFinanceTickersLoad()
        loader.load({symbol: transformed_data}, start_date=start_date_str)

        logger.info(f"Yahoo Finance market data loading process completed for symbol: {symbol}")

    ####################################
    # BINANCE API
    ####################################

    def load_binance_api_crypto_data(self, date_str: str):
        """
        Loads Binance API crypto data for the given date.

        :param date_str: Date in "%Y%m%d" format.
        """
        logger.info("Starting Binance API crypto data loading process")

        # Validate the date is not current date or future date
        current_date = datetime.now(self.utc).strftime("%Y%m%d")
        if date_str >= current_date:
            raise ValueError("date_str cannot be the current date or a future date")

        # Date string for end date
        start_date_str = date_str

        # Define date range for extraction
        start_date = datetime.strptime(start_date_str, "%Y%m%d").replace(tzinfo=self.utc)
        end_date = start_date + timedelta(days=1)
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
                    transformed_df = transformer.transform(symbol=symbol, df=df)
                    transformed_data[symbol] = transformed_df
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

        logger.info("Binance API crypto data loading process completed")

    def load_binance_api_crypto_data_by_symbol(self, date_str: str, symbol: str):
        """
        Loads Binance API crypto data for the given date and symbol.

        :param date_str: Date in "%Y%m%d" format.
        :param symbol: Ticker symbol.
        """
        logger.info(f"Starting Binance API crypto data loading process for symbol: {symbol}")

        # Validate the date is not current date or future date
        current_date = datetime.now(self.utc).strftime("%Y%m%d")
        if date_str >= current_date:
            raise ValueError("date_str cannot be the current date or a future date")

        # Define date range for extraction
        start_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=self.utc)
        end_date = start_date + timedelta(days=1)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        # Extract Crypto Data for the specific symbol
        extractor = BinanceAPIExtract(start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract_single_ticker(symbol)

        if not extracted_data:
            logger.warning(f"No data extracted for symbol: {symbol}")
            return

        # Transform Crypto Data
        transformer = BinanceAPITransform()
        transformed_data = transformer.transform(symbol=symbol, df=extracted_data[symbol])

        # Load Crypto Data
        loader = BinanceAPILoad()
        loader.load({symbol: transformed_data}, start_date=start_date_str)

        logger.info(f"Binance API crypto data loading process completed for symbol: {symbol}")

    ####################################
    # PYFREDAPI MACROECONOMIC DATA
    ####################################

    def load_pyfredapi_macroeconomic_data(self, date_str: str):
        """
        Loads PyFredAPI macroeconomic data for the given date.

        :param date_str: Date in "%Y%m%d" format.
        """
        logger.info("Starting PyFredAPI macroeconomic data loading process")

        # Validate the date is not current date or future date
        current_date = datetime.now(self.utc).strftime("%Y%m%d")
        if date_str >= current_date:
            raise ValueError("date_str cannot be the current date or a future date")

        # Date string for end date
        end_date_str = date_str

        # Define date range for extraction
        end_date = datetime.strptime(end_date_str, "%Y%m%d").replace(tzinfo=self.utc)
        start_date = end_date - timedelta(days=7)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        # Extract Macroeconomic Data
        extractor = PyFredAPIExtract(start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract()

        # Transform Macroeconomic Data
        transformer = PyFredAPITransform()
        transformed_data = {}
        for series_id, df in extracted_data.items():
            transformed_data[series_id] = transformer.transform(series_id, df)

        # Load Macroeconomic Data
        loader = PyFredAPILoad()
        loader.load(transformed_data)

        logger.info("PyFredAPI macroeconomic data loading process completed")

    def load_pyfredapi_macroeconomic_data_by_series(self, date_str: str, series_id: str):
        """
        Loads PyFredAPI macroeconomic data for the given date and series ID.

        :param date_str: Date in "%Y%m%d" format.
        :param series_id: Series ID.
        """
        logger.info(f"Starting PyFredAPI macroeconomic data loading process for series ID: {series_id}")

        # Validate the date is not current date or future date
        current_date = datetime.now(self.utc).strftime("%Y%m%d")
        if date_str >= current_date:
            raise ValueError("date_str cannot be the current date or a future date")

        # Define date range for extraction
        end_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=self.utc)
        start_date = end_date - timedelta(days=7)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        # Extract Macroeconomic Data for the specific series ID
        extractor = PyFredAPIExtract(start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract_indicator(series_id)

        if extracted_data is None or extracted_data.empty:
            logger.warning(f"No data extracted for series ID: {series_id}")
            return

        # Transform Macroeconomic Data
        transformer = PyFredAPITransform()
        transformed_data = transformer.transform(series_id, extracted_data)

        # Load Macroeconomic Data
        loader = PyFredAPILoad()
        loader.load({series_id: transformed_data})

        logger.info(f"PyFredAPI macroeconomic data loading process completed for series ID: {series_id}")

    ####################################
    # PORTFOLIO PERFORMANCE
    ####################################

    def insert_portfolio_performance_yesterday_data(self) -> dict:
        """
        Loads portfolio performance data for yesterday.
        Checks if data already exists to prevent duplicates.

        Returns:
            dict: Status and result of the operation.
        """
        logger.info("Starting portfolio performance data loading for yesterday")
        
        loader = PorfolioPerformanceLoad()
        result = loader.insert_portfolio_performance_yesterday_data()
        
        if result["status"] == "exists":
            logger.info(f"Portfolio performance data for yesterday already exists, no action taken")
        else:
            logger.info(f"Portfolio performance data for yesterday successfully loaded")
            
        return result
        
    def insert_portfolio_performance_data_for_date(self, date_str: str) -> dict:
        """
        Loads portfolio performance data for a specific date.
        
        Args:
            date_str (str): Date in ISO format "YYYYMMDD" (e.g., "20250414")
            
        Returns:
            dict: Status and result of the operation.
        """
        logger.info(f"Starting portfolio performance data loading for date: {date_str}")
        
        # Validate date format
        try:
            datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            error_msg = f"Invalid date format: {date_str}. Please use YYYYMMDD format."
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
            
        loader = PorfolioPerformanceLoad()
        result = loader.insert_portfolio_performance_data_for_date(date_str)
        
        if result["status"] == "exists":
            logger.info(f"Portfolio performance data for {date_str} already exists, no action taken")
        elif result["status"] == "inserted":
            logger.info(f"Portfolio performance data for {date_str} successfully loaded")
        else:
            logger.warning(f"Portfolio performance data loading for {date_str} returned: {result}")
            
        return result
    
    ####################################
    # BACKFILL METHODS
    ####################################

    ####################################
    # YFINANCE
    ####################################
    
    def backfill_yfinance_market_data(self, start_date: str, end_date: str):
        """
        Backfills Yahoo Finance market data for the given date range.

        :param start_date: Start date in "%Y%m%d" format.
        :param end_date: End date in "%Y%m%d" format.
        """
        logger.info(f"Starting backfill for Yahoo Finance market data from {start_date} to {end_date}")
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            self.load_yfinance_market_data(date_str)
            current_date += timedelta(days=1)
        logger.info("Backfill for Yahoo Finance market data completed")

    def backfill_yfinance_market_data_by_symbol(self, start_date: str, end_date: str, symbol: str):
        """
        Backfills Yahoo Finance market data for the given date range and symbol.

        :param start_date: Start date in "%Y%m%d" format.
        :param end_date: End date in "%Y%m%d" format.
        :param symbol: Ticker symbol.
        """
        logger.info(f"Starting backfill for Yahoo Finance market data for symbol {symbol} from {start_date} to {end_date}")
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            self.load_yfinance_market_data_by_symbol(date_str, symbol)
            current_date += timedelta(days=1)
        logger.info(f"Backfill for Yahoo Finance market data for symbol {symbol} completed")

    ####################################
    # BINANCE API
    ####################################

    def backfill_binance_api_crypto_data(self, start_date: str, end_date: str):
        """
        Backfills Binance API crypto data for the given date range.

        :param start_date: Start date in "%Y%m%d" format.
        :param end_date: End date in "%Y%m%d" format.
        """
        logger.info(f"Starting backfill for Binance API crypto data from {start_date} to {end_date}")
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            self.load_binance_api_crypto_data(date_str)
            current_date += timedelta(days=1)
        logger.info("Backfill for Binance API crypto data completed")

    def backfill_binance_api_crypto_data_by_symbol(self, start_date: str, end_date: str, symbol: str):
        """
        Backfills Binance API crypto data for the given date range and symbol.

        :param start_date: Start date in "%Y%m%d" format.
        :param end_date: End date in "%Y%m%d" format.
        :param symbol: Ticker symbol.
        """
        logger.info(f"Starting backfill for Binance API crypto data for symbol {symbol} from {start_date} to {end_date}")
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            self.load_binance_api_crypto_data_by_symbol(date_str, symbol)
            current_date += timedelta(days=1)
        logger.info(f"Backfill for Binance API crypto data for symbol {symbol} completed")

    ####################################
    # PYFREDAPI MACROECONOMIC DATA
    ####################################

    def backfill_pyfredapi_macroeconomic_data(self, start_date: str, end_date: str):
        """
        Backfills PyFredAPI macroeconomic data for the given date range.

        :param start_date: Start date in "%Y%m%d" format.
        :param end_date: End date in "%Y%m%d" format.
        """
        logger.info(f"Starting backfill for PyFredAPI macroeconomic data from {start_date} to {end_date}")
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            self.load_pyfredapi_macroeconomic_data(date_str)
            current_date += timedelta(days=1)
        logger.info("Backfill for PyFredAPI macroeconomic data completed")

    def backfill_pyfredapi_macroeconomic_data_by_series(self, start_date: str, end_date: str, series_id: str):
        """
        Backfills PyFredAPI macroeconomic data for the given date range and series ID.

        :param start_date: Start date in "%Y%m%d" format.
        :param end_date: End date in "%Y%m%d" format.
        :param series_id: Series ID.
        """
        logger.info(f"Starting backfill for PyFredAPI macroeconomic data for series ID {series_id} from {start_date} to {end_date}")
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_date = datetime.strptime(end_date, "%Y%m%d")
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            self.load_pyfredapi_macroeconomic_data_by_series(date_str, series_id)
            current_date += timedelta(days=1)
        logger.info(f"Backfill for PyFredAPI macroeconomic data for series ID {series_id} completed")

    ####################################
    # PORTFOLIO PERFORMANCE
    ####################################

    def backfill_portfolio_performance_data(self, start_date: str, end_date: str) -> dict:
        """
        Backfills portfolio performance data for the given date range.
        
        Args:
            start_date (str): Start date in ISO format "YYYYMMDD" (e.g., "20250414") 
            end_date (str): End date in ISO format "YYYYMMDD" (e.g., "20250420")
            
        Returns:
            dict: Summary of the backfill operation.
        """
        # Validate date formats
        try:
            datetime.strptime(start_date, "%Y%m%d")
            datetime.strptime(end_date, "%Y%m%d")
        except ValueError as e:
            error_msg = f"Invalid date format. Please use YYYYMMDD format. Error: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
        
        # Initialize Portfolio Performance Loader and run backfill
        loader = PorfolioPerformanceLoad()
        result = loader.backfill_portfolio_performance_data(start_date, end_date)
        
        if result["status"] == "completed":
            logger.info(f"Backfill for portfolio performance data completed: {result['inserted_count']} inserted, {result['skipped_count']} skipped")
        else:
            logger.error(f"Backfill for portfolio performance data failed: {result}")
            
        return result
    
    ####################################
    # FINANCIAL NEWS
    ####################################

    def load_recent_financial_news(self):
        """
        Loads recent financial news data.
        """
        logger.info("Starting financial news processing")

        # Scraper
        news_scraper = FinancialNewsScraper(
            collection_name=os.getenv("NEWS_COLLECTION", "financial_news"),
            scrape_num_articles=int(os.getenv("SCRAPE_NUM_ARTICLES", 1))
        )
        news_scraper.run()

        logger.info("Financial News processing completed!")

    ####################################
    # SUBREDDIT PRAW
    ####################################

    def load_recent_subreddit_praw_data(self):
        """
        Loads recent Subreddit PRAW data.
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

    def subreddit_praw_embedder_only(self):
        """
        Runs only the Subreddit PRAW embedder to process data.
        This is useful for re-embedding existing data without re-fetching it.
        """
        logger.info("Starting Subreddit PRAW embedder only")

        # Embedder
        praw_embedder = SubredditPrawEmbedder()
        praw_embedder.run()

        logger.info("Subreddit PRAW embedder only completed!")

    def subreddit_praw_sentiment_only(self):
        """
        Runs only the Subreddit PRAW sentiment analysis to process data.
        This is useful for re-analyzing existing data without re-fetching it.
        """
        logger.info("Starting Subreddit PRAW sentiment analysis only")

        # Sentiment Analysis
        praw_sentiment = SubredditPrawSentiment()
        praw_sentiment.run()

        logger.info("Subreddit PRAW sentiment analysis only completed!")

    def subreddit_praw_cleaner_only(self):
        """
        Runs only the Subreddit PRAW cleaner to process data.
        This is useful for cleaning existing data without re-fetching or re-analyzing it.
        """
        logger.info("Starting Subreddit PRAW cleaner only")

        # Cleaner
        praw_cleaner = SubredditPrawCleaner()
        praw_cleaner.run()

        logger.info("Subreddit PRAW cleaner only completed!")