import logging
from datetime import datetime, timedelta, timezone

from loaders.yfinance_tickers_extract import YFinanceTickersExtract
from loaders.yfinance_tickers_transform import YFinanceTickersTransform
from loaders.yfinance_tickers_load import YFinanceTickersLoad
from loaders.yfinance_tickers_cleaner import YFinanceTickersCleaner

from loaders.financial_news_scraper import FinancialNewsScraper

from loaders.pyfredapi_macroindicators_extract import PyFredAPIExtract
from loaders.pyfredapi_macroindicators_transform import PyFredAPITransform
from loaders.pyfredapi_macroindicators_load import PyFredAPILoad

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
        1. Loading Yahoo Finance market data for a given date. (load_yfinance_market_data)
        2. Loading Yahoo Finance market data for a given date and symbol. (load_yfinance_market_data_by_symbol)
        3. Loading PyFredAPI macroeconomic data for a given date. (load_pyfredapi_macroeconomic_data)
        4. Loading PyFredAPI macroeconomic data for a given date and series id. (load_pyfredapi_macroeconomic_data_by_series)
        5. Backfilling Yahoo Finance market data for a given date range. (backfill_yfinance_market_data)
        6. Backfilling Yahoo Finance market data for a given date range and symbol. (backfill_yfinance_market_data_by_symbol)
        7. Backfilling PyFredAPI macroeconomic data for a given date range. (backfill_pyfredapi_macroeconomic_data)
        8. Backfilling PyFredAPI macroeconomic data for a given date range and series id. (backfill_pyfredapi_macroeconomic_data_by_series)
        9. Loading recent financial news data. (load_recent_financial_news)
        """ 
        self.config_loader = ConfigLoader()
        self.utc = timezone.utc
        logger.info("LoaderService initialized")

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
