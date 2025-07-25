import yfinance as yf
from loaders.config.config_loader import ConfigLoader
from loaders.base_extract import BaseExtract
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class YFinanceTickersExtract(BaseExtract):
    def __init__(self, start_date: str, end_date: str, interval: str = "1m"):
        """
        Yahoo Finance tickers extractor.

        Args:
            start_date (str): Start date.
            end_date (str): End date.
            interval (str, optional): Interval. Defaults to "1m".
        """
        super().__init__(start_date, end_date)
        self.interval = interval

    def extract_tickers(self, tickers: str) -> dict:
        """
        Extract data for the specified tickers.

        Args:
            tickers (str): Tickers.

        Returns:
            dict: Data frames for the specified tickers.
        """
        data_frames = {}
        for ticker in tickers.split():
            logger.info(f"Extracting data for ticker: {ticker}")
            for attempt in range(3):
                try:
                    ticker_data = yf.Ticker(ticker).history(
                        start=self.dt.strftime("%Y-%m-%d") if self.dt else None,
                        end=self.dt_end.strftime("%Y-%m-%d") if self.dt_end else None,
                        interval=self.interval
                    )
                    # Retain only the specified columns
                    ticker_data = ticker_data.filter(
                        items=['Open', 'High', 'Low', 'Close', 'Volume'])
                    if ticker_data.empty:
                        logger.warning(f"No data returned for ticker: {ticker}")
                    else:
                        data_frames[ticker] = ticker_data
                        logger.info(
                            f"Successfully extracted data for ticker: {ticker}")
                    break
                except Exception as e:
                    logger.error(
                        f"Error extracting data for ticker {ticker}: {e}")
                    if attempt < 2:
                        logger.info(f"Retrying... ({attempt + 1}/3)")
                        time.sleep(2)
                    else:
                        logger.error(
                            f"Failed to extract data for ticker {ticker} after 3 attempts.")
        return data_frames
    
    def extract_single_ticker(self, ticker: str) -> dict:
        """
        Extract data for a single ticker.

        Args:
            ticker (str): A single ticker symbol.

        Returns:
            dict: Data frame for the specified ticker, or None if extraction fails.
        """
        logger.info(f"Extracting data for single ticker: {ticker}")
        for attempt in range(3):
            try:
                ticker_data = yf.Ticker(ticker).history(
                    start=self.dt.strftime("%Y-%m-%d") if self.dt else None,
                    end=self.dt_end.strftime("%Y-%m-%d") if self.dt_end else None,
                    interval=self.interval
                )
                # Retain only the specified columns
                ticker_data = ticker_data.filter(
                    items=['Open', 'High', 'Low', 'Close', 'Volume'])
                if ticker_data.empty:
                    logger.warning(f"No data returned for ticker: {ticker}")
                else:
                    logger.info(f"Successfully extracted data for ticker: {ticker}")
                    return {ticker: ticker_data}
                break
            except Exception as e:
                logger.error(f"Error extracting data for ticker {ticker}: {e}")
                if attempt < 2:
                    logger.info(f"Retrying... ({attempt + 1}/3)")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to extract data for ticker {ticker} after 3 attempts.")
        logger.warning(f"Extraction failed for ticker: {ticker}")
        return None

    def extract_equities(self, equities: str) -> dict:
        """
        Extract data for equities.

        Args:
            equities (str): Equities.

        Returns:
            dict: Data frames for the specified equities.
        """
        logger.info("Extracting equities data")
        data = self.extract_tickers(equities)
        logger.info("Successfully extracted equities data")
        return data

    def extract_bonds(self, bonds: str) -> dict:
        """
        Extract data for bonds.

        Args:
            bonds (str): Bonds.

        Returns:
            dict: Data frames for the specified bonds.
        """
        logger.info("Extracting bonds data")
        data = self.extract_tickers(bonds)
        logger.info("Successfully extracted bonds data")
        return data
    
    def extract_real_estate(self, real_estate: str) -> dict:
        """
        Extract data for real_estate.

        Args:
            real_estate (str): Real Estate.

        Returns:
            dict: Data frames for the specified real estate.
        """
        logger.info("Extracting real estate data")
        data = self.extract_tickers(real_estate)
        logger.info("Successfully extracted real estate data")
        return data

    def extract_commodities(self, commodities: str) -> dict:
        """
        Extract data for commodities.

        Args:
            commodities (str): Commodities.

        Returns:
            dict: Data frames for the specified commodities.
        """
        logger.info("Extracting commodities data")
        data = self.extract_tickers(commodities)
        logger.info("Successfully extracted commodities data")
        return data

    def extract_market_volatility(self, market_volatility: str) -> dict:
        """
        Extract data for market volatility.

        Args:
            market_volatility (str): Market volatility.

        Returns:
            dict: Data frames for the specified market volatility.
        """
        logger.info("Extracting market volatility data")
        data = self.extract_tickers(market_volatility)
        logger.info("Successfully extracted market volatility data")
        return data

    def extract(self):
        logger.info("Starting data extract process")
        config_loader = ConfigLoader()

        # Load configurations
        equities = config_loader.get("EQUITIES")
        bonds = config_loader.get("BONDS")
        real_estate = config_loader.get("REAL_ESTATE")
        commodities = config_loader.get("COMMODITIES")
        market_volatility = config_loader.get("MARKET_VOLATILITY")

        # Extract data for each asset type
        df_equities = self.extract_equities(equities)
        df_bonds = self.extract_bonds(bonds)
        df_real_estate = self.extract_real_estate(real_estate)
        df_commodities = self.extract_commodities(commodities)
        df_market_volatility = self.extract_market_volatility(market_volatility)

        logger.info("Data extract process completed")
        return {
            "equities": df_equities,
            "bonds": df_bonds,
            "real_estate": df_real_estate,
            "commodities": df_commodities,
            "market_volatility": df_market_volatility
        }


if __name__ == "__main__":
    extractor = YFinanceTickersExtract(start_date="20250625", end_date="20250626")

    # data = extractor.extract_single_ticker("VNQ")
    # print(data)

    data = extractor.extract()
    print(data)