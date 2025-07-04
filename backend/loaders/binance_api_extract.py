import requests
import pandas as pd
from datetime import datetime, timezone
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


class BinanceAPIExtract(BaseExtract):
    def __init__(self, start_date: str, end_date: str, interval: str = "1m"):
        """
        Binance API tickers extractor.

        Args:
            start_date (str): Start date.
            end_date (str): End date.
            interval (str, optional): Interval. Defaults to "1m".
        """
        super().__init__(start_date, end_date)
        self.interval = interval
        self.base_url = "https://api.binance.com/api/v3/klines"

    def extract_tickers(self, tickers: str) -> dict:
        """
        Extract data for the specified Binance tickers.

        Args:
            tickers (str): Tickers in space-separated format.

        Returns:
            dict: Data frames for the specified tickers.
        """
        data_dict = {}
        for ticker in tickers.split():
            logger.info(f"Extracting data for Binance symbol: {ticker}")
            for attempt in range(3):
                try:
                    ticker_data = self._fetch_binance_data(ticker)
                    if ticker_data.empty:
                        logger.warning(f"No data returned for symbol: {ticker}")
                    else:
                        data_dict[ticker] = ticker_data
                        logger.info(f"Successfully extracted data for symbol: {ticker}")
                    break
                except Exception as e:
                    logger.error(f"Error extracting data for symbol {ticker}: {e}")
                    if attempt < 2:
                        logger.info(f"Retrying... ({attempt + 1}/3)")
                        logger.debug(f"Waiting 15 seconds before retrying for symbol {ticker}...")
                        time.sleep(15) # Wait 15 seconds before retrying
                    else:
                        logger.error(f"Failed to extract data for symbol {ticker} after 3 attempts.")
        return data_dict
    
    def _fetch_binance_data(self, symbol: str) -> pd.DataFrame:
        """
        Fetch kline data from Binance API for a specific symbol.
        
        Args:
            symbol (str): Binance trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            pd.DataFrame: DataFrame with OHLCV data
        """
        # If we have different start and end dates, use only the start date for a single day
        if self.dt and self.dt_end and self.dt.date() != self.dt_end.date():
            logger.warning(f"Request spans multiple days. Using only {self.dt.date()} for extraction.")
            # Force single day by setting end date to same as start date, but end of day
            # Make sure to include the FULL day - use 23:59:59
            start_datetime = self.dt.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0) \
                            if self.dt.tzinfo is None else self.dt.replace(hour=0, minute=0, second=0, microsecond=0)
            end_datetime = start_datetime.replace(hour=23, minute=59, second=59, microsecond=999000)
            start_time_ms = int(start_datetime.timestamp() * 1000)
            end_time_ms = int(end_datetime.timestamp() * 1000)
            single_day_request = True
        else:
            # Check if we're dealing with a single day request
            single_day_request = True if self.dt and self.dt_end and self.dt.date() == self.dt_end.date() else False
            
            # Ensure we have datetime objects with UTC timezone for correct timestamp conversion
            if self.dt:
                # Start time should be at 00:00:00
                start_datetime = self.dt.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0) \
                                if self.dt.tzinfo is None else self.dt.replace(hour=0, minute=0, second=0, microsecond=0)
                start_time_ms = int(start_datetime.timestamp() * 1000)
            else:
                start_time_ms = None
                start_datetime = None
                
            # Set up end datetime - ALWAYS use 23:59:59 to get the full day
            if self.dt_end:
                end_datetime = self.dt_end.replace(tzinfo=timezone.utc, hour=23, minute=59, second=59, microsecond=999000) \
                            if self.dt_end.tzinfo is None else self.dt_end.replace(hour=23, minute=59, second=59, microsecond=999000)
                end_time_ms = int(end_datetime.timestamp() * 1000)
            else:
                end_time_ms = None
                end_datetime = None
        
        logger.info(f"Fetching data for {symbol} from {start_datetime} to {end_datetime} (UTC)")
        logger.debug(f"Using timestamps: startTime={start_time_ms}, endTime={end_time_ms}")
        
        all_data = []
        current_start = start_time_ms
        
        # Continue fetching until we reach the end date
        while current_start and end_time_ms and current_start < end_time_ms:
            params = {
                "symbol": symbol,
                "interval": self.interval,
                "startTime": current_start,
                "limit": 1000  # Binance limit is 1000 per request
            }
            
            # Always include endTime to ensure we get all data up to the end
            if end_time_ms:
                params["endTime"] = end_time_ms
                    
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            chunk_data = response.json()
            
            if not chunk_data:
                logger.warning(f"No data returned for chunk starting at {datetime.fromtimestamp(current_start/1000, tz=timezone.utc)}")
                break
                        
            logger.debug(f"Retrieved {len(chunk_data)} candles for chunk starting at {datetime.fromtimestamp(current_start/1000, tz=timezone.utc)}")
            all_data.extend(chunk_data)
            
            # If we got fewer than 1000 records, we've reached the end of available data
            if len(chunk_data) < 1000:
                logger.debug(f"Reached end of available data with {len(chunk_data)} records in final chunk")
                break
            
            # Update start time for next chunk (add 1ms to last timestamp)
            current_start = int(chunk_data[-1][0]) + 1
            
            # Avoid hitting API rate limits
            time.sleep(0.1)
        
        if not all_data:
            logger.warning(f"No data retrieved for {symbol} between {start_datetime} and {end_datetime}")
            return pd.DataFrame()
        
        logger.info(f"Retrieved a total of {len(all_data)} candles for {symbol}")
                
        # Create dataframe from the data
        df = pd.DataFrame(all_data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
        ])
        
        # Convert to proper datatypes
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms', utc=True)
        df.set_index("timestamp", inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].astype(float)
        
        # Rename columns to match expected format
        df.columns = ["Open", "High", "Low", "Close", "Volume"]
        
        # Validate data completeness for daily extracts
        if single_day_request and self.interval == "1m":
            expected_count = 1440  # 24 hours * 60 minutes
            actual_count = len(df)
            if actual_count != expected_count:
                logger.warning(f"Expected {expected_count} minute candles for {symbol} on {start_datetime.date()}, but got {actual_count}")
                # If we're specifically looking for a single day of data, enforce exactly 1440 points
                if actual_count > expected_count:
                    logger.info(f"Trimming data to exactly 1440 points for {start_datetime.date()}")
                    # Keep only the first 1440 points
                    df = df.iloc[:1440]
                elif actual_count < expected_count:
                    missing_count = expected_count - actual_count
                    logger.warning(f"Missing {missing_count} data points for {symbol} on {start_datetime.date()}")
                    
                    # Show the time range we actually have
                    if not df.empty:
                        first_time = df.index[0]
                        last_time = df.index[-1]
                        logger.info(f"Data covers from {first_time} to {last_time}")
                        
                        # Check if we're missing data at the beginning or end
                        expected_start = pd.Timestamp(start_datetime.date(), tz='UTC')
                        expected_end = pd.Timestamp(start_datetime.date()) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
                        expected_end = expected_end.tz_localize('UTC')
                        
                        if first_time > expected_start:
                            logger.warning(f"Missing data at the beginning. First point is {first_time}, expected {expected_start}")
                        
                        if last_time < expected_end:
                            logger.warning(f"Missing data at the end. Last point is {last_time}, expected {expected_end}")
            else:
                logger.info(f"Successfully retrieved all {expected_count} minute candles for {symbol} on {start_datetime.date()}")
        
        return df
    
    def extract_single_ticker(self, ticker: str) -> dict:
        """
        Extract data for a single Binance ticker.

        Args:
            ticker (str): A single ticker symbol.

        Returns:
            dict: Data frame for the specified ticker, or None if extraction fails.
        """
        logger.info(f"Extracting data for single Binance symbol: {ticker}")
        for attempt in range(3):
            try:
                ticker_data = self._fetch_binance_data(ticker)
                if ticker_data.empty:
                    logger.warning(f"No data returned for symbol: {ticker}")
                else:
                    logger.info(f"Successfully extracted data for symbol: {ticker}")
                    return {ticker: ticker_data}
                break
            except Exception as e:
                logger.error(f"Error extracting data for symbol {ticker}: {e}")
                if attempt < 2:
                    logger.info(f"Retrying... ({attempt + 1}/3)")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to extract data for symbol {ticker} after 3 attempts.")
        logger.warning(f"Extraction failed for symbol: {ticker}")
        return None

    def extract_crypto_assets(self, assets: str) -> dict:
        """
        Extract data for crypto assets.

        Args:
            assets (str): Space-separated crypto asset symbols.

        Returns:
            dict: Data frames for the specified crypto assets.
        """
        logger.info("Extracting crypto assets data")
        data = self.extract_tickers(assets)
        logger.info("Successfully extracted crypto assets data")
        return data

    def extract(self):
        """
        Extract all Binance assets data from configuration.
        
        Returns:
            dict: Dictionary with all extracted data.
        """
        logger.info("Starting Binance data extract process")
        config_loader = ConfigLoader()

        # Load Binance assets configuration
        binance_assets = config_loader.get("BINANCE_ASSETS")
        
        # Extract crypto assets data
        df_crypto = self.extract_crypto_assets(binance_assets)

        logger.info("Binance data extract process completed")
        return {
            "crypto": df_crypto
        }


if __name__ == "__main__":
    extractor = BinanceAPIExtract(start_date="20250624", end_date="20250625")

    # Test single ticker extraction
    # data = extractor.extract_single_ticker("BTCUSDT")
    # print(data)

    # Extract all configured assets
    data = extractor.extract()
    print(data)