import pytz
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class YFinanceTickersTransform:
    def __init__(self):
        """
        Yahoo Finance tickers transformer.
        """
        logger.info("YFinanceTickersTransform initialized")

    def normalize_symbol(self, ticker: str) -> str:
        """
        Normalizes the given ticker symbol by removing special character.

        Parameters:
            - ticker (str): The ticker symbol to be normalized.
        
        Returns:
            - str: Normalized ticker symbol.
        """
        # Remove leading '^' if present.
        if ticker.startswith("^"):
            ticker = ticker.lstrip("^")
        return ticker

    def transform(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the given DataFrame by performing the following operations:
            - Reset the index (Datetime) and rename it to 'timestamp'.
            - Add a new column 'symbol' with the given value.
            - Convert all column names to lowercase.
            - Reorder columns as 'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'.
            - Convert 'timestamp' to UTC timezone if not already.
        
        Parameters:
            - symbol (str): The symbol name to be added in the 'symbol' column.
            - df (pd.DataFrame): The input DataFrame with 'Datetime' as the index.
        
        Returns:
            - pd.DataFrame: Transformed DataFrame.
        """
        logger.info(f"Transforming data for symbol: {symbol}")
        try:
            # Normalize the symbol
            symbol = self.normalize_symbol(symbol)

            # Reset index and explicitly rename 'Datetime' to 'timestamp'
            df = df.reset_index().rename(columns={'Datetime': 'timestamp'})

            # Convert timestamp to datetime if not already
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Ensure timestamp is localized to the correct timezone if it’s naive (has no timezone)
            if df['timestamp'].dt.tz is None:
                if symbol in ["^VIX", "VIX-USD", "VIX"]:
                    timezone = pytz.timezone('US/Central')
                else:
                    timezone = pytz.timezone('US/Eastern')
                df['timestamp'] = df['timestamp'].dt.tz_localize(
                    timezone, ambiguous='NaT', nonexistent='shift_forward')

            # Convert 'timestamp' to UTC
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

            # Add the 'symbol' column
            df['symbol'] = symbol

            # Convert all column names to lowercase
            df.columns = df.columns.str.lower()

            # Reorder columns
            df = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]

            logger.info(f"Successfully transformed data for symbol: {symbol}")
            return df
        except Exception as e:
            logger.error(f"Error transforming data for symbol {symbol}: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    sample_data = {
        'AAPL': pd.DataFrame({
            'Datetime': pd.date_range(start='2023-01-01', periods=5, freq='D', tz='US/Eastern'),
            'Open': [150, 152, 153, 155, 157],
            'High': [151, 153, 154, 156, 158],
            'Low': [149, 151, 152, 154, 156],
            'Close': [150, 152, 153, 155, 157],
            'Volume': [1000, 1100, 1200, 1300, 1400]
        }),
        'MSFT': pd.DataFrame({
            'Datetime': pd.date_range(start='2023-01-01', periods=5, freq='D', tz='US/Eastern'),
            'Open': [250, 252, 253, 255, 257],
            'High': [251, 253, 254, 256, 258],
            'Low': [249, 251, 252, 254, 256],
            'Close': [250, 252, 253, 255, 257],
            'Volume': [2000, 2100, 2200, 2300, 2400]
        }),
        '^VIX': pd.DataFrame({
            'Datetime': pd.date_range(start='2023-01-01', periods=5, freq='D', tz='US/Central'),
            'Open': [15.14, 15.16, 15.18, 15.20, 15.22],
            'High': [15.16, 15.18, 15.20, 15.22, 15.24],
            'Low': [15.12, 15.14, 15.16, 15.18, 15.20],
            'Close': [15.14, 15.16, 15.18, 15.20, 15.22],
            'Volume': [0, 0, 0, 0, 0]
        })
    }

    transformer = YFinanceTickersTransform()
    for symbol, df in sample_data.items():
        transformed_df = transformer.transform(symbol, df)
        logger.info(f"Transformed data for symbol: {symbol}")
        logger.info(f"\n{transformed_df.head()}")