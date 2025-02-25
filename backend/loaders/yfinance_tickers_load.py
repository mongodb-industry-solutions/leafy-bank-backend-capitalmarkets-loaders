import pandas as pd
from db.mongo_db import MongoDBConnector
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class YFinanceTickersLoad(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, collection_name: str = "yfinanceMarketData"):
        """
        Yahoo Finance tickers loader.

        Args:
            uri (str, optional): MongoDB URI. Defaults to None.
            database_name (str, optional): Database name. Defaults to None.
            appname (str, optional): Application name. Defaults to None.
            collection_name (str, optional): Collection name. Defaults to "yfinanceMarketData".
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name
        logger.info("YFinanceTickersLoad initialized")

    def delete_existing_data(self, symbol: str, date: pd.Timestamp):
        """
        Deletes existing data for the specified symbol and date.

        Parameters:
            - symbol (str): The symbol for which to delete data.
            - date (pd.Timestamp): The date for which to delete data.
        """
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        query = {
            "symbol": symbol,
            "timestamp": {
                "$gte": start_of_day,
                "$lt": end_of_day
            }
        }
        result = self.db[self.collection_name].delete_many(query)
        logger.info(f"Deleted {result.deleted_count} documents for symbol: {symbol} on date: {date.date()}")

    def insert_market_data(self, df: pd.DataFrame) -> dict:
        """
        Inserts the transformed DataFrame into the MongoDB time-series collection.

        Parameters:
            - df (pd.DataFrame): The transformed DataFrame containing market data.

        Returns:
            - dict: MongoDB insert result.
        """
        try:
            # Confirm timestamp is aware and in UTC
            if df['timestamp'].dt.tz is None:
                # If timestamp is naive, localize to UTC
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')
            else:
                # If timestamp is aware, convert to UTC
                df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

            # Delete existing data for the same symbol and date
            if not df.empty:
                symbol = df['symbol'].iloc[0]
                date = df['timestamp'].iloc[0]
                self.delete_existing_data(symbol, date)

            # Convert timestamp to Python datetime object (ensuring stored as ISODate)
            df['timestamp'] = df['timestamp'].apply(lambda x: x.to_pydatetime())

            # Convert DataFrame to list of dictionaries
            records = df.to_dict(orient="records")

            # Insert data into the collection
            result = self.db[self.collection_name].insert_many(records)

            return {"inserted_count": len(result.inserted_ids)}
        except Exception as e:
            logger.error(f"Error inserting data into collection {self.collection_name}: {e}")
            return {"inserted_count": 0}

    def load(self, data: dict):
        """
        Load data into MongoDB for each ticker.

        Args:
            data (dict): Dictionary of DataFrames for the specified tickers.
        """
        for symbol, df in data.items():
            if df.empty:
                logger.warning(f"No data to insert for symbol: {symbol}")
                continue
            logger.info(f"Inserting market data for symbol: {symbol}")
            try:
                result = self.insert_market_data(df)
                logger.info(f"Inserted {result['inserted_count']} documents for symbol: {symbol}")
            except Exception as e:
                logger.error(f"Error inserting data for symbol {symbol}: {e}")

if __name__ == "__main__":
    # Example usage
    sample_data = {
        'AAPL': pd.DataFrame({
            'timestamp': pd.date_range(start='2023-01-01', periods=5, freq='D', tz='UTC'),
            'open': [150, 152, 153, 155, 157],
            'high': [151, 153, 154, 156, 158],
            'low': [149, 151, 152, 154, 156],
            'close': [150, 152, 153, 155, 157],
            'volume': [1000, 1100, 1200, 1300, 1400],
            'symbol': ['AAPL'] * 5
        }),
        'MSFT': pd.DataFrame({
            'timestamp': pd.date_range(start='2023-01-01', periods=5, freq='D', tz='UTC'),
            'open': [250, 252, 253, 255, 257],
            'high': [251, 253, 254, 256, 258],
            'low': [249, 251, 252, 254, 256],
            'close': [250, 252, 253, 255, 257],
            'volume': [2000, 2100, 2200, 2300, 2400],
            'symbol': ['MSFT'] * 5
        })
    }

    yfinance_loader = YFinanceTickersLoad()
    yfinance_loader.load(sample_data)

    query = {"symbol": "AAPL"}
    cursor = yfinance_loader.db[yfinance_loader.collection_name].find(query).sort("timestamp", -1).limit(5)

    from pprint import pprint
    
    # Pretty-print the results
    for document in cursor:
        pprint(document)
    
    # Cleanup: Delete all sample data
    symbols = sample_data.keys()
    for symbol in symbols:
        delete_result = yfinance_loader.db[yfinance_loader.collection_name].delete_many({"symbol": symbol})
        logger.info(f"Deleted {delete_result.deleted_count} documents for symbol: {symbol}")