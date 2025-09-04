import pandas as pd
from datetime import datetime, time, timedelta
import pytz
from loaders.db.mdb import MongoDBConnector
import logging
import os
from bson import json_util

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BinanceAPILoad(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, collection_name: str = os.getenv("BINANCE_TIMESERIES_COLLECTION")):
        """
        Binance API data loader.

        Args:
            uri (str, optional): MongoDB URI. Defaults to None.
            database_name (str, optional): Database name. Defaults to None.
            appname (str, optional): Application name. Defaults to None.
            collection_name (str, optional): Collection name. Defaults to "binanceCryptoData".
        """
        super().__init__(uri, database_name, collection_name, appname)
        self.collection_name = collection_name
        logger.info("BinanceAPILoad initialized")

    def delete_existing_data(self, symbol: str, date: datetime):
        """
        Deletes existing data for the specified symbol and date.

        Parameters:
            - symbol (str): The symbol for which to delete data.
            - date (datetime): The date for which to delete data.
        """
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        query = {
            "symbol": symbol,
            "timestamp": {
                "$gte": start_of_day,
                "$lt": end_of_day
            }
        }
        result = self.db[self.collection_name].delete_many(query)
        logger.info(f"Deleted {result.deleted_count} documents for symbol: {symbol} on date: {date.date()}")

    def normalize_symbol(self, ticker: str) -> str:
        """
        Normalizes the given ticker symbol by removing the suffix if present.

        Parameters:
            - ticker (str): The ticker symbol to be normalized.
        
        Returns:
            - str: Normalized ticker symbol.
        """
        # Remove 'USDT' suffix if present
        if ticker.endswith("USDT"):
            return ticker[:-4]  # Remove last 4 characters (USDT)
        return ticker

    def insert_crypto_data(self, df: pd.DataFrame) -> dict:
        """
        Inserts the transformed DataFrame into the MongoDB time-series collection.
        Adds a new attribute date_load_iso_utc with the current UTC timestamp.

        Parameters:
            - df (pd.DataFrame): The transformed DataFrame containing crypto data.

        Returns:
            - dict: MongoDB insert result.
        """
        try:
            # Ensure timestamp is aware and in UTC
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')
            else:
                df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

            # Delete existing data for the same symbol and date
            if not df.empty:
                symbol = df['symbol'].iloc[0]
                date = df['timestamp'].iloc[0]
                self.delete_existing_data(symbol, date)

            # Convert timestamp to Python datetime object (for ISODate storage)
            df['timestamp'] = df['timestamp'].apply(lambda x: x.to_pydatetime())

            # Add the new field date_load_iso_utc with current UTC timestamp
            current_utc = datetime.now(pytz.UTC)
            df['date_load_iso_utc'] = current_utc.strftime("%Y%m%d%H%M%S")

            # Remove the _id field from each document to avoid duplication
            records = df.to_dict(orient="records")
            for record in records:
                if '_id' in record:
                    del record['_id']

            # Insert data into the collection
            result = self.db[self.collection_name].insert_many(records)
            return {"inserted_count": len(result.inserted_ids)}
        except Exception as e:
            logger.error(f"Error inserting data into collection {self.collection_name}: {e}")
            return {"inserted_count": 0}

    def recover_last_day_data(self, symbol: str, start_date: str) -> pd.DataFrame:
        """
        Recovers the last available data from MongoDB for the given symbol.
        Adjusts the date part of each document's timestamp to the provided start_date 
        (format "YYYYMMDD") while preserving the time.
        Also adds the date_load_iso_utc field.

        Parameters:
            - symbol (str): The symbol for which to recover data.
            - start_date (str): The new date (format "YYYYMMDD") to assign.
        
        Returns:
            pd.DataFrame: A DataFrame with the recovered and adjusted data.
        """
        # Find the most recent document
        doc = self.db[self.collection_name].find_one({"symbol": symbol}, sort=[("timestamp", -1)])
        if not doc:
            logger.warning(f"No previous data found in DB for symbol: {symbol}")
            return pd.DataFrame()
        
        last_date = doc["timestamp"].date()
        # Retrieve all documents for that last date
        start_last_day = datetime.combine(last_date, time(0, 0, 0, tzinfo=pytz.UTC))
        end_last_day = start_last_day + timedelta(days=1)
        cursor = self.db[self.collection_name].find({
            "symbol": symbol,
            "timestamp": {
                "$gte": start_last_day,
                "$lt": end_last_day
            }
        })
        data = list(cursor)
        if not data:
            logger.warning(f"No documents found for symbol: {symbol} on last available day: {last_date}")
            return pd.DataFrame()
        df = pd.DataFrame(data)

        # Adjust the date part to the provided start_date while preserving time
        new_date = datetime.strptime(start_date, "%Y%m%d").date()
        def adjust_timestamp(ts):
            return datetime.combine(new_date, ts.time(), tzinfo=ts.tzinfo)
        df['timestamp'] = df['timestamp'].apply(adjust_timestamp)

        # Add the new field date_load_iso_utc with current UTC timestamp
        current_utc = datetime.now(pytz.UTC)
        df['date_load_iso_utc'] = current_utc.strftime("%Y%m%d%H%M%S")

        # Remove the _id field from each document to avoid duplication
        df = df.drop(columns=['_id'], errors='ignore')

        logger.info(f"Recovered {len(df)} documents for symbol: {symbol} from last available day {last_date} adjusted to date {new_date}")
        return df

    def recover_day_data_from_backup(self, symbol: str, start_date: str) -> pd.DataFrame:
        """
        Recovers data from the backup JSON file for the given symbol.
        Adjusts the date part of each document's timestamp to the provided start_date 
        (format "YYYYMMDD") while preserving the time.
        Also adds the date_load_iso_utc field.

        Parameters:
            - symbol (str): The symbol for which to recover data.
            - start_date (str): The new date (format "YYYYMMDD") to assign.
        
        Returns:
            pd.DataFrame: A DataFrame with the recovered and adjusted data.
        """
        backup_dir = "./backend/loaders/backup"
        filename = os.path.join(backup_dir, f"bkp_day_data_{symbol}.json")
        
        if not os.path.exists(filename):
            logger.warning(f"No backup file found for symbol: {symbol}")
            return pd.DataFrame()

        with open(filename, "r") as f:
            data = json_util.loads(f.read())

        if not data:
            logger.warning(f"No data found in backup file for symbol: {symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Adjust the date part to the provided start_date while preserving time
        new_date = datetime.strptime(start_date, "%Y%m%d").date()
        def adjust_timestamp(ts):
            if isinstance(ts, dict) and '$date' in ts:
                ts = pd.to_datetime(ts['$date'])
            return datetime.combine(new_date, ts.time(), tzinfo=ts.tzinfo)
        df['timestamp'] = df['timestamp'].apply(adjust_timestamp)

        # Add the new field date_load_iso_utc with current UTC timestamp
        current_utc = datetime.now(pytz.UTC)
        df['date_load_iso_utc'] = current_utc.strftime("%Y%m%d%H%M%S")

        logger.info(f"Recovered {len(df)} documents for symbol: {symbol} from backup file adjusted to date {new_date}")
        return df

    def load(self, data: dict, start_date: str):
        """
        Load data into MongoDB for each crypto symbol. If the DataFrame for a symbol
        is empty and start_date is provided, attempt to recover the last available data
        for that symbol and adjust its date to start_date. If that fails, attempt to recover
        from the backup JSON file.

        Args:
            data (dict): Dictionary of DataFrames for the specified crypto symbols.
            start_date (str): Start date (format "YYYYMMDD").
        """
        logger.info(f"Loading data using start_date {start_date}.")
        for symbol, df in data.items():
            if df.empty and start_date:
                logger.warning(f"No new data for symbol: {symbol}. Attempting recovery using start_date {start_date}.")
                df = self.recover_last_day_data(symbol, start_date)
                if df.empty:
                    logger.warning(f"Recovery using start_date {start_date} failed for symbol: {symbol}. Attempting recovery from backup.")
                    df = self.recover_day_data_from_backup(symbol, start_date)
                    if df.empty:
                        logger.error(f"Recovery from backup failed for symbol: {symbol}.")
                        continue
            elif df.empty:
                logger.warning(f"No data to insert for symbol: {symbol}")
                continue

            logger.info(f"Inserting crypto data for symbol: {self.normalize_symbol(ticker=symbol)}")
            try:
                result = self.insert_crypto_data(df)
                start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
                logger.info(f"Inserted {result['inserted_count']} documents for symbol: {self.normalize_symbol(ticker=symbol)} on date {start_date_formatted}")
            except Exception as e:
                logger.error(f"Error inserting data for symbol {symbol}: {e}")

if __name__ == "__main__":
    # Example usage with the Binance API pipeline
    from binance_api_extract import BinanceAPIExtract
    from binance_api_transform import BinanceAPITransform
    import pandas as pd
    
    # Extract data
    start_extraction_date = "20250624"  # Format: YYYYMMDD
    end_extraction_date = "20250625"  # Format: YYYYMMDD
    extractor = BinanceAPIExtract(start_date=start_extraction_date, end_date=end_extraction_date)
    extracted_data = extractor.extract()
    
    if 'crypto' in extracted_data and extracted_data['crypto']:
        # Transform data
        transformer = BinanceAPITransform()
        
        # Process each symbol individually for the loader
        transformed_data = {}
        for symbol, df in extracted_data['crypto'].items():
            transformed_df = transformer.transform(symbol, df)
            transformed_data[symbol] = transformed_df
            
        # Load data
        loader = BinanceAPILoad()
        loader.load(transformed_data, start_date=start_extraction_date)
        
        # Query and print sample of loaded data
        logger.info("Sample of loaded data:")
        for symbol in transformed_data.keys():
            normalized_symbol = loader.normalize_symbol(symbol)
            cursor = loader.db[loader.collection_name].find(
                {"symbol": normalized_symbol}
            ).sort("timestamp", -1).limit(3)
            
            for doc in cursor:
                logger.info(f"{normalized_symbol} data point: {doc['timestamp']} - Open: {doc['open']}, Close: {doc['close']}")
                
        logger.info("Example completed successfully!")
    else:
        logger.warning("No crypto data was extracted. Check the extraction process.")

    # Uncomment to clean up test data
    # for symbol in extracted_data.get('crypto', {}).keys():
    #     normalized_symbol = loader.normalize_symbol(symbol)
    #     delete_result = loader.db[loader.collection_name].delete_many({"symbol": normalized_symbol})
    #     logger.info(f"Deleted {delete_result.deleted_count} documents for symbol: {normalized_symbol}")