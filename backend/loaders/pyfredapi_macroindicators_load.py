import pandas as pd
from datetime import datetime
from loaders.db.mongo_db import MongoDBConnector
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PyFredAPILoad(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, collection_name: str = os.getenv("PYFREDAPI_COLLECTION", "pyfredapiMacroeconomicIndicators")):
        """
        PyFredAPI loader for macroeconomic indicators.

        Args:
            uri (str, optional): MongoDB URI. Defaults to None.
            database_name (str, optional): Database name. Defaults to None.
            appname (str, optional): Application name. Defaults to None.
            collection_name (str, optional): Collection name. Defaults to "pyfredapiMacroeconomicIndicators".
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name
        logger.info("PyFredAPILoad initialized")

    def insert_macroeconomic_data(self, df: pd.DataFrame) -> dict:
        """
        Inserts the transformed DataFrame into the MongoDB collection.
        Ensures no duplicate values are inserted.

        Parameters:
            - df (pd.DataFrame): The transformed DataFrame containing macroeconomic data.

        Returns:
            - dict: MongoDB insert result.
        """
        if df.empty:
            logger.warning("No data to insert. DataFrame is empty.")
            return {"inserted_count": 0}

        try:
            # Ensure date is in ISODate format and value is in Double format
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = df['value'].astype(float)

            # Get the most recent date and value from the DataFrame
            most_recent_date = df['date'].max()
            most_recent_value = df.loc[df['date'] == most_recent_date, 'value'].values[0]

            # Check if the most recent data is already in the collection
            query = {
                "series_id": df['series_id'].iloc[0],
                "date": {"$gte": most_recent_date}
            }
            existing_data = self.db[self.collection_name].find_one(query)

            if existing_data:
                logger.info(f"Data for series_id {df['series_id'].iloc[0]} on or after {most_recent_date} already exists. Skipping insertion.")
                return {"inserted_count": 0}

            # Convert date to Python datetime object (for ISODate storage)
            df['date'] = df['date'].apply(lambda x: x.to_pydatetime())

            # Remove the _id field from each document to avoid duplication
            records = df.to_dict(orient="records")
            for record in records:
                if '_id' in record:
                    del record['_id']

            # Insert data into the collection
            result = self.db[self.collection_name].insert_many(records)
            logger.info(f"Inserted {len(result.inserted_ids)} documents into collection {self.collection_name}")
            for record in records:
                logger.info(f"Inserted document with date: {record['date']} and value: {record['value']}")
            return {"inserted_count": len(result.inserted_ids)}
        except Exception as e:
            logger.error(f"Error inserting data into collection {self.collection_name}: {e}")
            return {"inserted_count": 0}

    def load(self, data: dict):
        """
        Load data into MongoDB for each macroeconomic indicator.

        Args:
            data (dict): Dictionary of DataFrames for the specified macroeconomic indicators.
        """
        logger.info("Loading macroeconomic data into MongoDB.")
        for series_id, df in data.items():
            if df.empty:
                logger.warning(f"No data to insert for series_id: {series_id}")
                continue

            logger.info(f"Inserting macroeconomic data for series_id: {series_id}")
            try:
                result = self.insert_macroeconomic_data(df)
                logger.info(f"Inserted {result['inserted_count']} documents for series_id: {series_id}")
            except Exception as e:
                logger.error(f"Error inserting data for series_id {series_id}: {e}")

if __name__ == "__main__":
    # Example usage
    sample_data = {
        'GDP': pd.DataFrame({
            'title': ["Gross Domestic Product"],
            'series_id': ["GDP"],
            'frequency': ["Quarterly"],
            'frequency_short': ["Q"],
            'units': ["Billions of Dollars"],
            'units_short': ["Bil. of $"],
            'date': [datetime(2024, 10, 1)],
            'value': [29719.647]
        }),
        'REAINTRATREARAT10Y': pd.DataFrame({
            'title': ["10-Year Real Interest Rate"],
            'series_id': ["REAINTRATREARAT10Y"],
            'frequency': ["Monthly"],
            'frequency_short': ["M"],
            'units': ["Percent"],
            'units_short': ["%"],
            'date': [datetime(2025, 2, 1)],
            'value': [2.031282]
        }),
        'UNRATE': pd.DataFrame({
            'title': ["Unemployment Rate"],
            'series_id': ["UNRATE"],
            'frequency': ["Monthly"],
            'frequency_short': ["M"],
            'units': ["Percent"],
            'units_short': ["%"],
            'date': [datetime(2025, 1, 1)],
            'value': [4]
        })
    }

    loader = PyFredAPILoad()
    loader.load(sample_data)