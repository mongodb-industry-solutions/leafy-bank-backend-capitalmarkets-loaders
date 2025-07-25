from datetime import datetime, timezone
from loaders.db.mdb import MongoDBConnector
import logging
import os
import requests
import time
import pandas as pd
import numpy as np
from pymongo import DESCENDING

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class CoingeckoStablecoinMarketCap(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, collection_name: str = os.getenv("COINGECKO_STABLECOIN_COLLECTION", "stablecoin_market_caps")):
        """
        Coingecko Stablecoin Market Cap extractor.

        Args:
            uri (str, optional): MongoDB URI. Defaults to None.
            database_name (str, optional): Database name. Defaults to None.
            appname (str, optional): Application name. Defaults to None.
            collection_name (str, optional): Collection name. Defaults to "stablecoin_market_caps".
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name
        self.base_url = "https://api.coingecko.com/api/v3/coins/markets"
        
        logger.info("CoingeckoStablecoinMarketCap initialized")
    
    def fetch_all_stablecoins(self):
        """
        Fetch all stablecoin data from CoinGecko API.
        
        Returns:
            pd.DataFrame: DataFrame containing stablecoin data
        """
        logger.info("Fetching stablecoin data from CoinGecko API")
        all_data = []
        
        for page in range(1, 6):  # Fetch up to 5 pages
            params = {
                "vs_currency": "usd",
                "category": "stablecoins",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": page
            }
            
            for attempt in range(3):  # Retry up to 3 times
                try:
                    response = requests.get(self.base_url, params=params)
                    response.raise_for_status()
                    data = response.json()
                    all_data.extend(data)
                    logger.info(f"Fetched page {page} with {len(data)} coins")
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed for page {page}: {e}")
                    if attempt < 2:
                        time.sleep(2)
                    else:
                        logger.error(f"Failed to fetch page {page} after 3 attempts")
            
            # If we got less than 250 results, we've reached the end
            if len(data) < 250:
                break
        
        # Convert to DataFrame
        df = pd.DataFrame([{
            "ID": coin["id"],
            "Symbol": coin["symbol"].upper(),
            "Name": coin["name"],
            "Market Cap": coin["market_cap"]
        } for coin in all_data if coin["market_cap"] is not None])
        
        logger.info(f"Fetched {len(df)} stablecoins from CoinGecko")
        return df
    
    def get_selected_coins(self):
        """
        Get the list of selected coins from MongoDB collection.
        
        Returns:
            set: Set of (Symbol, Name) tuples for selected coins
        """
        logger.info("Retrieving selected coins from MongoDB collection")
        selected_coins = set()
        
        # Get unique coins from the collection (excluding ALL_STABLECOINS)
        pipeline = [
            {"$match": {"Symbol": {"$ne": "ALL_STABLECOINS"}}},
            {"$group": {"_id": {"Symbol": "$Symbol", "Name": "$Name"}}},
            {"$project": {"Symbol": "$_id.Symbol", "Name": "$_id.Name", "_id": 0}}
        ]
        
        coins = list(self.db[self.collection_name].aggregate(pipeline))
        
        for coin in coins:
            selected_coins.add((coin["Symbol"], coin["Name"]))
        
        logger.info(f"Retrieved {len(selected_coins)} selected coins from MongoDB")
        return selected_coins
    
    def get_last_market_caps(self):
        """
        Retrieve the last available market cap data from MongoDB for each coin.
        
        Returns:
            dict: Dictionary mapping (Symbol, Name) to last market cap value
        """
        logger.info("Retrieving last market cap data from MongoDB")
        last_market_caps = {}
        
        # Get the latest date available in the collection
        latest_doc = list(self.db[self.collection_name].find().sort("Date", DESCENDING).limit(1))
        if not latest_doc:
            logger.warning("No previous data found in MongoDB collection")
            return {}
        
        latest_date = latest_doc[0]["Date"]
        logger.info(f"Latest date in collection: {latest_date}")
        
        # Get all documents from the latest date
        latest_docs = list(self.db[self.collection_name].find({"Date": latest_date}))
        
        for doc in latest_docs:
            key = (doc["Symbol"], doc["Name"])
            last_market_caps[key] = doc["Market Cap"]
        
        logger.info(f"Retrieved last market cap data for {len(last_market_caps)} coins")
        return last_market_caps
    
    def get_trend_direction(self, change):
        """
        Determine trend direction based on percentage change.
        
        Args:
            change (float): Percentage change value
            
        Returns:
            str: Trend direction ('up', 'down', or 'equal')
        """
        if pd.isna(change) or change == 0:
            return "equal"
        elif change > 0:
            return "up"
        else:
            return "down"
    
    def process_stablecoin_data(self):
        """
        Process stablecoin data: fetch current data, compare with last data, and calculate trends.
        
        Returns:
            pd.DataFrame: Processed stablecoin data with trends
        """
        # Fetch current data
        df_today = self.fetch_all_stablecoins()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        df_today["Date"] = today_str
        
        # Get last market cap data from MongoDB
        last_market_caps = self.get_last_market_caps()
        
        # Get selected coins from MongoDB
        selected_coins = self.get_selected_coins()
        
        # Calculate trends
        df_today["Market Cap_Yesterday"] = df_today.apply(
            lambda row: last_market_caps.get((row["Symbol"], row["Name"]), row["Market Cap"]),
            axis=1
        )
        
        df_today["Trend (%)"] = (
            (df_today["Market Cap"] - df_today["Market Cap_Yesterday"]) /
            df_today["Market Cap_Yesterday"].replace(0, np.nan) * 100
        ).round(2)
        
        df_today["Trend direction"] = df_today["Trend (%)"].apply(self.get_trend_direction)
        
        # Calculate ALL STABLECOINS aggregate
        total_today = df_today["Market Cap"].sum()
        total_yesterday = df_today["Market Cap_Yesterday"].sum()
        total_trend = ((total_today - total_yesterday) / total_yesterday * 100) if total_yesterday > 0 else 0
        total_trend_direction = self.get_trend_direction(total_trend)
        
        aggregate_row = pd.DataFrame([{
            "Date": today_str,
            "Symbol": "ALL_STABLECOINS",
            "Name": "All Stablecoins",
            "Market Cap": total_today,
            "Trend (%)": round(total_trend, 2),
            "Trend direction": total_trend_direction
        }])
        
        # Filter only selected coins
        df_filtered = df_today[df_today.apply(
            lambda row: (row["Symbol"], row["Name"]) in selected_coins,
            axis=1
        )]
        
        # Combine aggregate with selected coins
        df_final = pd.concat([
            aggregate_row,
            df_filtered[["Date", "Symbol", "Name", "Market Cap", "Trend (%)", "Trend direction"]]
        ])
        
        # Clean up and sort
        df_final = df_final.drop_duplicates(subset=["Symbol", "Name"])
        df_final = df_final.sort_values(by="Market Cap", ascending=False)
        
        logger.info(f"Processed {len(df_final)} stablecoin records")
        return df_final
    
    def store_data(self, df):
        """
        Store processed stablecoin data in MongoDB.
        
        Args:
            df (pd.DataFrame): DataFrame containing processed stablecoin data
        """
        logger.info("Storing stablecoin data in MongoDB")
        
        # Convert DataFrame to records
        records = df.to_dict('records')
        
        # Convert Date string to datetime object
        for record in records:
            record["Date"] = datetime.strptime(record["Date"], "%Y-%m-%d")
        
        try:
            # Insert records
            if records:
                result = self.db[self.collection_name].insert_many(records)
                logger.info(f"Successfully stored {len(result.inserted_ids)} stablecoin records")
            else:
                logger.warning("No records to store")
        except Exception as e:
            logger.error(f"Error storing stablecoin data: {e}")
            raise
    
    def run_daily_extraction(self) -> str:
        """
        Run the daily stablecoin market cap extraction process.
        """
        logger.info("Starting daily stablecoin market cap extraction")
        
        try:
            # Check if data for today already exists
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            today_date = datetime.strptime(today_str, "%Y-%m-%d")
            
            existing_count = self.db[self.collection_name].count_documents({"Date": today_date})
            if existing_count > 0:
                logger.info(f"Data for {today_str} already exists ({existing_count} records). Skipping extraction.")
                return "Data for today already exists"
            
            # Process and store data
            df_processed = self.process_stablecoin_data()
            self.store_data(df_processed)
            
            logger.info("Daily stablecoin market cap extraction completed successfully")
            return "Daily extraction completed successfully"
            
        except Exception as e:
            logger.error(f"Error during daily extraction: {e}")
            raise

if __name__ == "__main__":
    extractor = CoingeckoStablecoinMarketCap()
    extractor.run_daily_extraction()