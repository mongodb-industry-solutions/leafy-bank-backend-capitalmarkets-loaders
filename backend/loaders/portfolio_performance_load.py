from datetime import datetime, timedelta, timezone
from loaders.db.mdb import MongoDBConnector
import logging
import os
import random
from pymongo import DESCENDING

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PorfolioPerformanceLoad(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, collection_name: str = os.getenv("PORTFOLIO_PERFORMANCE_COLLECTION", "portfolio_performance")):
        """
        Porfolio Performance loader for portfolio performance data.

        Args:
            uri (str, optional): MongoDB URI. Defaults to None.
            database_name (str, optional): Database name. Defaults to None.
            appname (str, optional): Application name. Defaults to None.
            collection_name (str, optional): Collection name. Defaults to "portfolio_performance".
        """
        super().__init__(uri, database_name, appname)
        self.collection_name = collection_name
        logger.info("PorfolioPerformanceLoad initialized")

    def insert_portfolio_performance_yesterday_data(self) -> dict:
        """
        Inserts portfolio performance data into the MongoDB collection.
        Generates random daily and cumulative returns for the previous day.
        Ensures no duplicate values are inserted.

        Returns:
            - dict: MongoDB insert result or status message.
        """
        # Get current date and calculate yesterday in UTC
        today = datetime.now(timezone.utc)
        yesterday = today - timedelta(days=1)
        # Create midnight UTC timestamp for yesterday
        yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day, 
                                  tzinfo=timezone.utc)
        
        # Check if data for yesterday already exists
        collection = self.db[self.collection_name]
        yesterday_data = collection.find_one({"date": yesterday_start})
        
        if yesterday_data:
            logger.info(f"Data for {yesterday_start.date()} already exists. No action needed.")
            return {"status": "exists", "date": yesterday_start}
        
        # Get the most recent record to determine ranges for random generation
        latest_record = collection.find_one(
            sort=[("date", DESCENDING)]
        )
        
        # Generate random values based on existing data
        if latest_record:
            # Generate daily return within reasonable range (-1.5% to +1.5%)
            daily_return = round(random.uniform(-1.5, 1.5), 2)
            
            # Calculate new cumulative return based on previous value
            prev_cumulative = latest_record.get("percentage_of_cumulative_return", 0)
            # Adjust cumulative by daily return with some randomness
            cumulative_return = round(prev_cumulative + daily_return * random.uniform(0.8, 1.2), 2)
        else:
            # If no previous data, initialize with reasonable values
            daily_return = round(random.uniform(-1.0, 1.0), 2)
            cumulative_return = daily_return
        
        # Prepare new document with UTC timestamp
        new_data = {
            "date": yesterday_start,
            "percentage_of_daily_return": daily_return,
            "percentage_of_cumulative_return": cumulative_return
        }
        
        # Insert the document
        insert_result = collection.insert_one(new_data)
        logger.info(f"Generated and inserted data for {yesterday_start.date()} UTC: {new_data}")
        
        return {
            "status": "inserted",
            "inserted_id": str(insert_result.inserted_id),
            "date": yesterday_start,
            "data": new_data
        }
    
    def insert_portfolio_performance_data_for_date(self, target_date_str: str) -> dict:
        """
        Inserts portfolio performance data for a specific date.
        Used primarily for backfilling data.

        Args:
            target_date_str (str): Date in ISO format "YYYYMMDD" (e.g., "20250414")

        Returns:
            - dict: MongoDB insert result or status message.
        """
        # Parse target date
        try:
            target_date = datetime.strptime(target_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError as e:
            error_msg = f"Invalid date format. Please use YYYYMMDD format. Error: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
        
        # Create midnight UTC timestamp for target date
        target_date_start = datetime(target_date.year, target_date.month, target_date.day, 
                                tzinfo=timezone.utc)
        
        # Check if data for target date already exists
        collection = self.db[self.collection_name]
        existing_data = collection.find_one({"date": target_date_start})
        
        if existing_data:
            logger.info(f"Data for {target_date_start.date()} already exists. No action needed.")
            return {"status": "exists", "date": target_date_start}
        
        # Get the most recent record before the target date
        latest_record = collection.find_one(
            {"date": {"$lt": target_date_start}},
            sort=[("date", DESCENDING)]
        )
        
        # Generate random values based on existing data
        if latest_record:
            # Generate daily return within reasonable range (-1.5% to +1.5%)
            daily_return = round(random.uniform(-1.5, 1.5), 2)
            
            # Calculate new cumulative return based on previous value
            prev_cumulative = latest_record.get("percentage_of_cumulative_return", 0)
            # Adjust cumulative by daily return with some randomness
            cumulative_return = round(prev_cumulative + daily_return * random.uniform(0.8, 1.2), 2)
        else:
            # If no previous data, initialize with reasonable values
            daily_return = round(random.uniform(-1.0, 1.0), 2)
            cumulative_return = daily_return
        
        # Prepare new document with UTC timestamp
        new_data = {
            "date": target_date_start,
            "percentage_of_daily_return": daily_return,
            "percentage_of_cumulative_return": cumulative_return
        }
        
        # Insert the document
        insert_result = collection.insert_one(new_data)
        logger.info(f"Generated and inserted data for {target_date_start.date()} UTC: {new_data}")
        
        return {
            "status": "inserted",
            "inserted_id": str(insert_result.inserted_id),
            "date": target_date_start,
            "data": new_data
        }
        
    def backfill_portfolio_performance_data(self, start_date_str: str, end_date_str: str) -> dict:
        """
        Backfills portfolio performance data for a specific date range.
        
        Args:
            start_date_str (str): Start date in ISO format "YYYYMMDD" (e.g., "20250414")
            end_date_str (str): End date in ISO format "YYYYMMDD" (e.g., "20250420")
            
        Returns:
            - dict: Summary of the backfill operation.
        """
        logger.info(f"Starting backfill of portfolio performance data from {start_date_str} to {end_date_str}")
        
        # Parse start and end dates
        try:
            start_date = datetime.strptime(start_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(end_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError as e:
            error_msg = f"Invalid date format. Please use YYYYMMDD format. Error: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
            
        if end_date < start_date:
            error_msg = "End date cannot be earlier than start date."
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
            
        # Initialize counters for reporting
        inserted_count = 0
        skipped_count = 0
        
        # Iterate through each date in the range
        current_date = start_date
        while current_date <= end_date:
            # Convert datetime to string format for the helper method
            current_date_str = current_date.strftime("%Y%m%d")
            result = self.insert_portfolio_performance_data_for_date(current_date_str)
            
            if result["status"] == "inserted":
                inserted_count += 1
            elif result["status"] == "exists":
                skipped_count += 1
                
            # Move to the next day
            current_date += timedelta(days=1)
            
        logger.info(f"Backfill completed: {inserted_count} dates inserted, {skipped_count} dates skipped")
        
        return {
            "status": "completed",
            "start_date": start_date,
            "end_date": end_date,
            "inserted_count": inserted_count,
            "skipped_count": skipped_count
        }
        

if __name__ == "__main__":
    # Example usage
    loader = PorfolioPerformanceLoad()
    
    # Example 1: Check and load yesterday's data
    result = loader.insert_portfolio_performance_yesterday_data()
    print(f"Portfolio Performance Load Result: {result}")
    
    # Example 2: Load data for a specific date
    # single_date_result = loader.insert_portfolio_performance_data_for_date("20250415")
    # print(f"Single Date Load Result: {single_date_result}")
    
    # Example 3: Backfill data for a date range
    # backfill_result = loader.backfill_portfolio_performance_data("20250410", "20250414")
    # print(f"Backfill Result: {backfill_result}")