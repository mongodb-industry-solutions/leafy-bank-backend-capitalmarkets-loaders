import logging
from datetime import datetime, timedelta
from yfinance_tickers_extract import YFinanceTickersExtract
from yfinance_tickers_transform import YFinanceTickersTransform
from yfinance_tickers_load import YFinanceTickersLoad
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class YFinanceTickers2Weeks:
    def __init__(self):
        """
        Initializes the YFinanceTickers2Weeks class.
        """
        self.utc = pytz.UTC
        logger.info("YFinanceTickers2Weeks initialized")

    def run_etl_for_date(self, date):
        """
        Runs the ETL process for a specific date.
        """
        logger.info(f"Starting ETL process for date: {date}")

        # Define date range for extraction
        start_date_str = (date - timedelta(days=1)).strftime("%Y%m%d")
        end_date_str = date.strftime("%Y%m%d")

        logger.info(f"Extracting data from {start_date_str} to {end_date_str}")

        # Extract data
        extractor = YFinanceTickersExtract(
            start_date=start_date_str, end_date=end_date_str)
        extracted_data = extractor.extract()

        # Transform data
        transformer = YFinanceTickersTransform()
        transformed_data = {}
        for asset_type, data in extracted_data.items():
            logger.info(f"Transforming data for asset type: {asset_type}")
            for symbol, df in data.items():
                transformed_data[symbol] = transformer.transform(
                    symbol=symbol, df=df)

        # Load data
        loader = YFinanceTickersLoad()
        loader.load(transformed_data, start_date=start_date_str)

        logger.info(f"ETL process completed for date: {date}")

    def run_etl_for_last_2_weeks(self):
        """
        Runs the ETL process for the last 15 days, ending with the previous day.
        """
        end_date = datetime.now(self.utc) - timedelta(days=1)
        start_date = end_date - timedelta(days=14)

        current_date = start_date
        while current_date <= end_date:
            self.run_etl_for_date(current_date)
            current_date += timedelta(days=1)


if __name__ == "__main__":
    etl_runner = YFinanceTickers2Weeks()
    etl_runner.run_etl_for_last_2_weeks()
