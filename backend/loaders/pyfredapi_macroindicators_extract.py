import os
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from loaders.config.config_loader import ConfigLoader
from loaders.base_extract import BaseExtract
import pyfredapi as pf

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class PyFredAPIExtract(BaseExtract):
    def __init__(self, start_date: str, end_date: str):
        """
        PyFredAPI extractor for macroeconomic indicators.

        Args:
            start_date (str): Observation start date.
            end_date (str): Observation end date.
        """
        super().__init__(start_date, end_date)
        self.api_key = os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise ValueError("FRED_API_KEY is not set in the environment variables")

    def get_macro_indicators(self):
        """
        Loads macro indicators from the configuration file.
        """
        logging.info("Loading macro indicators from configuration")
        config_loader = ConfigLoader()

        # Load configurations
        macro_indicators = config_loader.get("MACRO_INDICATORS").split()

        return macro_indicators

    def extract_indicator(self, series_id: str) -> dict:
        """
        Extract data for the specified macroeconomic indicator.

        Args:
            series_id (str): Series ID.

        Returns:
            dict: Data frame for the specified series ID.
        """
        logger.info(f"Extracting data for series ID: {series_id}")
        logger.info(f"Observation start date: {self.dt.strftime('%Y-%m-%d')}")
        logger.info(f"Observation end date: {self.dt_end.strftime('%Y-%m-%d')}")
        extra_parameters = {
            "observation_start": self.dt.strftime("%Y-%m-%d"),
            "observation_end": self.dt_end.strftime("%Y-%m-%d"),
        }
        try:
            series_data = pf.get_series(series_id=series_id, api_key=self.api_key, **extra_parameters)
            if series_data.empty:
                logger.warning(f"No data returned for series ID: {series_id}")
            else:
                logger.info(f"Successfully extracted data for series ID: {series_id}")
            return series_data
        except Exception as e:
            logger.error(f"Error extracting data for series ID {series_id}: {e}")
            return None

    def extract(self):
        """
        Extract data for all macroeconomic indicators.

        Returns:
            dict: Data frames for all macroeconomic indicators.
        """
        logger.info("Starting data extract process")
        macro_indicators = self.get_macro_indicators()

        # Extract data for each macro indicator
        data_frames = {}
        for indicator in macro_indicators:
            data_frames[indicator] = self.extract_indicator(indicator)

        logger.info("Data extract process completed")
        return data_frames

if __name__ == "__main__":
    # Configure start_date to be today's date - 7 days and end_date to be today's date + 7 days
    start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y%m%d")
    end_date = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y%m%d")

    extractor = PyFredAPIExtract(start_date=start_date, end_date=end_date)

    data = extractor.extract()

    logger.info("Macroeconomic indicators data:")
    for indicator, df in data.items():
        logger.info(f"Indicator: {indicator}")
        logger.info(f"\n{df.head()}")