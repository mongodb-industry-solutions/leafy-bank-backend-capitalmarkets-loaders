import logging
from datetime import datetime, timedelta, timezone
from pyfredapi_macroindicators_extract import PyFredAPIExtract
from pyfredapi_macroindicators_transform import PyFredAPITransform
from pyfredapi_macroindicators_load import PyFredAPILoad

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    # Configure start_date to be today's date - 7 days and end_date to be today's date + 7 days
    start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y%m%d")
    end_date = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y%m%d")

    # Step 1: Extract data
    logger.info("Starting ETL process: Extract")
    extractor = PyFredAPIExtract(start_date=start_date, end_date=end_date)
    extracted_data = extractor.extract()

    # Step 2: Transform data
    logger.info("Starting ETL process: Transform")
    transformer = PyFredAPITransform()
    transformed_data = {}
    for series_id, df in extracted_data.items():
        transformed_data[series_id] = transformer.transform(series_id, df)

    # Step 3: Load data
    logger.info("Starting ETL process: Load")
    loader = PyFredAPILoad()
    loader.load(transformed_data)

    logger.info("ETL process completed successfully")

if __name__ == "__main__":
    main()