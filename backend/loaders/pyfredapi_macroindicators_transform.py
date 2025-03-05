import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PyFredAPITransform:
    def __init__(self):
        """
        PyFredAPI transformer for macroeconomic indicators.
        """
        logger.info("PyFredAPITransform initialized")

    def transform(self, macro_indicator_id: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the given DataFrame by performing the following operations:
            - Remove columns that are not 'date' or 'value'.
            - Convert 'date' column to ISODate format.
            - Convert 'value' column to Double format.
            - Add specific literals for each macro indicator.

        Parameters:
            - macro_indicator_id (str): The macro indicator ID (series ID).
            - df (pd.DataFrame): The input DataFrame with 'date' and 'value' columns.

        Returns:
            - pd.DataFrame: Transformed DataFrame.
        """
        logger.info(f"Transforming data for macro indicator: {macro_indicator_id}")

        if df.empty:
            logger.warning(f"No data available to transform for macro indicator: {macro_indicator_id}")
            return df

        try:
            # Remove unnecessary columns
            df = df.loc[:, ['date', 'value']]

            # Convert 'date' column to ISODate format
            df.loc[:, 'date'] = pd.to_datetime(df['date'])

            # Convert 'value' column to Double format
            df.loc[:, 'value'] = df['value'].astype(float)

            # Add specific literals for each macro indicator
            if macro_indicator_id == "GDP":
                df.loc[:, 'title'] = "Gross Domestic Product"
                df.loc[:, 'series_id'] = "GDP"
                df.loc[:, 'frequency'] = "Quarterly"
                df.loc[:, 'frequency_short'] = "Q"
                df.loc[:, 'units'] = "Billions of Dollars"
                df.loc[:, 'units_short'] = "Bil. of $"
            elif macro_indicator_id == "REAINTRATREARAT10Y":
                df.loc[:, 'title'] = "10-Year Real Interest Rate"
                df.loc[:, 'series_id'] = "REAINTRATREARAT10Y"
                df.loc[:, 'frequency'] = "Monthly"
                df.loc[:, 'frequency_short'] = "M"
                df.loc[:, 'units'] = "Percent"
                df.loc[:, 'units_short'] = "%"
            elif macro_indicator_id == "UNRATE":
                df.loc[:, 'title'] = "Unemployment Rate"
                df.loc[:, 'series_id'] = "UNRATE"
                df.loc[:, 'frequency'] = "Monthly"
                df.loc[:, 'frequency_short'] = "M"
                df.loc[:, 'units'] = "Percent"
                df.loc[:, 'units_short'] = "%"
            else:
                logger.warning(f"Unknown macro indicator ID: {macro_indicator_id}")
                return df

            # Reorder columns
            df = df.loc[:, ['title', 'series_id', 'frequency', 'frequency_short', 'units', 'units_short', 'date', 'value']]

            logger.info(f"Successfully transformed data for macro indicator: {macro_indicator_id}")
            return df
        except Exception as e:
            logger.error(f"Error transforming data for macro indicator {macro_indicator_id}: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    sample_data = {
        'GDP': pd.DataFrame({
            'realtime_start': ['2025-03-05'],
            'realtime_end': ['2025-03-05'],
            'date': ['2024-10-01'],
            'value': [29719.647]
        }),
        'REAINTRATREARAT10Y': pd.DataFrame({
            'realtime_start': ['2025-03-05'],
            'realtime_end': ['2025-03-05'],
            'date': ['2025-02-01'],
            'value': [2.031282]
        }),
        'UNRATE': pd.DataFrame({
            'realtime_start': ['2025-03-05'],
            'realtime_end': ['2025-03-05'],
            'date': ['2025-01-01'],
            'value': [4]
        })
    }

    transformer = PyFredAPITransform()
    for macro_indicator_id, df in sample_data.items():
        transformed_df = transformer.transform(macro_indicator_id, df)
        logger.info(f"Transformed data for macro indicator: {macro_indicator_id}")
        logger.info(f"\n{transformed_df.head()}")