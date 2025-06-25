import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BinanceAPITransform:
    def __init__(self):
        """
        Binance API data transformer.
        """
        logger.info("BinanceAPITransform initialized")

    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalizes the given Binance symbol by removing the USDT suffix if present.

        Parameters:
            - symbol (str): The Binance symbol to be normalized.
        
        Returns:
            - str: Normalized symbol without USDT suffix.
        """
        # Remove 'USDT' suffix if present
        if symbol.endswith("USDT"):
            return symbol[:-4]  # Remove last 4 characters (USDT)
        return symbol

    def transform(self, symbol: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the Binance DataFrame by performing the following operations:
            - Reset the index (timestamp) 
            - Add a new column 'symbol' with the normalized symbol value
            - Convert all column names to lowercase
            - Reorder columns as 'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'
        
        Parameters:
            - symbol (str): The symbol name to be added in the 'symbol' column.
            - df (pd.DataFrame): The input DataFrame with 'timestamp' as the index.
        
        Returns:
            - pd.DataFrame: Transformed DataFrame.
        """
        logger.info(f"Transforming data for Binance symbol: {symbol}")
        try:
            # Print the DataFrame structure for debugging
            logger.debug(f"DataFrame columns before reset_index: {df.columns}")
            logger.debug(f"DataFrame index name: {df.index.name}")
            
            # Normalize the symbol (remove USDT suffix)
            normalized_symbol = self.normalize_symbol(symbol)
            
            # Reset index to make timestamp a column and explicitly name it 'timestamp'
            df = df.reset_index(names=['timestamp'])
            
            logger.debug(f"DataFrame columns after reset_index: {df.columns}")
            
            # Add the normalized symbol column
            df['symbol'] = normalized_symbol
            
            # Convert all column names to lowercase
            df.columns = df.columns.str.lower()
            
            # Reorder columns
            df = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            
            logger.info(f"Successfully transformed data for symbol: {symbol}")
            return df
        except Exception as e:
            logger.error(f"Error transforming data for symbol {symbol}: {e}")
            raise
    
    def transform_crypto_data(self, data: dict) -> pd.DataFrame:
        """
        Transform all cryptocurrency data in the provided nested dictionary.
        
        Parameters:
            - data (dict): Dictionary containing crypto data where keys are symbols and values are DataFrames
                          (This should be the 'crypto' key from the extract output)
        
        Returns:
            - pd.DataFrame: Combined DataFrame with all transformed data
        """
        logger.info("Transforming all cryptocurrency data")
        transformed_frames = []
        
        for symbol, df in data.items():
            try:
                transformed_df = self.transform(symbol, df)
                transformed_frames.append(transformed_df)
            except Exception as e:
                logger.error(f"Error transforming {symbol}: {e}")
                # Continue with other symbols if one fails
        
        if transformed_frames:
            # Combine all transformed dataframes
            combined_df = pd.concat(transformed_frames, ignore_index=True)
            logger.info(f"Successfully transformed data for {len(transformed_frames)} symbols")
            return combined_df
        else:
            logger.warning("No data was successfully transformed")
            return pd.DataFrame()

if __name__ == "__main__":
    # Example usage with actual Binance extract data structure
    from binance_api_extract import BinanceAPIExtract
    
    # Create an extractor and get real data
    extractor = BinanceAPIExtract(start_date="20250624", end_date="20250625")
    extracted_data = extractor.extract()
    
    # Print the structure of the extracted data
    print("Extracted data structure:")
    print(f"Keys in main dictionary: {list(extracted_data.keys())}")
    if 'crypto' in extracted_data:
        print(f"Keys in 'crypto' dictionary: {list(extracted_data['crypto'].keys())}")
        
        # Take the first symbol as an example
        first_symbol = list(extracted_data['crypto'].keys())[0]
        print(f"\nSample data for {first_symbol}:")
        print(f"Type: {type(extracted_data['crypto'][first_symbol])}")
        print(f"Shape: {extracted_data['crypto'][first_symbol].shape}")
        print(f"Columns: {extracted_data['crypto'][first_symbol].columns.tolist()}")
        print(f"Index name: {extracted_data['crypto'][first_symbol].index.name}")
        print(extracted_data['crypto'][first_symbol].head(3))
    
    # Transform the data
    transformer = BinanceAPITransform()
    
    # Use the new method to transform all crypto data at once
    transformed_data = transformer.transform_crypto_data(extracted_data['crypto'])
    
    print("\nTransformed data:")
    print(f"Shape: {transformed_data.shape}")
    print(f"Columns: {transformed_data.columns.tolist()}")
    print(transformed_data.head())
    print(transformed_data.tail())