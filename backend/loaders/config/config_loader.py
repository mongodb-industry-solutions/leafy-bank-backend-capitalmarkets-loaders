import os
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ConfigLoader:
    """
    A class to load configuration from a JSON file.
    """
    def __init__(self, config_file: str = "config.json"):
        """
        Initialize the ConfigLoader with a relative config file path.
        The config file path will be resolved relative to the script's directory.
        """
        
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Construct the absolute path to the config.json file
        self.config_file = os.path.join(script_dir, config_file)
        
        # Check if the config file exists at the resolved path
        if not os.path.exists(self.config_file):
            logging.error(f"Config file does not exist at: {self.config_file}")
            raise FileNotFoundError(f"Config file not found: {self.config_file}")
        
        # Load the configuration data
        self.config_data = self._load_config()

    def _load_config(self):
        """
        Load and parse the JSON configuration file.
        """
        try:
            with open(self.config_file, "r") as file:
                config_data = json.load(file)
                logging.info(f"Successfully loaded configuration file: {self.config_file}")
                return config_data
        except FileNotFoundError:
            logging.error(f"Configuration file {self.config_file} not found.")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing {self.config_file}: {e}")
            raise

    def get(self, key, default=None):
        """
        Get a configuration value by key.
        """
        value = self.config_data.get(key, default)
        return value

# ==================
# Example usage
# ==================

if __name__ == "__main__":
    config_loader = ConfigLoader()

    # Load configurations
    EXAMPLE_CSV_PATH = config_loader.get("EXAMPLE_CSV_PATH")
    EXAMPLE_CSV_FILES = config_loader.get("EXAMPLE_CSV_FILES")
    EXAMPLE_INDUSTRY = config_loader.get("EXAMPLE_INDUSTRY")
    EXAMPLE_MODEL_ID = config_loader.get("EXAMPLE_MODEL_ID")

    print(EXAMPLE_CSV_PATH)
    print(EXAMPLE_CSV_FILES)
    print(EXAMPLE_INDUSTRY)
    print(EXAMPLE_MODEL_ID)
