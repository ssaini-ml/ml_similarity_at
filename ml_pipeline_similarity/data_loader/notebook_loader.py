import pandas as pd
import os
from pathlib import Path
from typing import Optional, Union
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NotebookDataLoader:
    """
    A class to load and process data in Jupyter notebooks.
    """
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize the data loader.
        
        Args:
            data_dir: Optional path to data directory. If None, uses default 'data' directory.
        """
        self.data_dir = Path(data_dir) if data_dir else Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def load_raw_data(self, filename: str = "Feature_Engineering_product_details___AT.csv") -> pd.DataFrame:
        """
        Load raw data from CSV file.
        
        Args:
            filename: Name of the CSV file to load
            
        Returns:
            pandas.DataFrame: Loaded data
        """
        file_path = self.raw_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found at {file_path}")
        
        logger.info(f"Loading data from {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows of data")
        return df
    
    def save_processed_data(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save processed data to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Name of the output file
        """
        file_path = self.processed_dir / filename
        df.to_csv(file_path, index=False)
        logger.info(f"Saved processed data to {file_path}")
    
    def get_data_paths(self) -> dict:
        """
        Get paths to data directories.
        
        Returns:
            dict: Dictionary containing paths to raw and processed data directories
        """
        return {
            "raw_dir": str(self.raw_dir),
            "processed_dir": str(self.processed_dir)
        }

# Example usage in Jupyter notebook:
"""
# Import the loader
from ml_pipeline_similarity.data_loader.notebook_loader import NotebookDataLoader

# Initialize the loader
loader = NotebookDataLoader()

# Load raw data
df = loader.load_raw_data()

# Process the data (using your existing processing functions)
processed_df = process_dataframe(df)  # Your existing processing function

# Save processed data
loader.save_processed_data(processed_df, "processed_data.csv")

# Get data paths
paths = loader.get_data_paths()
print(f"Raw data directory: {paths['raw_dir']}")
print(f"Processed data directory: {paths['processed_dir']}")
""" 