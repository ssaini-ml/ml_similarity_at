from .data_loader import load_data, load_and_query_data
import pandas as pd
import os
from pathlib import Path
from typing import Optional, Union, Dict, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    """
    A flexible data loader that can be used in both scripts and notebooks.
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
    
    def load_data(self, 
                 filename: str = "Feature_Engineering_product_details___AT.csv",
                 source: str = "raw") -> pd.DataFrame:
        """
        Load data from CSV file.
        
        Args:
            filename: Name of the CSV file to load
            source: Either 'raw' or 'processed' to specify which directory to load from
            
        Returns:
            pandas.DataFrame: Loaded data
        """
        if source == "raw":
            file_path = self.raw_dir / filename
        elif source == "processed":
            file_path = self.processed_dir / filename
        else:
            raise ValueError("source must be either 'raw' or 'processed'")
        
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found at {file_path}")
        
        logger.info(f"Loading data from {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows of data")
        return df
    
    def save_data(self, 
                 df: pd.DataFrame, 
                 filename: str,
                 destination: str = "processed") -> None:
        """
        Save data to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Name of the output file
            destination: Either 'raw' or 'processed' to specify which directory to save to
        """
        if destination == "raw":
            file_path = self.raw_dir / filename
        elif destination == "processed":
            file_path = self.processed_dir / filename
        else:
            raise ValueError("destination must be either 'raw' or 'processed'")
        
        df.to_csv(file_path, index=False)
        logger.info(f"Saved data to {file_path}")
    
    def get_directory_info(self) -> Dict[str, Any]:
        """
        Get information about data directories.
        
        Returns:
            dict: Dictionary containing paths and file information
        """
        info = {
            "raw_dir": str(self.raw_dir),
            "processed_dir": str(self.processed_dir),
            "raw_files": [f.name for f in self.raw_dir.glob("*.csv")],
            "processed_files": [f.name for f in self.processed_dir.glob("*.csv")]
        }
        return info


"""
# Import the loader
from ml_pipeline_similarity.data_loader import DataLoader

# Initialize the loader
loader = DataLoader()

# Load raw data
df = loader.load_data()  
# Or specify a different file
# df = loader.load_data("my_data.csv")

# Process the data (using your existing processing functions)
processed_df = process_dataframe(df)  # Your existing processing function

# Save processed data
loader.save_data(processed_df, "processed_data.csv")

# Get directory information
info = loader.get_directory_info()
print(f"Raw directory: {info['raw_dir']}")
print(f"Processed directory: {info['processed_dir']}")
print(f"Available raw files: {info['raw_files']}")
print(f"Available processed files: {info['processed_files']}")
"""

__all__ = ['load_data', 'load_and_query_data'] 