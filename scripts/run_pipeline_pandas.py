"""
Pandas Pipeline Configuration

This script was created as a specialized version of the pipeline that focuses
on pandas-specific data processing operations. It is separate from the main
pipeline to provide:

Note: This pipeline was specifically created to address memory-related issues
      encountered when running the main Spark pipeline on Windows. While Spark
      is powerful for distributed computing, it can be resource-intensive on
      Windows systems. This pandas implementation provides a more lightweight
      alternative for Windows environments while maintaining similar functionality
      for in-memory data processing.
"""

import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import os
import logging
from pathlib import Path

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    if pd.isna(text):
        return ""
    return str(text).lower().strip()

def run_pipeline(processed_dir):
    """
    Run the pandas pipeline and return the processed DataFrame.
    
    Args:
        processed_dir (Path): Directory where processed data should be saved
        
    Returns:
        pd.DataFrame: Processed DataFrame with embeddings
    """
    try:
        logger.info("Loading the model...")
        model = SentenceTransformer('distiluse-base-multilingual-cased-v2')

        # Loading data from the raw directory
        data_path = os.path.join("data", "raw", "Feature_Engineering_product_details___AT.csv")
        logger.info(f"Loading data from {data_path}")
        df = pd.read_csv(data_path)
        logger.info(f"Loaded {len(df)} rows of data")

        # Clean text columns
        logger.info("Cleaning text columns...")
        df['name_shop_clean'] = df['Name Shop'].apply(clean_text)
        df['description_clean'] = df['ABDA Name (Hersteller)'].apply(clean_text)

        # Generating embeddings
        logger.info("Generating embeddings...")
        df['name_shop_embedding'] = df['name_shop_clean'].apply(lambda x: model.encode(x).tolist())
        df['description_embedding'] = df['description_clean'].apply(lambda x: model.encode(x).tolist())

        # Save processed data
        output_path = processed_dir / 'processed_data.csv'
        df.to_csv(output_path, index=False)
        logger.info(f"Processed data saved to {output_path}")

        return df

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def main():
    try:
        # Set up directories
        data_dir = Path("data")
        processed_dir = data_dir / "processed"
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Run pipeline
        df = run_pipeline(processed_dir)
        
        # Showing results
        print("\nSample of the processed data:")
        print("\nOriginal columns:")
        print(df[['Name Shop', 'ABDA Name (Hersteller)']].head())
        print("\nCleaned columns:")
        print(df[['name_shop_clean', 'description_clean']].head())
        print("\nEmbedding shapes:")
        print("Name shop embedding shape:", len(df['name_shop_embedding'].iloc[0]))
        print("Description embedding shape:", len(df['description_embedding'].iloc[0]))

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main() 