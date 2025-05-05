"""
Data Loader & Pipeline Runner

This script loads raw data, runs it through a processing pipeline, 
and saves the output — built to work both locally and in GitHub Actions.


- Loads raw data from the project directory
- Runs preprocessing and transformations
- Saves processed data with timestamped filenames
- Tracks job status for CI workflows

Note:
- Paths are resolved relative to the repo root
- Uses custom functions from ml_pipeline_similarity
"""


import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


project_root = str(Path(os.getcwd()).parent)
sys.path.append(project_root)

from ml_pipeline_similarity import load_data, run_pipeline

def main():
    """
     Main function that handles loading, processing, and saving data.
    Designed to be compatible with CI pipelines like GitHub Actions.
    """
    try:
      
        data_dir = project_root / 'data'
        raw_dir = data_dir / 'raw'
        processed_dir = data_dir / 'processed'
        
       
        raw_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)
        
       
        logger.info("Loading data...")
        df = load_data(raw_dir)
        logger.info(f"Data loaded successfully. Shape: {df.shape}")
        
       
        logger.info("Starting pipeline execution...")
        results = run_pipeline(df)
        
        logger.info("Pipeline execution completed successfully")
        logger.info("Results summary:")
        for key, value in results.items():
            if hasattr(value, 'shape'):
                logger.info(f"{key} shape: {value.shape}")
            else:
                logger.info(f"{key}: {value}")
                
      
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = processed_dir / f'processed_data_{timestamp}.csv'
        results.to_csv(output_file, index=False)
        
        logger.info(f"Results saved to {output_file}")
        
      
        status_file = project_root / 'status.txt'
        with open(status_file, 'w') as f:
            f.write("completed\n")
            f.write(f"{timestamp}\n")
                
    except Exception as e:
        logger.error(f"Error in pipeline execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 