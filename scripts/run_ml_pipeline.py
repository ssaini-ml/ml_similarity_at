"""
ML Pipeline Runner
----------------
This script can handle both pandas and Spark processing based on the input.
It automatically detects which processing method to use based on the script name.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

def main():
    """
    Main function to run the machine learning pipeline.
    Automatically detects and uses the appropriate processing method.
    """
    try:
        # Determine processing method based on script name
        current_script = Path(sys.argv[0]).name
        if current_script == "run_pipeline_pandas.py":
            from scripts.run_pipeline_pandas import run_pipeline
            processing_method = "pandas"
        elif current_script == "run_pipeline.py":
            from scripts.run_pipeline import run_pipeline
            processing_method = "spark"
        else:
            # Default to pandas for other scripts
            from scripts.run_pipeline_pandas import run_pipeline
            processing_method = "pandas"

        logger.info(f"Starting ML pipeline using {processing_method} processing")
        
        # Set up directories
        data_dir = project_root / 'data'
        processed_dir = data_dir / 'processed'
        models_dir = project_root / 'models'
        
        processed_dir.mkdir(parents=True, exist_ok=True)
        models_dir.mkdir(parents=True, exist_ok=True)
        
        # Load and process data
        logger.info("Loading processed data...")
        df = run_pipeline(processed_dir)
        logger.info(f"Data loaded successfully. Shape: {df.shape}")
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = models_dir / f'ml_results_{timestamp}.csv'
        df.to_csv(output_file, index=False)
        
        logger.info(f"Results saved to {output_file}")
        
        # Create status file
        status_file = project_root / 'status.txt'
        with open(status_file, 'w') as f:
            f.write("completed\n")
            f.write(f"{timestamp}\n")
            
    except Exception as e:
        logger.error(f"Error in ML pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main() 