"""
Pipeline Configuration
--------------------
This module provides configuration settings for the ML pipeline.
It automatically detects which processing method to use based on the script being run.
"""

import os
import sys
from pathlib import Path

# Getting the name of the script being run
current_script = Path(sys.argv[0]).name

# Determine processing method based on script name
if current_script == "run_pipeline_pandas.py":
    PROCESSING_METHOD = "pandas"
    PIPELINE_MODULE = "scripts.run_pipeline_pandas"
elif current_script == "run_pipeline.py":
    PROCESSING_METHOD = "spark"
    PIPELINE_MODULE = "scripts.run_pipeline"
else:
    # Default to pandas for other scripts
    PROCESSING_METHOD = "pandas"
    PIPELINE_MODULE = "scripts.run_pipeline_pandas"

# Logging the selected method
print(f"Using {PROCESSING_METHOD} processing method")

# Common paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
MODELS_DIR = PROJECT_ROOT / "models"

# Ensure directories exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, MODELS_DIR]:
    directory.mkdir(parents=True, exist_ok=True) 