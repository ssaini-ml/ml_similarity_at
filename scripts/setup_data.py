import os
import shutil
from pathlib import Path

def setup_data_directory():
    # Create necessary directories
    data_dir = Path("data")
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    
    # Create directories if they don't exist
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Create a README in the data directory
    readme_content = """# Data Directory

This directory contains the data for the ML Recommendation System.

## Directory Structure
- `raw/`: Contains the original CSV files
- `processed/`: Contains processed data and embeddings

## Required Data
Place your CSV file in the `raw` directory with the following structure:
- Required columns: "Name Shop", "ABDA Name (Hersteller)"
- File should be named: `Feature_Engineering_product_details___AT.csv`

## Data Privacy
- Do not commit sensitive data to version control
- Add data files to .gitignore
- Keep data files local to your environment
"""
    
    with open(data_dir / "README.md", "w") as f:
        f.write(readme_content)
    
    print("Data directory structure created successfully!")
    print(f"Please place your CSV file in: {raw_dir}")
    print("Remember to add your data files to .gitignore")

if __name__ == "__main__":
    setup_data_directory() 