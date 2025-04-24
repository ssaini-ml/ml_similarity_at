# ML Similarity Project

This project provides multiple ways to run the ML similarity pipeline, allowing you to find similar items based on text embeddings. Choose the method that best suits your environment and needs.

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your data:
```bash
python scripts/setup_data.py
```

## Running the Similarity Pipeline

### Option 1: Pandas Pipeline (Recommended for Windows/Local Development)
Use this option if you're:
- Running on Windows
- Have limited memory resources
- Need quick similarity results for smaller datasets

```bash
python scripts/run_pipeline_pandas.py
```

### Option 2: Spark Pipeline (Recommended for Linux/Production)
Use this option if you're:
- Running on Linux/Unix
- Have large datasets
- Need distributed similarity computation
- Have sufficient memory resources

```bash
python scripts/run_pipeline.py
```

### Option 3: Auto-Select Pipeline
This will automatically choose between pandas and Spark based on your environment:
```bash
python scripts/run_ml_pipeline.py
```

## Running the Streamlit App

To visualize and interact with similarity results:
```bash
python -m streamlit run app/streamlit_app.py
```

The app will be available at: http://localhost:8502

## Data Processing Flow

1. **Data Loading**: `scripts/run_data_loader.py`
   - Loads raw text data
   - Performs text cleaning
   - Saves processed data

2. **Similarity Pipeline**: Choose one of:
   - Pandas pipeline for quick similarity computation
   - Spark pipeline for large-scale similarity analysis
   - Auto-select for convenience

3. **Visualization**: Streamlit app
   - View similarity results
   - Find similar items
   - Analyze similarity scores

##  When to Use Which Option

### Use Pandas Pipeline When:
- You're on Windows
- Your dataset fits in memory
- You need quick similarity results
- You're doing local development

### Use Spark Pipeline When:
- You're on Linux/Unix
- Your dataset is large
- You need distributed similarity computation
- You're in a production environment

### Use Auto-Select When:
- You want the system to choose the best option
- You're not sure which to use
- You're testing in different environments

## 🛠️ Troubleshooting



1. **Memory Errors with Spark on Windows**
   - Solution: Use the pandas pipeline instead
   - Command: `python scripts/run_pipeline_pandas.py`

2. **Streamlit Not Starting**
   - Solution: Make sure port 8502 is available
   - Try: `python -m streamlit run app/streamlit_app.py --server.port 8503`

3. **Data Loading Issues**
   - Solution: Run setup_data.py first
   - Command: `python scripts/setup_data.py`

## Notes

- The pandas pipeline is generally faster for smaller similarity computations
- The Spark pipeline is better for production and large-scale similarity analysis
- The Streamlit app works with both pipeline outputs
- All similarity results are saved in the `data/processed` directory



## Project Structure

```
ml_similarity/
├── app/
│   └── streamlit_app.py      # Streamlit interface for similarity visualization
├── api/
│   └── main.py              # FastAPI backend for similarity computation
├── data/
│   ├── raw/                 # Raw text data directory
│   └── processed/           # Processed similarity results
├── scripts/
│   ├── run_pipeline_pandas.py  # Pandas-based similarity pipeline
│   ├── run_pipeline.py         # Spark-based similarity pipeline
│   ├── run_ml_pipeline.py      # Auto-select similarity pipeline
│   └── setup_data.py          # Data setup script
├── tests/                   # Test files
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/ml_similarity.git
cd ml_similarity
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up data directory:
```bash
python scripts/setup_data.py
```

## Data Setup

1. Place your CSV file in the `data/raw` directory:
   - Required columns: "Name Shop", "ABDA Name (Hersteller)"
   - File should be named: `Feature_Engineering_product_details___AT.csv`
   - Note: This file is not included in git (see .gitignore)

2. Alternative: Use the file upload feature in the Streamlit app to process any CSV file with the same structure.

## Running the Application

### Streamlit Interface
```bash
python -m streamlit run app/streamlit_app.py
```
Access the web interface at http://localhost:8501

### FastAPI Backend
```bash
python api/main.py
```
API will be available at http://localhost:8000

## API Endpoints

- `POST /process`: Compute similarity for single text entries
- `POST /process_csv`: Process uploaded CSV file for similarity
- `POST /process_source_csv`: Process the source CSV file for similarity
- `GET /health`: Check API health

