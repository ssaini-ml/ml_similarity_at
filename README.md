# Streamlit ML Similarity Application

A Streamlit-based machine learning application that provides text similarity and recommendation services, with a FastAPI backend for API access.

## Recent Changes


- Added Docker support for containerized deployment
- Enhanced error handling and logging in the application
- Integrated FastAPI and Streamlit applications to run simultaneously
- Added proper process management for graceful shutdown
- Added Dockerfile.local for development environment

## Prerequisites

- Python 3.9 or higher
- Docker (for containerized deployment)
- pip (Python package manager)

## Installation

### Local Development

1. Clone the repository:
```bash
git clone <repository-url>
cd ml_similarity_at
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
# On Windows: venv\Scripts\activate
source venv/bin/activate  
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Application

### Local Development

You can run the application locally using the `run_app_local.py` script, which will:
- Automatically detect your operating system
- Run the appropriate pipeline (Pandas for Windows/macOS, Spark for Linux)
- Start both FastAPI and Streamlit servers
- Set up proper logging

```bash
python run_app_local.py
```

The application will be available at:
- Streamlit UI: http://localhost:8501
- FastAPI Backend: http://localhost:8000

### Docker Deployment

There are two Docker configurations available:

#### 1. Production Deployment (Dockerfile.streamlit)
This configuration is optimized for production use:

```bash
# Building docker image for steamlit image
docker build -f Dockerfile.streamlit -t streamlit-ml-similarity .

# Runing the streamlit container
docker run -p 8501:8501 -p 8000:8000 streamlit-ml-similarity
```

#### 2. Local Development (Dockerfile.local)
This configuration mirrors the local development setup with OS detection:

```bash
# Building the development local image
docker build -f Dockerfile.local -t ml-similarity-local .

# Runing the local container
docker run -p 8501:8501 -p 8000:8000 ml-similarity-local
```

Both Docker configurations will make the application available at:
- Streamlit UI: http://localhost:8501
- FastAPI Backend: http://localhost:8000

To stop any running container:
```bash
# List running containers
docker ps

# Stop the container
docker stop <container_id>
```

## Features

- Interactive Streamlit interface for text similarity analysis
- FastAPI backend for programmatic access
- CSV file processing and visualization
- Real-time recommendations
- Comprehensive logging
- OS-specific pipeline selection (Pandas/Spark)
- Docker support for both development and production

## Project Structure

```
ml_similarity_at/
├── app/                      # Streamlit application
│   ├── data_loader/         # Data loading utilities
│   ├── model/              # Model components
│   └── streamlit_app.py    # Main Streamlit interface
├── api/                     # FastAPI backend
│   └── main.py            # API endpoints and logic
├── src/                     # Core source code
│   ├── api/               # API implementation
│   ├── app/              # Application logic
│   ├── config.py         # Configuration settings
│   ├── data_loader/      # Data loading modules
│   ├── features/         # Feature engineering
│   ├── model/           # Model implementations
│   ├── pipeline/        # Pipeline components
│   └── run_streamlit_app.py
├── scripts/                # Utility scripts
│   ├── run_data_loader.py
│   ├── run_ml_pipeline.py
│   ├── run_pipeline.py
│   ├── run_pipeline_pandas.py
│   └── setup_data.py
├── data/                   # Data directory
│   └── raw/              # Raw data files
├── logs/                   # Log files
├── notebooks/             # Jupyter notebooks
├── tests/                 # Test files
├── ml_pipeline_similarity/ # Package directory
├── requirements.txt        # Python dependencies
├── setup.py               # Package setup
├── Dockerfile.local       # Development Dockerfile
├── Dockerfile.streamlit   # Production Dockerfile
└── run_app_local.py       # Local development runner
```

## Features

- Interactive Streamlit interface for text similarity analysis
- FastAPI backend for programmatic access
- CSV file processing and visualization
- Real-time recommendations
- Comprehensive logging
- OS-specific pipeline selection (Pandas/Spark)
- Docker support for both development and production
- JWT-based authentication
- Model caching for improved performance
- Data processing and embedding generation
- Test suite for core functionality

## Setup

1. Clone the repository:
```bash
git clone https://github.com/ssaini-ml/ml_similarity_at.git
cd ml_similarity_at
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

## Running the Application

### Local Development

Run the application with automatic OS detection:
```bash
python run_app_local.py
```

This will:
- Detect your operating system
- Choose the appropriate pipeline (Pandas for Windows/macOS, Spark for Linux)
- Start the FastAPI backend
- Launch the Streamlit interface

### Docker Development

Build and run the development container:
```bash
docker build -f Dockerfile.local -t ml-similarity-local .
docker run -p 8501:8501 -p 8000:8000 ml-similarity-local
```

### Docker Production

Build and run the production container:
```bash
docker build -f Dockerfile.streamlit -t ml-similarity-prod .
docker run -p 8501:8501 -p 8000:8000 ml-similarity-prod
```

## Access Points

- Streamlit UI: http://localhost:8501
- FastAPI Backend: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## API Endpoints

- `POST /process`: Compute similarity for single text entries
- `POST /process_csv`: Process uploaded CSV file for similarity
- `POST /process_source_csv`: Process the source CSV file for similarity
- `GET /health`: Check API health

## Data Processing Flow

1. **Data Loading**: `scripts/run_data_loader.py`
   - Loads raw text data
   - Performs text cleaning
   - Saves processed data

2. **Similarity Pipeline**: Choose one of:
   - Pandas pipeline for quick similarity computation
   - Spark pipeline for large-scale similarity analysis
   - Auto-select for convenience







