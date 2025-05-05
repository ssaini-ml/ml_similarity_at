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
cd ml_similarity_at-main
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
ml_similarity_at-main/
├── app/
│   └── streamlit_app.py      # Main Streamlit application
├── api/
│   └── main.py              # FastAPI backend
├── data/
│   └── raw/
│       └── products.csv
├── scripts/
│   ├── run_pipeline.py      # Spark pipeline for Linux
│   └── run_pipeline_pandas.py # Pandas pipeline for Windows/macOS
├── logs/
├── requirements.txt         # Python dependencies
├── Dockerfile.streamlit    # Production Docker configuration
├── Dockerfile.local       # Development Docker configuration
├── run_app_local.py      # Local development runner with OS detection
└── README.md
```




