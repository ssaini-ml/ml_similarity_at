import os
import sys
import platform
import subprocess
from loguru import logger
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()

# Configureing logging
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    "logs/app.log",
    rotation="500 MB",
    retention="10 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG"
)

def detect_os():
    """Detect the operating system and return appropriate pipeline script."""
    system = platform.system().lower()
    logger.info(f"Detected operating system: {system}")
    # macOS
    if system == "darwin":  
        return "scripts/run_pipeline_pandas.py"
    elif system == "linux":
    # for Spark Linux
        return "scripts/run_pipeline.py"  
    elif system == "windows":
        return "scripts/run_pipeline_pandas.py"  
    # for Pandas Windows
    else:
        logger.warning(f"Unsupported operating system: {system}. Defaulting to pandas pipeline.")
        return "scripts/run_pipeline_pandas.py"

def run_fastapi():
    """Run the FastAPI application using Uvicorn."""
    try:
        logger.info("Starting FastAPI server...")
        subprocess.run([
            "uvicorn",
            "api.main:app",
            "--host", "0.0.0.0",
            "--port", "8000",
            "--reload",
            "--workers", "1"
        ])
    except Exception as e:
        logger.error(f"Error starting FastAPI server: {str(e)}")
        raise

def run_streamlit():
    """Run the Streamlit application."""
    try:
        logger.info("Starting Streamlit server...")
        subprocess.run([
            "streamlit",
            "run",
            "app/streamlit_app.py",
            "--server.port", "8501",
            "--server.address", "0.0.0.0",
            "--server.headless", "true"
        ])
    except Exception as e:
        logger.error(f"Error starting Streamlit server: {str(e)}")
        raise

def run_pipeline():
    """Run the appropriate pipeline based on OS."""
    try:
        pipeline_script = detect_os()
        logger.info(f"Running pipeline script: {pipeline_script}")
        
        # ruuning run data loader
        logger.info("Running data loader...")
        subprocess.run([sys.executable, "scripts/run_data_loader.py"])
        
        # runing the appropriate pipeline
        logger.info("Running pipeline...")
        subprocess.run([sys.executable, pipeline_script])
        
    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting application...")
        
        # Createing processes for all applications
        fastapi_process = subprocess.Popen([
            sys.executable, "-c",
            "import uvicorn; uvicorn.run('api.main:app', host='0.0.0.0', port=8000, reload=True, workers=1)"
        ])
        
        streamlit_process = subprocess.Popen([
            "streamlit", "run", "app/streamlit_app.py",
            "--server.port", "8501",
            "--server.address", "0.0.0.0"
        ])
        
        # Runing the pipeline
        run_pipeline()
        
        # Waiting for both processes to complete
        fastapi_process.wait()
        streamlit_process.wait()
        
    except KeyboardInterrupt:
        logger.info("Shutting down application...")
        if 'fastapi_process' in locals():
            fastapi_process.terminate()
        if 'streamlit_process' in locals():
            streamlit_process.terminate()
        logger.info("Application shutdown complete.")
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        if 'fastapi_process' in locals():
            fastapi_process.terminate()
        if 'streamlit_process' in locals():
            streamlit_process.terminate()
        raise 