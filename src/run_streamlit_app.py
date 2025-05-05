import os
import sys
import subprocess
import multiprocessing
from loguru import logger
from dotenv import load_dotenv

# Loading  environment variables
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

if __name__ == "__main__":
    try:
        logger.info("Starting application...")
        
        # Createing processes for both applications
        fastapi_process = multiprocessing.Process(target=run_fastapi)
        streamlit_process = multiprocessing.Process(target=run_streamlit)
        
        # Start both processes
        fastapi_process.start()
        streamlit_process.start()
        
        # Waiting for both processes to complete
        fastapi_process.join()
        streamlit_process.join()
        
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