import streamlit as st
from loguru import logger
import sys
import os

# Configure logging
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    "logs/streamlit.log",
    rotation="500 MB",
    retention="10 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG"
)

def main():
    try:
        logger.info("Starting Streamlit application...")
        
        # Set page config
        st.set_page_config(
            page_title="ML Similarity App",
            page_icon="🤖",
            layout="wide"
        )
        
        # Add title
        st.title("ML Similarity App")
        
        # Add sidebar
        with st.sidebar:
            st.header("Navigation")
            page = st.radio(
                "Go to",
                ["Home", "Similarity Search", "Model Training", "Settings"]
            )
        
        # Main content area
        if page == "Home":
            st.header("Welcome to ML Similarity App")
            st.write("This application helps you find similar items using machine learning.")
            
        elif page == "Similarity Search":
            st.header("Similarity Search")
            st.write("Search for similar items here.")
            
        elif page == "Model Training":
            st.header("Model Training")
            st.write("Train and evaluate models here.")
            
        elif page == "Settings":
            st.header("Settings")
            st.write("Configure application settings here.")
        
        logger.info("Streamlit application running successfully")
        
    except Exception as e:
        logger.error(f"Error in Streamlit application: {str(e)}")
        st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 