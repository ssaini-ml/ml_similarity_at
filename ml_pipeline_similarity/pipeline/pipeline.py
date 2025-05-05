from pyspark.sql import SparkSession
from ml_pipeline_similarity.data_loader.data_loader import load_data
from ml_pipeline_similarity.features.processing import process_dataframe
from ml_pipeline_similarity.features.embeddings import compute_embeddings
import logging


logging.basicConfig(level=logging.INFO)

def run_pipeline(spark, source, columns_transformations, embedding_columns, is_table=False, query=None):
    """
    Run pipeline following steps:
    1. Load data
    2. Process data (transformations)
    3. Generate embeddings

    Example Usage:
    
    # Initializing or get the existing Spark session
    spark = SparkSession.builder.appName("MLPipeline").getOrCreate()

    # Define the transformations for the columns
    columns_transformations = {
        "column_1": [("transformation_1", "column_1_transformed")],
        "column_2": [("transformation_2", "column_2_transformed")],
    }

    # List of columns to generate embeddings for
    embedding_columns = ["description", "title"]

    # Run the pipeline
    result_df = run_pipeline(
        spark=spark,
        source="path_to_data_or_table",
        columns_transformations=columns_transformations,
        embedding_columns=embedding_columns,
        is_table=False
    )

    Parameters:
   
    spark : SparkSession
        The Spark session (can be passed from an existing session)
    source : str
        Path to CSV file or SQL table name
    columns_transformations : dict
        Dictionary containing column transformations to apply.
        Example: {"column_name": [("transformation_function", "new_column_name")]}
    embedding_columns : list
        List of column names to compute embeddings for
    is_table : bool, optional
        If true, source will be treated as a SQL table, by default False
    query : str, optional
        Custom SQL query to fetch data, by default None

    Returns:

    DataFrame
        Transformed DataFrame with embeddings
    """
    try:
      
        if not isinstance(columns_transformations, dict):
            raise ValueError("columns_transformations must be a dictionary")
        if not isinstance(embedding_columns, list):
            raise ValueError("embedding_columns must be a list")

        logging.info("Starting pipeline execution")

  
        df = load_data(spark, source, is_table, query)
        logging.info(f"Loaded {df.count()} rows from {source}")

        df_transformed = process_dataframe(df, columns_transformations)
        logging.info(f"Applied transformations, resulting in {df_transformed.count()} rows")

        df_with_embeddings = compute_embeddings(df_transformed, embedding_columns)
        logging.info(f"Generated embeddings, resulting in {df_with_embeddings.count()} rows")

        logging.info("Pipeline execution completed successfully!")
        return df_with_embeddings

    except Exception as e:
        logging.error(f"Error during pipeline execution: {str(e)}")
        raise 