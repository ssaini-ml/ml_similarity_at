from pyspark.sql import SparkSession
from ml_pipeline.data_loader import load_data
from ml_pipeline.processing import process_dataframe
from ml_pipeline.embeddings import compute_embeddings

def run_pipeline(source, columns_transformations, embedding_columns, is_table=False, query=None):
    """
     Run pipeline folllowing steps 

    1. load data
    2. process data
    3. Embeddings gneration

    Parameters:
        source (str): Path to CSV file or SQL table name
        columns_transformations (dict): Transformations to apply 
        embedding_columns (list): Columns to compute embeddings
        is_table (bool): if true ,source will be treated as table.
        query (str, optional):Custome query to fetch data 

    Returns:
        DataFrame: Transformed DataFrame with embeddings
    """
    spark = SparkSession.builder.appName("MLPipeline").getOrCreate()

    
    df = load_data(spark, source, is_table, query)

    
    df_transformed = process_dataframe(df, columns_transformations)

   
    df_with_embeddings = compute_embeddings(df_transformed, embedding_columns)

   
    df_with_embeddings.write.format("delta").mode("overwrite").saveAsTable("datascience.product_embeddings")

    print("Pipeline execution completed successfully!")

    return df_with_embeddings
