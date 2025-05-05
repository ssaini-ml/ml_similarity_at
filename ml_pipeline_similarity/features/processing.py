from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Clean a text column by converting to lowercase and trimming whitespace.
    
    Parameters:
   
    df : DataFrame
        Input Spark DataFrame
    column_name : str
        Name of the column to clean
        
    Returns:
  
    DataFrame
        DataFrame with cleaned column
    """
    return df.withColumn(column_name, trim(lower(col(column_name))))

def process_dataframe(df: DataFrame, columns_transformations: dict) -> DataFrame:
    """
    Apply transformations to specified columns in the DataFrame.
    
    Parameters:
  
    df : DataFrame
        Input Spark DataFrame
    columns_transformations : dict
        Dictionary containing column transformations to apply.
        Format: {"column_name": [(transformation_function, "new_column_name")]}
        
    Returns:
   
    DataFrame
        Transformed DataFrame
    """
    try:
        result_df = df
        
        for column_name, transformations in columns_transformations.items():
            for transform_func, new_column_name in transformations:
                result_df = result_df.withColumn(new_column_name, transform_func(col(column_name)))
                logger.info(f"Applied transformation to {column_name} -> {new_column_name}")
                
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing dataframe: {str(e)}")
        raise 