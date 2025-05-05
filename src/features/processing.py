from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_replace, trim, concat_ws
from typing import List, Dict, Optional, Tuple

def clean_text_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Clean a text column by performing the following transformations:
    - Removing HTML tags
    - Converting to lowercase
    - Removing special characters and extra whitespace
    - Trimming whitespace

    Parameters:
    -----------
    df : DataFrame
        Input Spark DataFrame.
    column_name : str
        Name of the text column to clean.

    Returns:
    --------
    DataFrame
        DataFrame with cleaned text column.
    """
   
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'<[^>]*>', ''))
    

    df = df.withColumn(column_name, lower(col(column_name)))
    
 
    df = df.withColumn(column_name, regexp_replace(regexp_replace(col(column_name), r'[^\w\s]', ' '), r'\s+', ' '))
    
    
    df = df.withColumn(column_name, trim(col(column_name)))

    return df

def process_dataframe(
    df: DataFrame,
    columns_transformations: Dict[str, List[Tuple]] = {},  
    text_columns: Optional[List[str]] = None,  
    combine_columns: bool = True,  
    combined_column_name: str = "description"  
) -> DataFrame:
    """
    Process the DataFrame by applying transformations and optionally combining columns.
    
    Parameters:
    -----------
    df : DataFrame
        Input Spark DataFrame with product data.
    columns_transformations : dict, optional
        Dictionary mapping column names to transformations.
        Example:
            {"column_name": [(transformation_func, new_column_name)]}
    text_columns : list, optional
        List of text column names to process for cleaning (removing HTML tags, special characters, etc.).
    combine_columns : bool, optional
        If True, combine the processed text columns into one (default is True).
    combined_column_name : str, optional
        The name of the combined text column if `combine_columns` is True (default is "description").
    
    Returns:
    --------
    DataFrame
        Processed DataFrame with transformations applied.
    
    Example:
    --------
    # Apply transformations
    transformations = {
        "name": [(transform_func, "name_cleaned")],
        "category": [(transform_func, "category_cleaned")]
    }
    df = process_dataframe(df, columns_transformations=transformations)

    # Clean and combine text columns
    df = process_dataframe(df, text_columns=["name", "category", "description"], combine_columns=True)
    """
    

    for col_name, operations in columns_transformations.items():
        for transformation, new_name in operations:
            df = df.withColumn(new_name, transformation(df[col_name]))

    
    if text_columns:
        for col_name in text_columns:
            if col_name in df.columns:
                df = clean_text_column(df, col_name)


    if combine_columns and text_columns:
        valid_columns = [col(c) for c in text_columns if c in df.columns]
        if valid_columns:
            df = df.withColumn(combined_column_name, concat_ws(" ", *valid_columns))

    return df 