from pyspark.sql.functions import regexp_replace, concat_ws, initcap, expr

def process_dataframe(df, columns_transformations):
    """
    Process DataFrame based on user-defined operations.

    Parameters:
        df (DataFrame): Spark DataFrame.
        columns_transformations (dict): Dictionary mapping column names to transformations.
        
        - Keys: Column names in the original DataFrame.
        - Values:
          - Tuple (transformation function, new_column_name) for a single operation.
          - List of tuples [(transformation function, new_column_name)] for multiple operations.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    try:
        for col_name, operations in columns_transformations.items():
            if not operations:  
                continue
            
            if isinstance(operations, tuple):
                transformation, new_name = operations
                df = df.withColumn(new_name, transformation(df[col_name])) if transformation else df
            elif isinstance(operations, list):
                for transformation, new_name in operations:
                    df = df.withColumn(new_name, transformation(df[col_name]))
        
       
        df = df.drop(*columns_transformations.keys())

       
        df = df.withColumn(
            "category",
            concat_ws(" ", df["url_prefix"], df["category_caption_level_2"], df["family"])
        )

        df = df.withColumn(
            "descriptions",
            concat_ws(" ", df["Langbeschreibung_new"], df["Keywords"], df["ABDA Warengruppe Code"], df["seo_slug"])
        )

        df = df.withColumn("descriptions", regexp_replace(df["descriptions"], r'\s{3,}', ' '))

        return df

    except Exception as e:
        raise Exception(f"Error in processing DataFrame: {e}")
