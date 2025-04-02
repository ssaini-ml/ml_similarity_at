from pyspark.sql import SparkSession

def load_data(spark, source, is_table=False, query=None):
    """
    Load data from a CSV file, a table, or custom SQL queries.

    Parameters:
        spark (SparkSession): Spark session.
        source (str): CSV file path or a table name.
        is_table (bool): If True, source will be treated as a table name.
        query (str, optional): Custom SQL query to fetch data.

    Returns:
        DataFrame: Spark DataFrame.
    """
    try:
        if query:
            print(f"Executing query: {query}")
            return spark.sql(query)
        elif is_table:
            print(f"Loading data from table: {source}")
            return spark.sql(f"SELECT * FROM {source}")
        else:
            print(f"Loading data from CSV: {source}")
            df = spark.read.csv(source, sep=";", header=True, inferSchema=True)
            return df
    except Exception as e:
        raise Exception(f"Error loading data from {source}: {str(e)}")
