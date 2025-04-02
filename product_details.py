df = spark.read.csv("dbfs:/product_details/products_AT_de_AT.csv", sep=";", header=True, inferSchema=True)
df.createOrReplaceTempView('product_details_at')



df = spark.sql("""
        select p.*, i.item_id, i.category_caption_level_2, i.family 
        from product_details_at p
        inner join curated.dim_item_detail_pim i 
        on p.identifier = i.identifier
        """)

from pyspark.sql import SparkSession, DataFrame

def load_data(
    spark: SparkSession,
    source_type: str,
    source: str,
    format: str = "parquet",
    options: dict = None,
    database: str = None
) -> DataFrame:
    
    """
    Unified data loader that supports reading from:
    - Files (CSV, Parquet, JSON, etc.)
    - Spark/Hive/Delta tables
    - Custom SQL queries

    Parameters:
    -----------
    spark : SparkSession
        The active SparkSession.
    source_type : str
        One of: 'file', 'table', or 'sql'
    source : str
        - File path if source_type='file'
        - Table name if source_type='table'
        - SQL query string if source_type='sql'
    format : str, optional
        File format for source_type='file'. Default: 'parquet'
    options : dict, optional
        Options like header, delimiter, inferSchema for files.
    database : str, optional
        Used when reading from tables.

    Returns:
    --------
    DataFrame
    """
    source_type = source_type.lower()

    if source_type == "file":
        reader = spark.read
        if options:
            for k, v in options.items():
                reader = reader.option(k, v)
        return reader.format(format).load(source)

    elif source_type == "table":
        full_table_name = f"{database}.{source}" if database else source
        return spark.table(full_table_name)

    elif source_type == "sql":
        return spark.sql(source)

    else:
        raise ValueError("Invalid source_type. Use 'file', 'table', or 'sql'.")


