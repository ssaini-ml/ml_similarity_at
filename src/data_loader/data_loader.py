from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data(
    spark: SparkSession,
    source: Optional[str] = None,
    is_table: bool = False,
    options: Optional[Dict] = None,
    join_table: Optional[str] = None,
    join_key: str = "identifier",
    query: Optional[str] = None
) -> DataFrame:
    """
    Dynamically load data from a CSV file, Spark SQL table, or a SQL query, with optional join.

    Parameters:
    -----------
    spark : SparkSession
        The Spark session.
    source : str, optional
        Path to the CSV file or name of a table.
    is_table : bool
        If True, treat `source` as a Spark SQL table name.
    options : dict, optional
        Options for reading the CSV file (e.g., {"sep": ";", "header": True}).
    join_table : str, optional
        Name of a Spark SQL table to join with.
    join_key : str
        Column name to join on. Defaults to "identifier".
    query : str, optional
        If provided, executes this SQL query instead of loading CSV/table.

    Returns:
    --------
    DataFrame
        A Spark DataFrame loaded according to the given parameters.

    Examples:
    ---------
    Load CSV with join:
        df = load_data(spark, source="s3://bucket/data.csv", options={"sep": ";"}, join_table="lookup", join_key="id")

    Load from table:
        df = load_data(spark, source="events_table", is_table=True)

    Load using SQL query:
        df = load_data(spark, query="SELECT * FROM users WHERE age > 30")
    """
    try:
        if query:
            logger.info(f"Executing SQL query.")
            return spark.sql(query)

        if not source:
            raise ValueError("Either `source` or `query` must be provided.")

        if is_table:
            logger.info(f"Loading from table: {source}")
            df = spark.table(source)
        else:
            csv_options = {"header": True, "inferSchema": True, "sep": ","}
            if options:
                csv_options.update(options)
            logger.info(f"Loading CSV from: {source} with options: {csv_options}")
            df = spark.read.options(**csv_options).csv(source)

        if join_table:
            logger.info(f"Joining with table: {join_table} on key: {join_key}")
            df = df.join(spark.table(join_table), join_key, "inner")

        return df

    except Exception as e:
        raise RuntimeError(f"Failed to load data: {e}")

def load_and_query_data(
    spark: SparkSession,
    source: Optional[str] = None,
    temp_view_name: Optional[str] = None,
    sql_query: Optional[str] = None,
    is_table: bool = False,
    options: Optional[Dict] = None,
    join_table: Optional[str] = None,
    join_key: str = "identifier",
    query: Optional[str] = None
) -> DataFrame:
    """
    Load data and execute a SQL query, using either direct SQL or registered temp view.

    Parameters:
    -----------
    spark : SparkSession
        The Spark session.
    source : str, optional
        CSV path or table name.
    temp_view_name : str, optional
        Name of the temporary view to register.
    sql_query : str, optional
        SQL query to execute over the temporary view.
    is_table : bool
        Whether to treat `source` as a table.
    options : dict, optional
        CSV options.
    join_table : str, optional
        Optional table to join with.
    join_key : str
        Column to join on.
    query : str, optional
        Full SQL query to run directly (skips `source` and `sql_query`).

    Returns:
    --------
    DataFrame
        The result of the SQL query.

    Examples:
    ---------
    Run query on loaded CSV:
        df = load_and_query_data(
            spark,
            source="data.csv",
            temp_view_name="my_view",
            sql_query="SELECT col1 FROM my_view WHERE col2 > 100"
        )

    Run direct SQL query:
        df = load_and_query_data(
            spark,
            query="SELECT user_id FROM user_table WHERE region = 'EU'"
        )
    """
    df = load_data(
        spark=spark,
        source=source,
        is_table=is_table,
        options=options,
        join_table=join_table,
        join_key=join_key,
        query=query
    )

    if query:
        return df  

    if not temp_view_name or not sql_query:
        raise ValueError("`temp_view_name` and `sql_query` must be provided if not using `query`.")

    df.createOrReplaceTempView(temp_view_name)
    return spark.sql(sql_query) 