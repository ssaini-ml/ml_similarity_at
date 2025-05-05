from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import logging

logger = logging.getLogger(__name__)

def get_model():
    """
    Load and cache the SentenceTransformer model.
    Only loads once during the session.

    Returns:
        SentenceTransformer: Loaded embedding model
    """
    if not hasattr(get_model, "model"):
        get_model.model = SentenceTransformer('distiluse-base-multilingual-cased-v2')
    return get_model.model

def compute_embeddings(df: DataFrame, columns: list) -> DataFrame:
    """
    Compute contextual SentenceTransformer embeddings for the specified columns.

    Parameters:
        df (DataFrame): Input Spark DataFrame
        columns (list): List of column names to embed

    Returns:
        DataFrame: DataFrame with added embedding columns for each input column
    """
    model = get_model()

    def encode_text(text):
        if text is None:
            return None
        try:
            return model.encode(str(text)).tolist()
        except Exception as e:
            logger.error(f"Error encoding text: {str(e)}")
            return None

    # Register UDF for embedding computation
    embed_udf = udf(encode_text, ArrayType(FloatType()))

    # Apply embeddings to each column
    result_df = df
    for column in columns:
        embedding_col = f"{column}_embedding"
        result_df = result_df.withColumn(embedding_col, embed_udf(column))
        logger.info(f"Generated embeddings for column: {column}")

    return result_df 