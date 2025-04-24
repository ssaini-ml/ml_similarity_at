from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StructType, StructField, ArrayType, FloatType
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from sentence_transformers import SentenceTransformer


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


def generate_embedding_schema(columns):
    """
    Create output schema for embedded columns.

    Parameters:
        columns (list): List of column names to embed

    Returns:
        StructType: Spark Struct schema for embedding outputs
    """
    return StructType([
        StructField(f"{col_name}_embedding", ArrayType(FloatType())) for col_name in columns
    ])


def compute_embeddings(df: DataFrame, columns: list) -> DataFrame:
    """
    Compute contextual SentenceTransformer embeddings for the specified columns.

    Parameters:
        df (DataFrame): Input Spark DataFrame
        columns (list): List of column names to embed

    Returns:
        DataFrame: DataFrame with added embedding columns for each input column
    """

    embedding_schema = generate_embedding_schema(columns)

    @pandas_udf(embedding_schema)
    def compute_all_embeddings_udf(*columns_data):
        model = get_model()

        def encode_texts(text_list):
      
            num_threads = 4
            chunk_size = len(text_list) // num_threads + 1
            chunks = [text_list[i:i + chunk_size] for i in range(0, len(text_list), chunk_size)]

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                results = list(executor.map(
                    lambda chunk: model.encode(chunk, batch_size=128, show_progress_bar=False),
                    chunks
                ))

            return [emb.tolist() for sublist in results for emb in sublist]

 
        embeddings_dict = {
            f"{col}_embedding": encode_texts(col_data.fillna("").astype(str).tolist())
            for col, col_data in zip(columns, columns_data)
        }

        return pd.DataFrame(embeddings_dict)


    df = df.withColumn("embeddings", compute_all_embeddings_udf(*[col(c) for c in columns]))

    for field in embedding_schema.fields:
        df = df.withColumn(field.name, col(f"embeddings.{field.name}"))

    df = df.drop("embeddings").cache()

    return df 