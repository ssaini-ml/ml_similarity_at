

import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from ml_pipeline_similarity_package.ml_pipeline.data_loader import load_data
from ml_pipeline_similarity_package.ml_pipeline.processing import process_dataframe
from ml_pipeline_similarity_package.ml_pipeline.embeddings import compute_embeddings
from pyspark.sql import SparkSession






def run_pipeline():
    spark = SparkSession.builder.appName("ML Pipeline").getOrCreate()
    source = input("Enter the data source (file path or table name): ").strip()
    is_table = input("Is this a table? (yes/no): ").strip().lower() == "yes"
    columns_transformations = {
        "Name AX": ("initcap", "Name_ax"),
        "Warnungen": [("regexp_replace", "Warnungen_new")]
    }
    embedding_columns = ["Name_ax", "category", "descriptions"]
    df = load_data(spark, source, is_table)
    df = process_dataframe(df, columns_transformations)
    df = compute_embeddings(df, embedding_columns)
    df.show(5)
    return df

if __name__ == "__main__":
    run_pipeline()
