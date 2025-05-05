"""

This script serves as the primary pipeline implementation using Spark, separate from the
pandas-specific version. 

- Environment-specific optimization (Spark for Linux/Unix, pandas for Windows)
"""

from pyspark.sql import SparkSession
from ml_pipeline_similarity import run_pipeline
from pyspark.sql.functions import lower, trim
import os

def main():
    # Initializeing Spark session
    spark = SparkSession.builder \
        .appName("MLPipeline") \
        .getOrCreate()

    # Defineing the data source path
    data_path = os.path.join("data", "raw", "Feature_Engineering_product_details___AT.csv")

    # Defineing column transformations using PySpark functions
    columns_transformations = {
        "Name Shop": [
            (lower, "name_shop_lower"),
            (trim, "name_shop_clean")
        ],
        "ABDA Name (Hersteller)": [
            (lower, "description_lower"),
            (trim, "description_clean")
        ]
    }

    # Define columns to generate embeddings for
    embedding_columns = ["name_shop_clean", "description_clean"]

    try:
        # Runing the pipeline
        result_df = run_pipeline(
            spark=spark,
            source=data_path,
            columns_transformations=columns_transformations,
            embedding_columns=embedding_columns,
            is_table=False
        )

        # Ssample results
        print("Pipeline completed successfully!")
        print("\nSample of the processed data:")
        result_df.select("Name Shop", "name_shop_clean", "description_clean", "name_shop_clean_embedding").show(5, truncate=False)

    except Exception as e:
        print(f"Error running pipeline: {str(e)}")
        raise

    finally:
        # Stoping the Spark session
        spark.stop()

if __name__ == "__main__":
    main() 