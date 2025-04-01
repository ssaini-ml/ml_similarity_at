from pyspark.sql.functions import initcap, regexp_replace, col
from ml_pipeline.pipeline import run_pipeline

source = "dbfs:/product_details/products_AT_de_AT.csv"

columns_transformations = {
    "Name AX": (lambda c: initcap(c), "Name_ax"),  
    "Warnungen": [(lambda c: regexp_replace(c, r'<CautionId>.*?</CautionId>', ''), "Warnungen_cleaned")]
}

embedding_columns = ["Name_ax", "category", "descriptions"]

df_result = run_pipeline(source, columns_transformations, embedding_columns, is_table=False)
