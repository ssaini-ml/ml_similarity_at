import unittest
from pyspark.sql import SparkSession



from ml_pipeline_similarity_package.ml_pipeline.embeddings  import  compute_embeddings

from pyspark.sql.types import StructType, StructField, StringType

class TestEmbedding(unittest.TestCase): 
  

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestEmbedding").getOrCreate()

    def test_compute_embeddings(self):
 
        data = [("Name", "Category", "Description")]
        schema = StructType([
            StructField("Name_ax", StringType(), True),
            StructField("category", StringType(), True),
            StructField("descriptions", StringType(), True),
        ])
        df = self.spark.createDataFrame(data, schema)

      
        embedding_columns = ["Name_ax", "category", "descriptions"]

       
        df_with_embeddings = compute_embeddings(df, embedding_columns)

        
        for col in embedding_columns:
            self.assertIn(f"{col}_embedding", df_with_embeddings.columns)

if __name__ == "__main__":
    unittest.main()
