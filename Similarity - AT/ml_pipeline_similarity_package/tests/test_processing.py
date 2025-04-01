import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from ml_pipeline.processing import process_dataframe
from pyspark.sql.functions import initcap, regexp_replace

class TestProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestProcessing").getOrCreate()

    def test_process_dataframe(self):
     
        data = [("test name", "<CautionId>123</CautionId> Warning text")]
        df = self.spark.createDataFrame(data, ["Name AX", "Warnungen"])

        
        transformations = {
            "Name AX": (lambda c: initcap(c), "Name_ax"),
            "Warnungen": [(lambda c: regexp_replace(c, r'<CautionId>.*?</CautionId>', ''), "Warnungen_cleaned")]
        }

       
        transformed_df = process_dataframe(df, transformations)

        
        self.assertIn("Name_ax", transformed_df.columns)
        self.assertIn("Warnungen_cleaned", transformed_df.columns)

        result = transformed_df.collect()[0]
        self.assertEqual(result["Name_ax"], "Test Name")  
        self.assertEqual(result["Warnungen_cleaned"], " Warning text")  

if __name__ == "__main__":
    unittest.main()
