import pandas as pd

def load_product_data(path):
    """Load product data from CSV file"""
    df = pd.read_csv(path)
    return df 