from src.data_loader.loader import load_product_data
from src.model.recommender import recommend_products

def run_pipeline(user_id: str):
    data_path = "data/raw/products.csv"
    df = load_product_data(data_path)
    recs = recommend_products(user_id, df)
    return recs