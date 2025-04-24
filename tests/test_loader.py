def test_load_product_data():
    from src.data_loader.loader import load_product_data
    import pandas as pd
    import os

    os.makedirs("data/raw", exist_ok=True)
    sample_data = pd.DataFrame({"product_id": [1, 2, 3]})
    sample_path = "data/raw/test_products.csv"
    sample_data.to_csv(sample_path, index=False)

    df = load_product_data(sample_path)
    assert not df.empty
    assert "product_id" in df.columns