def load_product_data(path: str):
    import pandas as pd
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        raise RuntimeError(f"Error loading data: {e}")