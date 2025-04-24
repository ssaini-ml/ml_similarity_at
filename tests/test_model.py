def test_recommend_products():
    from src.model.recommender import recommend_products
    import pandas as pd

    df = pd.DataFrame({"product_id": [1, 1, 2, 3, 3, 3]})
    recs = recommend_products("user_123", df)
    assert isinstance(recs, list)
    assert len(recs) <= 5
    assert 3 in recs