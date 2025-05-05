def recommend_products(user_id: str, product_df):
    if product_df.empty:
        return []
    recommendations = product_df['product_id'].value_counts().head(5).index.tolist()
    return recommendations