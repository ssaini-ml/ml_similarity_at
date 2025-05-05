def recommend_products(user_id, df, n=5):
    """Generate product recommendations for a user"""
    # For now, just return the top n most frequent products
    top_products = df['product_id'].value_counts().head(n).index.tolist()
    return top_products 