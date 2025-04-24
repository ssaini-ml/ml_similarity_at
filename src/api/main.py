from fastapi import FastAPI
from src.pipeline.product_pipeline import run_pipeline

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "ML Recommendation API is up!"}

@app.get("/recommendations")
def get_recommendations(user_id: str):
    result = run_pipeline(user_id)
    return {"user_id": user_id, "recommendations": result}