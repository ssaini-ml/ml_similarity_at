from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import pandas as pd
from typing import List, Dict
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="ML Recommendation API",
    description="API for text processing and embedding generation",
    version="1.0.0"
)

# Initialize the model globally
model = SentenceTransformer('distiluse-base-multilingual-cased-v2')

def clean_text(text: str) -> str:
    if pd.isna(text):
        return ""
    return str(text).lower().strip()

class TextInput(BaseModel):
    name_shop: str
    description: str

class TextResponse(BaseModel):
    name_shop_clean: str
    description_clean: str
    name_shop_embedding: List[float]
    description_embedding: List[float]

class BatchResponse(BaseModel):
    processed_count: int
    results: List[TextResponse]

def process_dataframe(df: pd.DataFrame) -> List[dict]:
    results = []
    for _, row in df.iterrows():
        try:
            name_shop_clean = clean_text(row['Name Shop'])
            description_clean = clean_text(row['ABDA Name (Hersteller)'])
            
            name_shop_embedding = model.encode(name_shop_clean).tolist()
            description_embedding = model.encode(description_clean).tolist()
            
            results.append({
                "name_shop_clean": name_shop_clean,
                "description_clean": description_clean,
                "name_shop_embedding": name_shop_embedding,
                "description_embedding": description_embedding
            })
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            continue
    
    return results

@app.post("/process", response_model=TextResponse)
async def process_text(input_data: TextInput):
    try:
        # Clean text
        name_shop_clean = clean_text(input_data.name_shop)
        description_clean = clean_text(input_data.description)

        # Generate embeddings
        name_shop_embedding = model.encode(name_shop_clean).tolist()
        description_embedding = model.encode(description_clean).tolist()

        return {
            "name_shop_clean": name_shop_clean,
            "description_clean": description_clean,
            "name_shop_embedding": name_shop_embedding,
            "description_embedding": description_embedding
        }
    except Exception as e:
        logger.error(f"Error processing text: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process_csv", response_model=BatchResponse)
async def process_csv_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded CSV file
        df = pd.read_csv(file.file)
        results = process_dataframe(df)
        
        return {
            "processed_count": len(results),
            "results": results
        }
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process_source_csv", response_model=BatchResponse)
async def process_source_csv():
    try:
        # Path to the source CSV file
        data_path = os.path.join("data", "raw", "Feature_Engineering_product_details___AT.csv")
        
        # Check if file exists
        if not os.path.exists(data_path):
            raise HTTPException(status_code=404, detail="Source CSV file not found")
        
        # Read and process the CSV file
        df = pd.read_csv(data_path)
        results = process_dataframe(df)
        
        return {
            "processed_count": len(results),
            "results": results
        }
    except Exception as e:
        logger.error(f"Error processing source CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy", "model_loaded": model is not None}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 