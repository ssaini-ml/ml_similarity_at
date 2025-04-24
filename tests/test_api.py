from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "ML Recommendation API is up!"}

def test_get_recommendations():
    response = client.get("/recommendations?user_id=testuser")
    assert response.status_code == 200
    assert "recommendations" in response.json()