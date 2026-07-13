import os
import sys
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

sys.path.append(os.path.dirname(__file__))

import builtins

original_open = builtins.open

def mock_open_fn(file, mode='r', *args, **kwargs):
    # Only intercept model.pkl; let other files (like zoneinfo/tz files) open normally
    if "model.pkl" in str(file):
        return MagicMock()
    return original_open(file, mode, *args, **kwargs)

@pytest.fixture
def mock_app():
    # Patch os.path.exists, open, and pickle.load before importing and starting the app
    with patch("os.path.exists", return_value=True), \
         patch("builtins.open", side_effect=mock_open_fn), \
         patch("pickle.load") as mock_pickle_load:
         
        # Create a mock scikit-learn classifier
        mock_clf = MagicMock()
        mock_clf.predict_proba.return_value = [[0.15, 0.85]]
        mock_clf.predict.return_value = [1]
        mock_pickle_load.return_value = mock_clf
        
        from serve import app
        yield app

def test_health_endpoint(mock_app):
    with TestClient(mock_app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

def test_predict_endpoint_success(mock_app):
    with TestClient(mock_app) as client:
        payload = {
            "recency_days": 10,
            "frequency": 5,
            "monetary": 150.0
        }
        response = client.post("/predict", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "churn_probability" in data
        assert "churn_label" in data
        assert data["churn_label"] == 1
        assert data["churn_probability"] == 0.85
