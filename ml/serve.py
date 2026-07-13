import os
import pickle
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd

model = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global model
    model_path = os.environ.get("MODEL_PATH", "/app/scripts/model.pkl")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at {model_path}")
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    yield


app = FastAPI(title="Churn Prediction Serving API", lifespan=lifespan)


class ChurnPredictionRequest(BaseModel):
    recency_days: int
    frequency: int
    monetary: float


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/predict")
async def predict(payload: ChurnPredictionRequest):
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded.")

    features_df = pd.DataFrame(
        [
            {
                "recency_days": payload.recency_days,
                "frequency": payload.frequency,
                "monetary": payload.monetary,
            }
        ]
    )

    try:
        churn_prob = float(model.predict_proba(features_df)[0][1])
        churn_label = int(model.predict(features_df)[0])

        return {"churn_probability": churn_prob, "churn_label": churn_label}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Prediction inference failed: {str(e)}"
        )
