from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from io import BytesIO
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from minio import Minio
import os

from app.models import PowerPlant
from app.services import get_s3_client, process_csv_data, get_data_from_s3

app = FastAPI(
    title="Power Plant API",
    description="API for visualizing power plant net generation data from EPA's eGRID dataset",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory cache for available states
states_cache = None

@app.get("/")
async def read_root():
    return {"message": "Welcome to the Power Plant API"}

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...), s3_client = Depends(get_s3_client)):
    """
    Upload a CSV file to S3 bucket.
    The file should follow the structure of the GEN23 sheet from EPA's eGRID dataset.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    
    try:
        contents = await file.read()
        
        # Validate CSV structure
        df = pd.read_csv(BytesIO(contents), encoding='utf-8')
        
        # Check if required columns exist
        required_columns = ["GENID", "PNAME", "PSTATEABB", "GENNTAN"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise HTTPException(
                status_code=400, 
                detail=f"CSV file is missing required columns: {', '.join(missing_columns)}"
            )
        
        # Upload to S3
        s3_client.put_object(
            Bucket=os.environ.get("S3_BUCKET_NAME", "power-viz"),
            Key=file.filename,
            Body=contents
        )
        
        # Clear the cache to refresh data
        global states_cache
        states_cache = None
        
        return {"filename": file.filename, "status": "uploaded"}
    
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.get("/states", response_model=List[str])
async def get_states(s3_client = Depends(get_s3_client)):
    """
    Get list of all available states in the dataset
    """
    global states_cache
    
    if states_cache is not None:
        return states_cache
    
    try:
        data = await get_data_from_s3(s3_client)
        if data.empty:
            return []
        
        # Get unique states
        states = data["PSTATEABB"].unique().tolist()
        states.sort()
        
        # Update cache
        states_cache = states
        
        return states
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving states: {str(e)}")

@app.get("/plants", response_model=List[PowerPlant])
async def get_plants(
    state: str = Query(..., description="State abbreviation (e.g., CA, NY)"),
    limit: int = Query(10, description="Number of top plants to return"),
    s3_client = Depends(get_s3_client)
):
    """
    Get top N power plants by net generation for a specific state
    """
    try:
        data = await get_data_from_s3(s3_client)
        if data.empty:
            return []
        
        # Filter by state and calculate totals for each plant
        state_data = data[data["PSTATEABB"] == state]
        
        if state_data.empty:
            return []
        
        # Group by plant and sum the generation values
        plant_totals = state_data.groupby(["ORISPL", "PNAME"]).agg({
            "GENNTAN": "sum"
        }).reset_index()
        
        # Sort by net generation (descending) and take top N
        plant_totals = plant_totals.sort_values("GENNTAN", ascending=False).head(limit)
        
        # Convert to list of PowerPlant models
        plants = [
            PowerPlant(
                id=str(row["ORISPL"]),
                name=row["PNAME"],
                state=state,
                netGeneration=float(row["GENNTAN"])
            )
            for _, row in plant_totals.iterrows()
        ]
        
        return plants
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving plants: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 