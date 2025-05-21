from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Depends
from typing import List, Optional
from io import BytesIO
import pandas as pd
import os
from botocore.exceptions import ClientError

from app.models import PowerPlant
from app.services import get_s3_client, get_data_from_s3
from app.utils.data_cleaner import clean_csv_data, clean_excel_data, convert_to_api_format

router = APIRouter(prefix="/api/power-plants", tags=["power-plants"])

# In-memory cache for available states
states_cache = None

@router.post("/upload")
async def upload_csv(file: UploadFile = File(...), s3_client = Depends(get_s3_client)):
    """
    Upload a CSV file to S3 bucket.
    The file should follow the structure of the GEN23 sheet from EPA's eGRID dataset.
    """
    if not file.filename.endswith(('.csv', '.xlsx')):
        raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")
    
    try:
        contents = await file.read()
        
        # Clean and process the file data
        if file.filename.endswith('.csv'):
            df = clean_csv_data(contents)
        else:
            df = clean_excel_data(contents)
        
        # Convert to API format
        api_df = convert_to_api_format(df)
        
        if api_df.empty:
            raise HTTPException(
                status_code=400, 
                detail="Could not extract required data from the file. Ensure it has the necessary columns."
            )
        
        # Write cleaned data to a buffer
        buffer = BytesIO()
        api_df.to_csv(buffer, index=False)
        buffer.seek(0)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=os.environ.get("S3_BUCKET_NAME", "power-viz"),
            Key=f"cleaned_{file.filename.rsplit('.', 1)[0]}.csv",
            Body=buffer.getvalue()
        )
        
        # Clear the cache to refresh data
        global states_cache
        states_cache = None
        
        return {
            "filename": f"cleaned_{file.filename.rsplit('.', 1)[0]}.csv", 
            "status": "uploaded",
            "records_count": len(api_df)
        }
    
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@router.get("/states", response_model=List[str])
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

@router.get("/", response_model=List[PowerPlant])
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