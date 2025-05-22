#!/usr/bin/env python3
"""
Script to debug and fix state retrieval issues.
"""
import os
import io
import pandas as pd
from minio import Minio
import traceback
import asyncio
import sys

# Add the app directory to the path so we can import the modules
sys.path.append("/app")

# S3 configuration
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "power-viz")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "False").lower() == "true"

def get_minio_client():
    """
    Get a MinIO client.
    """
    return Minio(
        S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=S3_USE_SSL,
    )

def process_csv_data(file_content):
    """
    Process CSV data directly, similar to what the backend does.
    """
    # Read CSV file
    df = pd.read_csv(io.BytesIO(file_content))
    print(f"Read CSV with {len(df)} rows and columns: {df.columns.tolist()}")
    
    # Check if required columns exist
    required_columns = ["GENID", "PNAME", "PSTATEABB", "GENNTAN", "ORISPL"]
    for col in required_columns:
        if col not in df.columns:
            print(f"Missing required column: {col}")
            return pd.DataFrame()
    
    # Select only the columns we need
    selected_df = df[required_columns].copy()
    
    # Clean data
    # Convert net generation to numeric, handling non-numeric values
    selected_df["GENNTAN"] = pd.to_numeric(selected_df["GENNTAN"], errors="coerce")
    
    # Drop rows with missing values
    initial_rows = len(selected_df)
    selected_df.dropna(subset=["GENNTAN", "PSTATEABB", "PNAME", "ORISPL"], inplace=True)
    final_rows = len(selected_df)
    
    if initial_rows != final_rows:
        print(f"Dropped {initial_rows - final_rows} rows with missing values")
    
    return selected_df

async def get_states_from_backend():
    """
    Import and use the backend function to get states.
    """
    try:
        # Import the modules we need
        from app.services import get_s3_client, get_data_from_s3
        
        # Get a client
        client = get_s3_client()
        print(f"Got S3 client: {type(client)}")
        
        # Get data
        data = await get_data_from_s3(client)
        print(f"Got data from S3: {len(data)} rows")
        
        # Get states
        if not data.empty:
            states = data["PSTATEABB"].unique().tolist()
            states.sort()
            print(f"States from backend function: {states}")
        else:
            print("No data returned from backend function")
            
    except Exception as e:
        print(f"Error getting states from backend: {str(e)}")
        traceback.print_exc()

async def fix_states_direct():
    """
    Fix states by directly modifying the module's cache.
    """
    try:
        client = get_minio_client()
        target_file = "cleaned_sample_power_plants.csv"
        
        # Get the file
        response = client.get_object(S3_BUCKET_NAME, target_file)
        content = response.read()
        
        # Process it
        df = process_csv_data(content)
        
        # Get unique states
        if not df.empty:
            states = df["PSTATEABB"].unique().tolist()
            states.sort()
            print(f"States directly from file: {states}")
            
            # Now forcefully update the module's cache
            print("Attempting to update the module's cache...")
            
            # Try to access the module
            try:
                import app.routes.power_plants
                print(f"Current states_cache: {app.routes.power_plants.states_cache}")
                
                # Replace it
                app.routes.power_plants.states_cache = states
                print(f"Updated states_cache: {app.routes.power_plants.states_cache}")
                
                # Restart the FastAPI application
                print("Restart the backend container for the changes to take effect.")
                
            except Exception as e:
                print(f"Error updating states_cache: {str(e)}")
                traceback.print_exc()
        else:
            print("No data in the file")
            
    except Exception as e:
        print(f"Error fixing states direct: {str(e)}")
        traceback.print_exc()

async def main():
    """
    Main function.
    """
    print("=== Debugging State Retrieval ===")
    
    # Check file in MinIO
    client = get_minio_client()
    print(f"Listing files in bucket: {S3_BUCKET_NAME}")
    objects = list(client.list_objects(S3_BUCKET_NAME, recursive=True))
    
    if not objects:
        print("No files found in the bucket.")
        return
    
    for obj in objects:
        print(f"Found file: {obj.object_name} ({obj.size} bytes)")
    
    print("\n=== Getting States from Backend Function ===")
    await get_states_from_backend()
    
    print("\n=== Fixing States Directly ===")
    await fix_states_direct()

if __name__ == "__main__":
    asyncio.run(main()) 