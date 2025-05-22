#!/usr/bin/env python3
"""
Script to check the contents of files in MinIO.
"""
import io
import pandas as pd
from minio import Minio
import os

# S3 configuration
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "power-viz")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "False").lower() == "true"

def main():
    """
    Main function to check MinIO contents.
    """
    print(f"Connecting to MinIO at {S3_ENDPOINT}")
    
    # Create MinIO client
    client = Minio(
        S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=S3_USE_SSL,
    )
    
    # List all files in the bucket
    print(f"Listing files in bucket: {S3_BUCKET_NAME}")
    objects = list(client.list_objects(S3_BUCKET_NAME, recursive=True))
    
    if not objects:
        print("No files found in the bucket.")
        return
    
    for obj in objects:
        print(f"Found file: {obj.object_name} ({obj.size} bytes)")
    
    # Check for the sample power plants file
    target_file = "cleaned_sample_power_plants.csv"
    if any(obj.object_name == target_file for obj in objects):
        print(f"\nRetrieving file: {target_file}")
        
        # Get the file
        response = client.get_object(S3_BUCKET_NAME, target_file)
        content = response.read()
        
        # Read into pandas
        df = pd.read_csv(io.BytesIO(content))
        
        # Print info
        print(f"\nFile contains {len(df)} rows")
        print("\nColumn names:")
        print(df.columns.tolist())
        
        # Check states
        if "PSTATEABB" in df.columns:
            states = df["PSTATEABB"].unique().tolist()
            print(f"\nUnique states in file: {states}")
        else:
            print("\nNo PSTATEABB column found in the file!")
        
        # Print first few rows
        print("\nFirst 5 rows:")
        print(df.head())
    else:
        print(f"\nFile not found: {target_file}")

if __name__ == "__main__":
    main() 