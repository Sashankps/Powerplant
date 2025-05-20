import os
import boto3
from io import BytesIO
import pandas as pd
from typing import List, Dict, Any, Optional
from fastapi import Depends
import asyncio
from minio import Minio
import io

# S3 configuration
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "power-viz")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "False").lower() == "true"

# Cache for the processed data
data_cache = None
data_cache_timestamp = None

def get_s3_client():
    """
    Returns an S3 client (boto3 or MinIO)
    """
    # Check if we're using MinIO or AWS S3
    if "amazonaws.com" in S3_ENDPOINT:
        # Use boto3 for AWS S3
        return boto3.client(
            's3',
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
    else:
        # Use MinIO client for MinIO
        return Minio(
            S3_ENDPOINT,
            access_key=S3_ACCESS_KEY,
            secret_key=S3_SECRET_KEY,
            secure=S3_USE_SSL,
        )

async def get_data_from_s3(s3_client) -> pd.DataFrame:
    """
    Fetches all CSV files from S3, processes them, and returns a consolidated DataFrame
    """
    global data_cache, data_cache_timestamp
    
    # Check if cache is valid (less than 5 minutes old)
    current_time = pd.Timestamp.now()
    if data_cache is not None and data_cache_timestamp is not None:
        cache_age = (current_time - data_cache_timestamp).total_seconds()
        if cache_age < 300:  # 5 minutes
            return data_cache
    
    # List all CSV files in the bucket
    if isinstance(s3_client, boto3.client):
        # boto3 client
        try:
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
            if 'Contents' not in response:
                return pd.DataFrame()
            
            files = [item['Key'] for item in response['Contents'] if item['Key'].endswith('.csv')]
            
            all_data = []
            for file in files:
                obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file)
                file_content = obj['Body'].read()
                df = process_csv_data(BytesIO(file_content))
                all_data.append(df)
            
        except Exception as e:
            print(f"Error fetching data from S3: {e}")
            return pd.DataFrame()
    else:
        # MinIO client
        try:
            objects = s3_client.list_objects(S3_BUCKET_NAME, recursive=True)
            files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
            
            all_data = []
            for file in files:
                response = s3_client.get_object(S3_BUCKET_NAME, file)
                file_content = response.read()
                df = process_csv_data(BytesIO(file_content))
                all_data.append(df)
                
        except Exception as e:
            print(f"Error fetching data from MinIO: {e}")
            return pd.DataFrame()
    
    # Combine all data frames
    if not all_data:
        return pd.DataFrame()
    
    combined_data = pd.concat(all_data, ignore_index=True)
    
    # Update cache
    data_cache = combined_data
    data_cache_timestamp = current_time
    
    return combined_data

def process_csv_data(file_content: BytesIO) -> pd.DataFrame:
    """
    Process the CSV data from the GEN23 sheet
    """
    try:
        # Read CSV file
        df = pd.read_csv(file_content, encoding='utf-8')
        
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
        selected_df.dropna(subset=["GENNTAN", "PSTATEABB", "PNAME", "ORISPL"], inplace=True)
        
        return selected_df
    
    except Exception as e:
        print(f"Error processing CSV data: {e}")
        return pd.DataFrame() 