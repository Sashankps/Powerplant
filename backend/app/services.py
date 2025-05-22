import os
import boto3
from io import BytesIO
import pandas as pd
from typing import List, Dict, Any, Optional
from fastapi import Depends
import asyncio
from minio import Minio
import io
import traceback

from app.utils.logger import logger, log_audit

# S3 configuration
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "power-viz")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "False").lower() == "true"

# Export the bucket name for use in other modules
__all__ = ['get_s3_client', 'get_data_from_s3', 'S3_BUCKET_NAME']

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
        logger.info(f"Creating boto3 S3 client for AWS S3 at {S3_ENDPOINT}")
        log_audit("system", "CONNECT", "aws_s3", "SUCCESS", f"Endpoint: {S3_ENDPOINT}")
        return boto3.client(
            's3',
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
    else:
        # Use MinIO client for MinIO
        logger.info(f"Creating MinIO client for endpoint {S3_ENDPOINT}")
        log_audit("system", "CONNECT", "minio", "SUCCESS", f"Endpoint: {S3_ENDPOINT}")
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
            logger.debug("Returning data from cache")
            log_audit("system", "READ", "data_cache", "SUCCESS", "Using cached data")
            return data_cache
    
    logger.info(f"Fetching data from S3 bucket: {S3_BUCKET_NAME}")
    log_audit("system", "READ", f"s3_bucket:{S3_BUCKET_NAME}", "STARTED")
    
    # List all CSV files in the bucket
    if isinstance(s3_client, Minio):
        # MinIO client
        try:
            logger.debug("Using MinIO client to list objects")
            objects = s3_client.list_objects(S3_BUCKET_NAME, recursive=True)
            files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
            logger.info(f"Found {len(files)} CSV files in the bucket")
            
            all_data = []
            for file in files:
                logger.debug(f"Fetching file: {file}")
                log_audit("system", "READ", f"s3_file:{file}", "STARTED")
                response = s3_client.get_object(S3_BUCKET_NAME, file)
                file_content = response.read()
                logger.debug(f"Processing file: {file} ({len(file_content)} bytes)")
                df = process_csv_data(BytesIO(file_content))
                if not df.empty:
                    all_data.append(df)
                    logger.debug(f"Processed {len(df)} rows from {file}")
                    log_audit("system", "READ", f"s3_file:{file}", "SUCCESS", f"Processed {len(df)} rows")
                else:
                    logger.warning(f"Empty dataframe after processing {file}")
                    log_audit("system", "READ", f"s3_file:{file}", "WARNING", "Empty dataframe after processing")
                
        except Exception as e:
            logger.error(f"Error fetching data from MinIO: {str(e)}")
            logger.error(traceback.format_exc())
            log_audit("system", "READ", f"s3_bucket:{S3_BUCKET_NAME}", "FAILURE", f"Error: {str(e)}")
            return pd.DataFrame()
    else:
        # boto3 client (default case)
        try:
            logger.debug("Using boto3 client to list objects")
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
            if 'Contents' not in response:
                logger.warning(f"No contents found in bucket: {S3_BUCKET_NAME}")
                log_audit("system", "READ", f"s3_bucket:{S3_BUCKET_NAME}", "WARNING", "No contents found")
                return pd.DataFrame()
            
            files = [item['Key'] for item in response['Contents'] if item['Key'].endswith('.csv')]
            logger.info(f"Found {len(files)} CSV files in the bucket")
            
            all_data = []
            for file in files:
                logger.debug(f"Fetching file: {file}")
                log_audit("system", "READ", f"s3_file:{file}", "STARTED")
                obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file)
                file_content = obj['Body'].read()
                logger.debug(f"Processing file: {file} ({len(file_content)} bytes)")
                df = process_csv_data(BytesIO(file_content))
                if not df.empty:
                    all_data.append(df)
                    logger.debug(f"Processed {len(df)} rows from {file}")
                    log_audit("system", "READ", f"s3_file:{file}", "SUCCESS", f"Processed {len(df)} rows")
                else:
                    logger.warning(f"Empty dataframe after processing {file}")
                    log_audit("system", "READ", f"s3_file:{file}", "WARNING", "Empty dataframe after processing")
            
        except Exception as e:
            logger.error(f"Error fetching data from S3: {str(e)}")
            logger.error(traceback.format_exc())
            log_audit("system", "READ", f"s3_bucket:{S3_BUCKET_NAME}", "FAILURE", f"Error: {str(e)}")
            return pd.DataFrame()
    
    # Combine all data frames
    if not all_data:
        logger.warning("No valid data found in any files")
        log_audit("system", "READ", f"s3_bucket:{S3_BUCKET_NAME}", "WARNING", "No valid data found in any files")
        return pd.DataFrame()
    
    combined_data = pd.concat(all_data, ignore_index=True)
    logger.info(f"Combined data has {len(combined_data)} rows")
    log_audit("system", "PROCESS", "combined_data", "SUCCESS", f"Combined {len(all_data)} files, {len(combined_data)} rows")
    
    # Update cache
    data_cache = combined_data
    data_cache_timestamp = current_time
    logger.debug("Updated data cache")
    
    return combined_data

def process_csv_data(file_content: BytesIO) -> pd.DataFrame:
    """
    Process the CSV data from the GEN23 sheet
    """
    try:
        # Read CSV file
        df = pd.read_csv(file_content, encoding='utf-8')
        logger.debug(f"Read CSV with {len(df)} rows")
        
        # Check if required columns exist
        required_columns = ["GENID", "PNAME", "PSTATEABB", "GENNTAN", "ORISPL"]
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Missing required column: {col}")
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
            logger.debug(f"Dropped {initial_rows - final_rows} rows with missing values")
        
        return selected_df
    
    except Exception as e:
        logger.error(f"Error processing CSV data: {str(e)}")
        logger.error(traceback.format_exc())
        log_audit("system", "PROCESS", "csv_data", "FAILURE", f"Error: {str(e)}")
        return pd.DataFrame() 