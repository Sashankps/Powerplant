# External Services Requirements

This application requires the following external services:

## 1. S3-Compatible Object Storage

You have two options for S3-compatible storage:

### Option A: AWS S3 (Production)

For production environments, you can use Amazon S3:

1. Create an AWS account if you don't have one: https://aws.amazon.com/
2. Create an S3 bucket
3. Create an IAM user with programmatic access and assign the AmazonS3FullAccess policy
4. Note the Access Key ID and Secret Access Key for configuration

Configure the application by setting the following environment variables:
```
S3_ENDPOINT=s3.amazonaws.com
S3_ACCESS_KEY=your_access_key
S3_SECRET_KEY=your_secret_key
S3_BUCKET_NAME=your_bucket_name
S3_USE_SSL=True
```

### Option B: MinIO (Development/Testing)

For local development or testing, you can use MinIO (already included in docker-compose.yml):

MinIO runs within the docker-compose setup, so no external account is needed. The default credentials are:
- Username: minioadmin
- Password: minioadmin

The MinIO Console will be available at http://localhost:9001 after running docker-compose.

## 2. Database (Optional - For Future Extensions)

The current implementation uses file-based storage, but if you want to extend the application with a database:

### Option: PostgreSQL

1. Create a PostgreSQL database
2. Update the application code to use the database
3. Add the database connection details to the environment variables

## No Additional Services Required

Besides the S3-compatible storage, the application does not currently require any other external services. All components (frontend, backend, storage) are containerized and included in the docker-compose.yml file.

To run the application with all required services:
```bash
docker-compose up -d
```

## Data Source

The application requires EPA's eGRID 2023 dataset (specifically the GEN23 sheet) to be uploaded as a CSV file. Download it from:
https://www.epa.gov/system/files/documents/2025-01/egrid2023_data_rev1.xlsx 