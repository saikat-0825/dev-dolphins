import pandas as pd
import boto3
import time
import os
import uuid

# ----------- CONFIG -----------

# Google Drive file download (assuming public or you use gdown with auth)
GDRIVE_FILE_URL = "https://drive.google.com/uc?id=169SfI9jSJUICbmeGzuRGrtdVST5-InNv"  # Replace with your file ID

# S3 Setup
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
S3_BUCKET = ""
S3_FOLDER = ""

# Local temp file
LOCAL_FILE = "transactions.csv"

# --------------------------------
# Step 1: Download file from Google Drive (once)

def download_from_gdrive():
    import gdown
    if not os.path.exists(LOCAL_FILE):
        print("Downloading file from Google Drive...")
        gdown.download(GDRIVE_FILE_URL, LOCAL_FILE, quiet=False)

    # Verify the content is a CSV (optional, read first few bytes)
    with open(LOCAL_FILE, "r", encoding="utf-8") as f:
        sample = f.read(8192)
        # Simple check for HTML content which Google Drive sometimes returns
        if "<html" in sample.lower():
            print("ERROR: Downloaded file appears to be HTML, not CSV.")
            os.remove(LOCAL_FILE)
            raise ValueError("Downloaded file is not a valid CSV. Please check the link and permissions.")

def upload_chunk_to_s3(df_chunk, chunk_id, s3_client):
    csv_buffer = df_chunk.to_csv(index=False)
    s3_key = f"{S3_FOLDER}transactions_chunk_{chunk_id}.csv"
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer)
    print(f"Uploaded chunk {chunk_id} to S3.")

def mechanism_X():
    download_from_gdrive()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    chunk_size = 10000
    chunk_id = 0

    try:
        # Try detecting the delimiter for robustness
        with open(LOCAL_FILE, "r", encoding="utf-8") as f:
            sample = f.read(1024)
        if ',' not in sample and '\t' in sample:
            delimiter = '\t'
        else:
            delimiter = ','

        for chunk in pd.read_csv(LOCAL_FILE, chunksize=chunk_size, delimiter=delimiter):
            upload_chunk_to_s3(chunk, chunk_id, s3_client)
            chunk_id += 1
            time.sleep(1)
    except pd.errors.ParserError as e:
        print("Failed to parse the CSV file:", e)
        print("Please verify the file's format and structure.")
        raise

if __name__ == "__main__":
    mechanism_X()
