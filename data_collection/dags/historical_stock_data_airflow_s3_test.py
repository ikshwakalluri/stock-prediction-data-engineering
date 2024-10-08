import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd
import yfinance as yf
import io
import os

# Define your default arguments
default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# DAG DEFINITION
with DAG(
    dag_id="historical_stock_data_airflow_s3_test",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # Function to fetch data from Yahoo Finance and store it in a local file per batch
    def fetch_batch_data(batch, temp_dir):

        os.makedirs(
            temp_dir, exist_ok=True
        )  # Create temp directory if it doesn't exist
        for index, row in batch.iterrows():
            company_symbol = row["Symbol"]
            print(f"Fetching data for {company_symbol}")
            # Fetch stock data using yfinance
            stock_data = yf.download(
                company_symbol, start="2000-01-01", end="2024-10-01"
            )

            # Save data locally
            file_path = os.path.join(temp_dir, f"{company_symbol}.parquet")
            stock_data.to_parquet(file_path, compression="snappy")
            print(f"Saved {company_symbol} data to {file_path}")

    # Function to upload the local batch data to AWS S3
    def upload_batch_to_s3(temp_dir, batch_number, bucket_name):
        s3_hook = S3Hook(aws_conn_id="aws_default")
        # Iterate through files in the temp directory
        for filename in os.listdir(temp_dir):
            local_file_path = os.path.join(temp_dir, filename)
            s3_key = f"stock_data/batch_{batch_number}/{filename}"
            s3_hook.load_file(
                filename=local_file_path,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True,
            )
            print(f"Uploaded {filename} to s3://{bucket_name}/{s3_key}")


temp_dir = "/opt/airflow/data_collection/temp_data/"
