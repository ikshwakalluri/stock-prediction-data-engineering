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
import concurrent.futures
import shutil

# Define your default arguments
default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# DAG DEFINITION
with DAG(
    dag_id="batch_stock_data_airflow_s3_test",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    def fetch_and_store_data_locally(
        company_symbol,
        batch_num,
        base_temp_dir="/opt/airflow/data_collection/temp_data/",
    ):
        # Fetch historical data from 2000-01-01 to 2024-10-01
        try:
            stock_data = yf.download(
                company_symbol,
                start="2000-01-01",
                end="2024-10-01",
                timeout=30,
                threads=True,
            )
        except Exception as e:
            print(f"Error fetching data for {company_symbol}: {e}")
        stock_data.reset_index(inplace=True)
        stock_data["Year"] = stock_data["Date"].dt.year
        stock_data["Month"] = stock_data["Date"].dt.month

        # Group data by year and save yearly data in Parquet format

        for (year, month), data in stock_data.groupby(["Year", "Month"]):
            # Create partitioned directory for company and year
            partition_dir = os.path.join(
                base_temp_dir,
                f"batch_{batch_num}/company={company_symbol}/year={year}/month={month}/",
            )
            os.makedirs(partition_dir, exist_ok=True)

            # Save the entire year's data to a single Parquet file
            file_path = os.path.join(
                partition_dir, f"{company_symbol}_data_{year}.parquet"
            )
            data.to_parquet(file_path, compression="snappy")

    def compress_folder(folder_path):
        shutil.make_archive(folder_path, "zip", folder_path)
        shutil.rmtree(folder_path)
        print(f"Compressed {folder_path} to {folder_path}.zip")

    # Function to upload batch to AWS S3
    # def upload_batch_to_s3(base_temp_dir, batch_number, bucket_name):
    #     s3_hook = S3Hook(aws_conn_id="aws_default")

    #     # Define the specific batch directory to upload
    #     batch_dir = os.path.join(base_temp_dir, f"batch_{batch_number}")

    #     # Walk through all subdirectories and files within the batch_dir
    #     for root, dirs, files in os.walk(batch_dir):
    #         print(f"Processing directory: {root}")
    #         for file in files:
    #             local_file_path = os.path.join(root, file)

    #             # Create the corresponding S3 key to maintain the directory structure
    #             relative_path = os.path.relpath(
    #                 local_file_path, base_temp_dir
    #             )  # Relative path from temp_dir
    #             s3_key = f"raw_stock_data/batch_{batch_number}/{relative_path}"

    #             # Upload the file to S3
    #             s3_hook.load_file(
    #                 filename=local_file_path,
    #                 key=s3_key,
    #                 bucket_name=bucket_name,
    #                 replace=True,
    #             )
    # print(f"Uploaded {file} to s3://{bucket_name}/{s3_key}")
    def upload_compressed_to_s3(base_temp_dir, batch_num, bucket_name):
        zip_filename = os.path.join(base_temp_dir, f"batch_{batch_num}")
        s3_hook = S3Hook(aws_conn_id="aws_default")
        zip_file_path = f"{zip_filename}.zip"
        s3_key = f"raw_stock_data/"
        # Upload the compressed file to S3
        s3_hook.load_file(
            filename=zip_file_path, key=s3_key, bucket_name=bucket_name, replace=True
        )
        print(f"Uploaded {zip_file_path} to s3://{bucket_name}/{s3_key}")

    # Main function to process batches
    def process_batches():
        base_temp_dir = "/opt/airflow/data_collection/temp_data/"
        df_symbols = pd.read_csv(f"{base_temp_dir}company_tick_symbols_processed.csv")
        df_symbols = df_symbols.head(150)

        bucket_name = "raw-stock-data-airflow"
        if not os.path.exists(base_temp_dir):
            os.makedirs(base_temp_dir)
        batch_size = 100

        for i in range(0, len(df_symbols), batch_size):
            batch_num = i // batch_size + 1
            batch = df_symbols.iloc[i : i + batch_size]
            # for index, row in batch.iterrows():
            #     print(f"Fetching and storing data for {row['Symbol']}")
            #     company_symbol = row["Symbol"]
            #     fetch_and_store_data_locally(company_symbol, batch_num, base_temp_dir)

            # Clean up temp directory after processing
            # compress_folder(f"{base_temp_dir}/batch_{batch_num}")
            upload_compressed_to_s3(
                base_temp_dir,
                batch_num,
                bucket_name,
            )
            try:
                shutil.rmtree(
                    f"{base_temp_dir}/batch_{batch_num}"
                )  # Remove the entire batch directory
                print(f"Cleaned up temp files for batch {batch_num}")
            except Exception as e:
                print(f"Error during cleanup: {e}")

    # Define the tasks
    process_batches_task = PythonOperator(
        task_id="process_batches",
        python_callable=process_batches,
    )
