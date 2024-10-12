import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
}

# DAG DEFINITION
with DAG(
    dag_id="stock_data_airflow_test",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    def fetch_data():
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=full&apikey=WGZXNCUUJJGWK8QA"
        r = requests.get(url)
        data = r.json()
        filepath = "/opt/airflow/data_collection/temp_data/raw_stock_data.csv"

        with open(filepath, "w") as f:
            json.dump(data, f)
        f.close()
        print("Data fetched and saved to local file")

    def pre_process_data():
        input_filepath = "/opt/airflow/data_collection/temp_data/raw_stock_data.csv"
        output_filepath = (
            "/opt/airflow/data_collection/temp_data/processed_stock_data.csv"
        )

        with open(input_filepath, "r") as f:
            data = json.load(f)
        f.close()
        df = pd.DataFrame(data)
        df1 = pd.DataFrame()
        df.reset_index(inplace=True)
        compnay_name = df["Meta Data"][1]
        df.drop(["Meta Data"], axis=1, inplace=True)
        df.drop(index=df.index[0:5], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df1["Date"] = df["index"].apply(lambda x: x)
        df1["Company_Name"] = compnay_name
        df1["Open"] = df["Time Series (Daily)"].apply(lambda x: x["1. open"])
        df1["High"] = df["Time Series (Daily)"].apply(lambda x: x["2. high"])
        df1["Low"] = df["Time Series (Daily)"].apply(lambda x: x["3. low"])
        df1["Close"] = df["Time Series (Daily)"].apply(lambda x: x["4. close"])
        df1["Volume"] = df["Time Series (Daily)"].apply(lambda x: x["5. volume"])

        df1.to_csv(output_filepath, index=False)

        print("Data pre-processed")

    def upload_local_file_to_s3():
        # Initialize S3Hook
        s3_hook = S3Hook(
            aws_conn_id="aws_default"
        )  # Use the AWS connection (or environment variables)
        bucket_name = "raw-stock-data-airflow"
        local_file_path = "/opt/airflow/data_collection/temp_data/processed_stock_data.csv"  # Path to the file in the Docker container
        s3_key = "test_airflow_local/test_processed_data.csv"  # The path inside the S3 bucket

        # Upload the local file to S3
        s3_hook.load_file(
            filename=local_file_path,  # Local file path
            key=s3_key,  # S3 key (destination path)
            bucket_name=bucket_name,
            replace=True,  # Overwrite the file if it exists
        )
        print(f"File successfully uploaded to s3://{bucket_name}/{s3_key}")

    ## TASKS

    # Task 1 : Fetch data
    fetch_data = PythonOperator(task_id="fetch_data", python_callable=fetch_data)
    # Task 2 : Pre-process data
    pre_process_data = PythonOperator(
        task_id="pre_process_data", python_callable=pre_process_data
    )
    # Task 3 : Upload data to S3
    upload_task = PythonOperator(
        task_id="upload_local_file_task", python_callable=upload_local_file_to_s3
    )

    # TASK DEPENDENCIES
    fetch_data >> pre_process_data >> upload_task
