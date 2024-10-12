from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def upload_local_file_to_s3():
    # Initialize S3Hook
    s3_hook = S3Hook(
        aws_conn_id="aws_default"
    )  # Use the AWS connection (or environment variables)
    bucket_name = "raw-stock-data-airflow"
    local_file_path = "/opt/airflow/data_collection/temp_data/daily_data.csv"  # Path to the file in the Docker container
    s3_key = "test_airflow_local/test_data.csv"  # The path inside the S3 bucket

    # Upload the local file to S3
    s3_hook.load_file(
        filename=local_file_path,  # Local file path
        key=s3_key,  # S3 key (destination path)
        bucket_name=bucket_name,
        replace=True,  # Overwrite the file if it exists
    )
    print(f"File successfully uploaded to s3://{bucket_name}/{s3_key}")


# Define the DAG
with DAG(
    dag_id="airflow_test_s3_upload_local_file",
    schedule_interval=None,  # Run the DAG manually for testing
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define a Python task that runs the upload_local_file_to_s3 function
    upload_task = PythonOperator(
        task_id="upload_local_file_task", python_callable=upload_local_file_to_s3
    )

    upload_task
