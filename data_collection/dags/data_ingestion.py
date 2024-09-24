import json
from xml.dom.pulldom import default_bufsize
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
}


with DAG(
    dag_id="data_ingestion", schedule_interval="@daily", default_args=default_args
) as dag:

    def fetch_data():
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=WGZXNCUUJJGWK8QA"
        r = requests.get(url)
        data = r.json()

        filepath = "/opt/airflow/data_collection/temp_data/daily_data.csv"

        with open(filepath, "w") as f:
            json.dump(data, f)
        f.close()
        print("Data fetched and saved to file")

    fetch_data = PythonOperator(task_id="fetch_data", python_callable=fetch_data)

    # save_data = PythonOperator(task_id="save_data", python_callable=save_data)

    fetch_data
