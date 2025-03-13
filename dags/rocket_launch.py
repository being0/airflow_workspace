from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
import json
from pprint import pprint
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_postgres import BigQueryToPostgresOperator


def process_data(ti, **context):
    launch_data = json.loads(ti.xcom_pull(task_ids="get_launches"))["results"]
    print("Pulled Data:", pprint(launch_data))
    # required_columns = ["rocket_ids", "name", "mission_name", "launch_status", "country", "launch_service_provider"]
    launch_df = pd.DataFrame([{
        "rocket_id": result["rocket"]["id"],
        "rocket_name": result["name"],
        "mission_name": result["mission"]["name"],
        "launch_status": result["status"]["name"],
        "country": result["pad"]["country"]["name"],
        "launch_service_provider": result["launch_service_provider"]["name"],
        "type_launch_service_provider": result["launch_service_provider"]["type"]["name"],
        "window_start": result["window_start"],
        "window_end": result["window_start"],
        "launch_date": context['ds']
    } for result in launch_data])
    pprint(launch_df)
    launch_df.to_parquet(f"/tmp/{context['ds']}.parquet")

def check_launches(**context):
    ti = context['ti']
    launches = json.loads(ti.xcom_pull(task_ids='get_launches'))
    if(launches['count'] == 0):
        raise AirflowSkipException("no launches")

def convert_parquet_to_csv():
    parquet_path = "/tmp/input.parquet"
    csv_path = "/tmp/output.csv"
    # Convert Parquet to CSV
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, index=False)


# Define the DAG
with DAG(
    'rocket_launches',
    schedule_interval='@daily',  # Runs every day
    start_date=datetime(2025, 3, 1),
) as dag: 
    # **Step 1: API Health Check Sensor**
    check_api_health = HttpSensor(
        task_id='check_api_health',
        http_conn_id='space_dev_api',  # Ensure this connection exists in Airflow UI
        endpoint='',  # Checking the base API endpoint
        method='GET',
        poke_interval=60,  # Check every 60 seconds
        timeout=600,  # Wait up to 10 minutes before failing
    )

get_launches = SimpleHttpOperator(
    task_id='get_launches',
    http_conn_id='space_dev_api',
    endpoint="",
    method='GET',
    data="window_start__gte={{data_interval_start | ds}}&window_start__lte={{data_interval_end | ds}}",
    response_filter=lambda response: response.text,  # Store response as text
    log_response=True,
    do_xcom_push=True,
    dag=dag,
)

task_process_data = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

check_launch_happens = PythonOperator(
        task_id="check_launch_happens",
        python_callable=check_launches
    )

process_data_task = PythonOperator(
        task_id="process_data_task",
        python_callable=process_data
    )

store_launches_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id="store_launches_data_to_gcs",
        gcp_conn_id="gcp_default",
        src="/tmp/{{ data_interval_start | ds }}.parquet",
        dst="reza_launches/",
        bucket="aflow-training-bol-2024-03-12",
    )

load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_parquet_to_bigquery",
    bucket="aflow-training-bol-2024-03-12",
    source_objects=["reza_launches/{{ data_interval_start | ds }}.parquet"],  # Path to file in GCS
    destination_project_dataset_table="aflow-training-bol-2024-03-12.aflow_training_bol_2024_03_12.reza_launches",
    source_format="PARQUET",
    write_disposition="WRITE_APPEND",  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    gcp_conn_id="gcp_default",
    autodetect=True,  # Let BigQuery detect schema
    dag=dag,
)

create_postgres_table = SQLExecuteQueryOperator(
        task_id="create_postgres_table",
        sql="""
        CREATE TABLE IF NOT EXISTS launches (
        rocket_id INTEGER,
        rocket_name VARCHAR(100),
        mission_name VARCHAR(100),
        launch_status VARCHAR(100),
        country VARCHAR(100),
        launch_service_provider VARCHAR(100),
        type_launch_service_provider VARCHAR(100),
        window_start timestamp,
        window_end  timestamp,
        launch_date date
        )
        """,
        conn_id="airflow_db",
)

bigquery_to_postgres = BigQueryToPostgresOperator(
        task_id="bigquery_to_postgres",
        postgres_conn_id="airflow_db",
        gcp_conn_id="gcp_default",
        dataset_table=f"aflow_training_bol_2024_03_12.reza_launches",
        target_table_name="launches",
        replace=False,
    )

check_api_health >> get_launches >> check_launch_happens >> process_data_task >> task_process_data >> store_launches_data_to_gcs >> load_to_bigquery >> create_postgres_table >> bigquery_to_postgres