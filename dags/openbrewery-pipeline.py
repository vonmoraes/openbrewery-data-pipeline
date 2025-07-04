from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.insert(0,"/opt/airflow/")
from scripts.extract import extract_breweries
from scripts.transform import transform_breweries_bronze_to_silver

import logging
task_logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'brewery_pipeline',
    default_args=default_args,
    description='ETL pipeline for Open Brewery DB',
    schedule_interval=timedelta(days=7),
)

extract_breweries_task = PythonOperator(
    task_id='get_data_task',
    python_callable=extract_breweries,
    provide_context=True,
    dag=dag,
)


transform_breweries_bronze_to_silver_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_breweries_bronze_to_silver,
    provide_context=True,
    dag=dag,
)

extract_breweries_task >> transform_breweries_bronze_to_silver_task