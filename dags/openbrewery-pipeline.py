# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# # from src.extract import extract_breweries
# # import sys
# # sys.path.append('/opt/airflow/src')


# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1,
# }

# with DAG(
#     dag_id='brewery_pipeline',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     description='ETL pipeline for Open Brewery DB',
# ) as dag:

#     extract_task = PythonOperator(
#         task_id='extract_breweries',
#         python_callable=null,
#     )

#     extract_task


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.insert(0,"/opt/airflow/")
from scripts.extract import extract_breweries

import logging
task_logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'brewery_pipeline',
    default_args=default_args,
    description='ETL pipeline for Open Brewery DB',
    schedule_interval=timedelta(days=1),  # frequencia
)

run_task = PythonOperator(
    task_id='get_data_task',
    python_callable=extract_breweries,
    provide_context=True,  # Fornece o contexto para a função
    dag=dag,
)

run_task