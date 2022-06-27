import os
from datetime import datetime

from airflow.operators.python import PythonOperator
from helpers import get_data_callable, ingest_callable, transform_callable

from airflow import DAG

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DATABASE = os.getenv('POSTGRES_DB')
POSTGRES_TABLE = os.getenv('POSTGRES_TABLE')

local_workflow = DAG(
    'LocalIngestionDag',
    schedule_interval='* * * * *',
    start_date=datetime(2022, 1, 1),
    catchup=False
)

URL_PREFIX = 'https://api.coincap.io/v2/assets/'

with local_workflow:
    wget_task = PythonOperator(
        task_id='wget',
        python_callable=get_data_callable,
        op_kwargs=dict(
            url=URL_PREFIX
        )
    )

    transform_taks = PythonOperator(
        task_id='transform',
        python_callable=transform_callable,
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            db=POSTGRES_DATABASE,
            table_name=POSTGRES_TABLE,
        )
    )

    wget_task >> transform_taks >> ingest_task
