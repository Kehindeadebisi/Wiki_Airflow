from airflow import DAG
from airflow.utils import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

from ./dags/dependencies/includes/views.py import download_views, fetch_views, load_db, analyse_views


with DAG(
    dag_id = "wiki",
    start_date = datetime(2024, 10, 13),

) as dag:

    download_task = PythonOperator(
        task_id="download_data",
        python_callable= download_views
    )

    extract_task = BashOperator(
        task_id="extract_file",
        bash_command='gunzip -q "$/airflow-project/dags/dependencies/includes/views.gz airflow-project/dags/dependencies/includes/views.txt"'
    )

    fetch_task = PythonOperator(
        task_id="fetch",
        python_callable= fetch_views
    )

    load_task= PythonOperator(
        task_id="load",
        python_callable= load_db
    )

    download_task >> extract_task >> fetch_task >> load_task