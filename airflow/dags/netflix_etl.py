# airflow/dags/netflix_etl.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mutiara',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'netflix_etl',
    start_date=datetime(2025, 9, 1),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    task_run_etl = BashOperator(
        task_id='run_etl',
        bash_command=(
            'cd "C:/Users/MSI Modern 14/codee/portooo/Netflix Movies and TV Shows" '
            '&& venv\\Scripts\\activate '
            '&& python etl/etl_pipeline.py'
        )
    )
