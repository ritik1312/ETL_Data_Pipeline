from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess


def run_etl_script():

    script_path = "/mnt/c/Capstone2/main.py"
    
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Script failed with return code {result.returncode}. Error details: {result.stderr}")


default_args = {
    'start_date': datetime(2024, 9, 9),
    'retries' : 3,
    'retry_delay' : timedelta(minutes=10),
    'execution_timeout' : timedelta(hours=1),
    'catchup': False,
}

with DAG(
        dag_id="Compliance_Security_dag",
        default_args=default_args,
        schedule_interval="0 15 * * *",
        catchup=False
    ) as dag:

    run_script_task = PythonOperator(
        task_id="run_ETL_pipeline",
        python_callable=run_etl_script,
        dag=dag
    )

run_script_task
