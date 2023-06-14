from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args={
    "owner": "ismail",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="dag_avec_scheduler",
    default_args=default_args,
    start_date=datetime(2023, 6,13),
    schedule_interval="0 22 * * 1-5",
) as dag :
    task1 = BashOperator(
        task_id="premiere_tache",
        bash_command="echo je suis task!"
    )