from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args={
    "owner": "ismail",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="notre_premier_dag",
    description="c'est notre premier dag",
    default_args=default_args,
    start_date=datetime(2020, 7, 29, 2),
    schedule_interval="@daily"
) as dag :
    task1 = BashOperator(
        task_id="premiere_tache",
        bash_command="echo je suis task1!"
    )
    task2 = BashOperator(
        task_id="deuxieme_tache",
        bash_command="echo je suis task2!"
    )
    task3 = BashOperator(
        task_id="troisieme_tache",
        bash_command="echo je suis task3!"
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    # task1 >> task2
    # task1 >> task3
    task1 >> [task2,task3]