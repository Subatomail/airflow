from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args={
    "owner": "ismail",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

def bonjour(age, ti):
    nom=ti.xcom_pull(task_ids="get_name", key="last_name")
    prenom=ti.xcom_pull(task_ids="get_name", key="first_name")
    print(f"Bonjour, je suis {prenom} {nom} et j'ai {age} ans!")

def get_name(ti):
    ti.xcom_push(key="first_name",value="Tom")
    ti.xcom_push(key="last_name",value="LeChat")

with DAG(
    dag_id="dag_avec_PythonOperator",
    description="c'est un dag avec PythonOperator",
    default_args=default_args,
    start_date=datetime(2023, 6, 12),
    schedule_interval="@daily"
) as dag :
    task1 = PythonOperator(
        task_id="bonjour",
        python_callable=bonjour,
        op_kwargs={"age":23}
    )
    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )
    
    task2 >> task1

    