from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args={
    "owner": "ismail",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

@dag(dag_id="dag_avec_taskflow",
    default_args=default_args,
    start_date=datetime(2023, 6, 12),
    schedule_interval="@daily")
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name':'Tom',
        'last_name':'LeChat'}

    @task()
    def get_age():
        return 23

    @task()
    def bonjour(nom,prenom,age):
        print(f"Bonjour, je suis {prenom} {nom} et j'ai {age} ans !")

    nom_dict=get_name()
    age=get_age()
    bonjour(nom=nom_dict['last_name'],prenom=nom_dict['first_name'],age=age)

bonjour_dag=hello_world_etl()
