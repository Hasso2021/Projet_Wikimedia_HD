"""DAG de test — vérifie qu'Airflow 3 exécute bien une tâche Bash."""

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="hello_world",
    description="Test minimal : confirme que la stack Airflow répond",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["wikimedia", "demo"],
) as dag:
    BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow 3 — stack is working!"',
    )
