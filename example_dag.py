from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple Python function
def hello_world():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id="example_dag",
    description="A simple example DAG for testing Airflow setup",
    schedule="@daily",  # Correct keyword for Airflow 2.9+
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
    )
