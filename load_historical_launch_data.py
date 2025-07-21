from datetime import datetime, timedelta
import requests
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from db_utils import load_to_postgres, load_to_mongodb, load_to_neo4j, load_to_qdrant


def extract_historical_launches(**context):
    url = "https://ll.thespacedevs.com/2.2.0/launch/?window_end__lt=now&limit=100"
    all_launches = []

    while url:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        launches = data.get("results", [])
        for launch in launches:
            try:
                all_launches.append({
                    "id": launch["id"],
                    "mission": launch["name"],
                    "net": launch["net"],
                    "provider": launch["launch_service_provider"]["name"],
                    "rocket": launch["rocket"]["configuration"]["name"],
                    "pad": launch["pad"]["name"],
                    "orbit": launch.get("mission", {}).get("orbit", {}).get("name", "Unknown"),
                    "description": launch.get("mission", {}).get("description", "")
                })
            except Exception as e:
                logging.warning(f"Skipping launch due to error: {e}")

        url = data.get("next")  # Get next page if exists

    context['ti'].xcom_push(key='launch_data', value=all_launches)


def load_all_dbs(**context):
    launches = context['ti'].xcom_pull(task_ids='extract_historical_launches', key='launch_data')
    for launch in launches:
        load_to_postgres(launch)
        load_to_mongodb(launch)
        load_to_neo4j(launch)
        load_to_qdrant(launch)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'launch_ingest_dag',
    default_args=default_args,
    description='Ingests all historical launches into Postgres, Mongo, Neo4j, Qdrant',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spacelaunch']
)

with dag:
    extract_task = PythonOperator(
        task_id='extract_historical_launches',
        python_callable=extract_historical_launches,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_databases',
        python_callable=load_all_dbs,
        provide_context=True
    )

    extract_task >> load_task
