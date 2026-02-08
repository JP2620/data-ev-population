from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="ev_data_ingestor",
    start_date=datetime(2026, 2, 8),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    run_ingest_ev_data = DockerOperator(
        task_id='ingest_ev_data',
        image='python:3.9-slim',
        command='python -c "print(\'Hello from Docker!\')"',
        docker_url='unix:///var/run/docker.sock',
        network_mode='data-ev-population_default',
    )

    end = EmptyOperator(task_id="end")

    start >> run_ingest_ev_data >> end
