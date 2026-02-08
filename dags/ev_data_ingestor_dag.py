from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount

with DAG(
    dag_id="ev_data_ingestor",
    start_date=datetime(2026, 2, 8),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    run_ingest_ev_data = DockerOperator(
        task_id='ingest_ev_data',
        image='ev-ingestion:latest',
        command='python ingest.py',
        docker_url='unix:///var/run/docker.sock',
        network_mode='data-ev-population_default',
        environment={
            "DB_HOST": Variable.get("DB_HOST"),
            "DB_USER": Variable.get("DB_USER"),
            "DB_PASSWORD": Variable.get("DB_PASSWORD"),
            "DB_PORT": Variable.get("DB_PORT"),
            "DB_NAME": Variable.get("DB_NAME"),
        },
        mounts=[
            Mount(
                source=Variable.get("HOST_DATA_DIR"),
                target="/data",
                type="bind",
            )
        ]
    )

    end = EmptyOperator(task_id="end")

    start >> run_ingest_ev_data >> end
