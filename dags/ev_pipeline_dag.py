from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount

_DB_ENV = {
    "DB_HOST": Variable.get("DB_HOST"),
    "DB_USER": Variable.get("DB_USER"),
    "DB_PASSWORD": Variable.get("DB_PASSWORD"),
    "DB_PORT": Variable.get("DB_PORT"),
    "DB_NAME": Variable.get("DB_NAME"),
}

with DAG(
    dag_id="ev_pipeline",
    start_date=datetime(2026, 2, 8),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    ingest = DockerOperator(
        task_id="ingest_ev_data",
        image="ev-ingestion:latest",
        command="python ingest.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="data-ev-population_default",
        mount_tmp_dir=False,
        environment=_DB_ENV,
        mounts=[
            Mount(
                source=Variable.get("HOST_DATA_DIR"),
                target="/data",
                type="bind",
            )
        ],
    )

    source_freshness = DockerOperator(
        task_id="dbt_source_freshness",
        image="ev-dbt:latest",
        command="dbt source freshness",
        docker_url="unix:///var/run/docker.sock",
        network_mode="data-ev-population_default",
        mount_tmp_dir=False,
        environment=_DB_ENV,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="ev-dbt:latest",
        command="dbt build",
        docker_url="unix:///var/run/docker.sock",
        network_mode="data-ev-population_default",
        mount_tmp_dir=False,
        environment=_DB_ENV,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest >> source_freshness >> dbt_build >> end
