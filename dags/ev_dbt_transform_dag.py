from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

_DBT_ENV = {
    "DB_HOST": Variable.get("DB_HOST"),
    "DB_USER": Variable.get("DB_USER"),
    "DB_PASSWORD": Variable.get("DB_PASSWORD"),
    "DB_PORT": Variable.get("DB_PORT"),
    "DB_NAME": Variable.get("DB_NAME"),
}

_DBT_COMMON = {
    "image": "ev-dbt:latest",
    "docker_url": "unix:///var/run/docker.sock",
    "network_mode": "data-ev-population_default",
    "mount_tmp_dir": False,
    "environment": _DBT_ENV,
}

with DAG(
    dag_id="ev_dbt_transform",
    start_date=datetime(2026, 2, 8),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    source_freshness = DockerOperator(
        task_id="dbt_source_freshness",
        command="dbt source freshness",
        **_DBT_COMMON,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build",
        command="dbt build",
        **_DBT_COMMON,
    )

    end = EmptyOperator(task_id="end")

    start >> source_freshness >> dbt_build >> end
