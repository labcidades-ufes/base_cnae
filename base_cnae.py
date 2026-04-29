from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

docker_network = os.getenv("DOCKER_NETWORK")

with DAG(
    dag_id="base_cnae",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["cnae", "ibge", "referencia"],
) as dag:

    coleta = DockerOperator(
        task_id="coleta_cnae",
        image="base_cnae-coleta:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=docker_network,
        mount_tmp_dir=False,
    )

    pre_processamento = DockerOperator(
        task_id="pre_processamento_cnae",
        image="base_cnae-pre_processamento:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=docker_network,
        mount_tmp_dir=False,
    )

    processamento = DockerOperator(
        task_id="processamento_cnae",
        image="base_cnae-processamento:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=docker_network,
        mount_tmp_dir=False,
    )

    coleta >> pre_processamento >> processamento
