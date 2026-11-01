from datetime import datetime, timezone
import hashlib
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator

BASE = 'base_cnae'
BUCKET = 'homologacao-airflow-cnae-20261013225408-1e3ef87f'
REDE = 'datalake_9b12ac_airflow'


def referencia_carga(run_id):
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("Identificador da execução ausente.")
    return hashlib.sha256(run_id.encode("utf-8")).hexdigest()

ambiente = {
    "MINIO_ENDPOINT": "{{ conn.minio_curadoria_cnae.host }}:{{ conn.minio_curadoria_cnae.port }}",
    "MINIO_ACCESS_KEY": "{{ conn.minio_curadoria_cnae.login }}",
    "MINIO_SECRET_KEY": "{{ conn.minio_curadoria_cnae.password }}",
    "MINIO_BUCKET": "{{ params.bucket_homologacao }}",
    "MINIO_USE_SSL": "false",
    "CNAE_MODO_FTP_TESTE": "true",
    "CNAE_CARGA_ID": "{{ referencia_carga(run_id) }}",
}

with DAG(
    dag_id=BASE,
    description="Homologação manual das três camadas no MinIO",
    start_date=datetime(2026, 10, 13, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"referencia_carga": referencia_carga},
    is_paused_upon_creation=True,
    default_args={
        "owner": "Julia Ricas Nunes Pereira",
        "email": ["julia.rn.pereira@edu.ufes.br"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    params={
        "bucket_homologacao": Param(
            BUCKET,
            type="string",
            minLength=3,
            maxLength=63,
            pattern="^homologacao-airflow-[a-z0-9-]+$",
        )
    },
    tags=[BASE, "homologacao", "curadoria"],
) as dag:
    tarefas = []

    for etapa in ("coleta", "pre_processamento", "processamento"):
        task_id = (
            f"{etapa}_cnae"
            if BASE == "base_cnae"
            else etapa
        )

        tarefa = DockerOperator(
            task_id=task_id,
            image=f"{BASE}-{etapa}:referencia-20261014T155310Z-1184",
            api_version="auto",
            auto_remove="success",
            docker_url="unix://var/run/docker.sock",
            network_mode=REDE,
            mount_tmp_dir=False,
            environment=ambiente.copy(),
        )
        tarefas.append(tarefa)

    tarefas[0] >> tarefas[1] >> tarefas[2]


# IMAGENS_PROTEGIDAS_CURADORIA_V1
_imagens_protegidas = {'coleta_cnae': 'base_cnae-coleta:datados-20261029T191754Z-760363', 'pre_processamento_cnae': 'base_cnae-pre_processamento:datados-20261029T191754Z-760363', 'processamento_cnae': 'base_cnae-processamento:datados-20261029T191754Z-760363'}
for _tarefa_id, _imagem_id in _imagens_protegidas.items():
    dag.get_task(_tarefa_id).image = _imagem_id
del _tarefa_id, _imagem_id, _imagens_protegidas


# ALERTAS_CURADORIA_SMTP_V1
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

for _tarefa_curadoria in dag.tasks:
    _tarefa_curadoria.email_on_failure = False
    _tarefa_curadoria.email_on_retry = False
    _tarefa_curadoria.on_failure_callback = SmtpNotifier(
        smtp_conn_id="smtp_curadoria",
        from_email="julia.rn.pereira@edu.ufes.br",
        to="julia.rn.pereira@edu.ufes.br",
        subject="[LabCidades][FALHA] {{ ti.dag_id }} / {{ ti.task_id }}",
        html_content=(
            "<p>Uma tarefa de curadoria falhou.</p>"
            "<p>DAG: {{ ti.dag_id }}</p>"
            "<p>Tarefa: {{ ti.task_id }}</p>"
            "<p>Execução: {{ ti.run_id }}</p>"
            "<p>Consulte os logs no Airflow antes de reprocessar.</p>"
            "<p>Se houver arquivos já gravados, preserve a carga "
            "para análise. Para refazer a carga completa, "
            "inicie outra execução com novo run_id.</p>"
        ),
    )
del _tarefa_curadoria


# CAMINHOS_DATADOS_CURADORIA_V1
for _tarefa_data_referencia in dag.tasks:
    _tarefa_data_referencia.environment['CNAE_DATA_REFERENCIA'] = "{{ dag_run.start_date.strftime('%Y%m%d') }}"
del _tarefa_data_referencia
