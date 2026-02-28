"""
DAG: enem_pipeline
==================
Operators do Airflow:

- S3KeySensor         → aguarda arquivos existirem no S3
- GlueCrawlerOperator → dispara o crawler nativamente
- GlueCrawlerSensor   → aguarda o crawler terminar (sem polling manual)
- PythonOperator      → lógica customizada em Python
- BashOperator        → comandos de terminal (DBT)
- BranchPythonOperator → desvio condicional de fluxo
- DummyOperator       → marca início/fim de grupos lógicos
"""

import boto3
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "audrey.vasconcelos",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

BUCKET       = "audrey-enem-pipeline"
CRAWLER_NAME = "enem-bronze-crawler"
DBT_DIR      = "/opt/airflow/dbt/enem_analytics"
DBT_PROFILES = f"{DBT_DIR}/.dbt"
DBT_BIN      = "/opt/dbt-venv/bin/dbt" 

def checar_qualidade_pos_dbt(**context):
    """
    BranchPythonOperator: verifica se os marts foram gerados com dados.
    Se estiver ok  → segue para 'pipeline_concluido'
    Se estiver vazio → segue para 'alertar_dados_vazios'
    """
    athena = boto3.client("athena", region_name="us-east-1")

    query = "SELECT COUNT(*) AS total FROM enem_db_marts.mart_desempenho_por_estado"

    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": f"s3://{BUCKET}/athena-results/"
        }
    )

    execution_id = response["QueryExecutionId"]

    # Aguarda resultado
    import time
    for _ in range(10):
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        status = result["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "CANCELLED"):
            raise Exception(f"Query de validação falhou: {status}")
        time.sleep(3)

    rows = athena.get_query_results(QueryExecutionId=execution_id)
    total = int(rows["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])
    log.info(f"Total de registros em mart_desempenho_por_estado: {total}")

    if total > 0:
        return "pipeline_concluido"
    else:
        return "alertar_dados_vazios"


def alertar_dados_vazios(**context):
    """Loga alerta quando marts estão vazios após o DBT rodar."""
    log.error("ALERTA: mart_desempenho_por_estado está vazio após dbt run!")
    raise ValueError("Pipeline concluído mas marts estão vazios. Verificar logs do DBT.")


with DAG(
    dag_id="enem_pipeline",
    description="Pipeline ENEM: S3 → Glue → DBT com múltiplos operators",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 1 * *",
    catchup=False,
    tags=["enem", "glue", "dbt", "athena"],
) as dag:

    # ── EmptyOperator: marca o início do pipeline ─────────────────────────────
    # Facilita leitura do grafo no Airflow UI
    inicio = EmptyOperator(
        task_id="inicio_pipeline"
    )

    # ── S3KeySensor: só continua se os arquivos já existirem no S3 ────────────
    # Evita rodar o crawler em um bucket vazio
    aguardar_arquivo_2023 = S3KeySensor(
        task_id="aguardar_arquivo_bronze_2023",
        bucket_name=BUCKET,
        bucket_key="bronze/enem/ano=2023/*.parquet",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=60 * 10,
        poke_interval=30,
        mode="reschedule",
    )

    aguardar_arquivo_2024 = S3KeySensor(
        task_id="aguardar_arquivo_bronze_2024",
        bucket_name=BUCKET,
        bucket_key="bronze/enem/ano=2024/*.parquet",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=60 * 10,
        poke_interval=30,
        mode="reschedule",
    )

    # ── GlueCrawlerOperator: dispara o crawler nativamente ────────────────────
    disparar_crawler = GlueCrawlerOperator(
        task_id="disparar_glue_crawler",
        config={"Name": CRAWLER_NAME},
        aws_conn_id="aws_default",
    )

    # ── GlueCrawlerSensor: aguarda o crawler terminar ─────────────────────────
    # Substitui loop manual com time.sleep
    aguardar_crawler = GlueCrawlerSensor(
        task_id="aguardar_glue_crawler",
        crawler_name=CRAWLER_NAME,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 20,  # 20 minutos
        mode="reschedule",
    )

    # ── BashOperator: executa o DBT run ───────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            cd {DBT_DIR} &&
            {DBT_BIN} run --profiles-dir {DBT_PROFILES}
        """,
    )

    # ── BashOperator: executa o DBT test ─────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd {DBT_DIR} &&
            {DBT_BIN} test --profiles-dir {DBT_PROFILES}
        """,
    )

    # ── BranchPythonOperator: decide o próximo passo baseado em dados ─────────
    # Uso de fluxo condicional
    checar_qualidade = BranchPythonOperator(
        task_id="checar_qualidade_marts",
        python_callable=checar_qualidade_pos_dbt,
    )

    # ── PythonOperator: alerta quando há problema ─────────────────────────────
    task_alertar = PythonOperator(
        task_id="alertar_dados_vazios",
        python_callable=alertar_dados_vazios,
    )

    # ── EmptyOperator: marca o fim bem-sucedido ───────────────────────────────
    fim = EmptyOperator(
        task_id="pipeline_concluido",
        trigger_rule="none_failed",  # executa mesmo se uma branch foi ignorada
    )

    # ── Definição do fluxo ────────────────────────────────────────────────────
    inicio >> [aguardar_arquivo_2023, aguardar_arquivo_2024]
    [aguardar_arquivo_2023, aguardar_arquivo_2024] >> disparar_crawler
    disparar_crawler >> aguardar_crawler >> dbt_run >> dbt_test
    dbt_test >> checar_qualidade
    checar_qualidade >> [fim, task_alertar]
    task_alertar >> fim