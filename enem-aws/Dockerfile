FROM apache/airflow:2.8.0-python3.11

# ─── Virtualenv isolado para o DBT ───────────────────────────────────────────
# Necessário porque o Airflow 2.8 usa opentelemetry-proto==1.22 (protobuf<5)
# enquanto dbt-core 1.11.6 exige protobuf>=6 — conflito irresolvível no mesmo env
USER root
RUN python -m venv /opt/dbt-venv && chown -R airflow: /opt/dbt-venv

USER airflow
# PIP_USER=false necessário porque a imagem do Airflow define PIP_USER=true
# por padrão, o que conflita com instalações em virtualenv
RUN PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-core==1.11.6" \
    "dbt-athena-community==1.10.0" \
    "dbt-adapters==1.22.6" \
    "dbt-common==1.37.2" \
    "dbt-semantic-interfaces==0.9.0" \
    "boto3==1.42.59"

# ─── Provider Amazon no env do Airflow (sem dbt, sem conflito) ───────────────
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==8.19.0" \
    "boto3==1.42.59"