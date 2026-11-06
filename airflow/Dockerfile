FROM apache/airflow:2.10.2-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

USER airflow

# 1. ติดตั้ง AWS Provider ให้ระบบหลักของ Airflow ใช้เรียก Lambda
RUN uv pip install --no-cache apache-airflow-providers-amazon

# 2. สร้าง Virtual Environment แยกสำหรับ dbt Core + Athena
RUN uv venv /opt/airflow/dbt_venv && \
    uv pip install --python /opt/airflow/dbt_venv/bin/python --no-cache \
    dbt-athena-community