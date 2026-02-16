FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY dbt/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt