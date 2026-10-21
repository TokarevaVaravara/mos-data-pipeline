# Airflow 3.3.2 (Python 3.11) + Java 17 + отдельный venv проекта (pyspark 3.5.9 и requirements.txt).
# Код проекта (src, sql, dags) и data/ подключаются томами из docker-compose.yml.
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root
# JRE без GUI-библиотек: Spark нужна только JVM; procps (ps) использует скрипт запуска Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# venv проекта: точные версии из requirements.txt, те же, что в локальных тестах; Airflow сюда не ставится
COPY requirements.txt /opt/project-requirements.txt
RUN python -m venv /opt/project-venv \
    && /opt/project-venv/bin/pip install --no-cache-dir -r /opt/project-requirements.txt

USER airflow
# в окружение Airflow - только pytest для тестов DAG, по constraints этой версии Airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pytest==8.3.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ENV PROJECT_DIR=/opt/project \
    PROJECT_PYTHON=/opt/project-venv/bin/python
