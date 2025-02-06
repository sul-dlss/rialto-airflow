FROM apache/airflow:2.10.4-python3.12

USER root
RUN apt-get update && apt-get install -y gcc git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

USER airflow

COPY rialto_airflow ./rialto_airflow

COPY requirements.txt .

RUN uv pip install --no-cache "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt
