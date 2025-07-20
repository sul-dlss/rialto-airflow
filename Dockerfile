FROM apache/airflow:2.10.4-python3.12

USER root

RUN apt-get update

RUN sudo curl -sL https://deb.nodesource.com/setup_24.x | bash -

RUN apt-get install -y gcc git libpq-dev nodejs

RUN npm install -g yarn

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

USER airflow

COPY rialto_airflow ./rialto_airflow
COPY README.md uv.lock pyproject.toml .

# For the Airflow application to be able to find dependencies they need to be
# installed for the airflow user in $VIRTUAL_ENV /home/airflow/.local 
# However `uv sync` will put them in /opt/airflow/.venv
#
# By building the wheels and then installing them with `uv pip install` we will
# ensure that uv gets the dependencies (from uv.lock) and puts them in
# /home/airflow/.local

RUN uv build
RUN uv pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" dist/*.whl
