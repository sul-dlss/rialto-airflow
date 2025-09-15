import logging

from rialto_airflow.schema.reports import RIALTO_REPORTS_DB_NAME
from rialto_airflow.database import (
    create_database,
    database_exists,
)


PERMANENT_DATABASES: list[str] = [RIALTO_REPORTS_DB_NAME]


def init_permanent_databases() -> None:
    """
    For any that don't yet exist, create the databases that will live indefinitely between
    DAG runs (e.g. the reporting database that Tableau connects to, which needs a stable name to
    reference; but not the harvest databases).
    """
    for db_name in PERMANENT_DATABASES:
        if not database_exists(db_name):
            create_database(db_name)
            logging.info(f"created database {db_name}")
        else:
            logging.info(f"found database {db_name}")


if __name__ == "__main__":
    init_permanent_databases()
