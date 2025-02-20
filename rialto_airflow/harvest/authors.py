import csv
import logging
import os

from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rialto_airflow.database import Author
from rialto_airflow.utils import rialto_authors_file


db_name = Variable.get("rialto_db_name")
data_dir = Variable.get("data_dir")

# an Engine, which the Session will use for connection resources
engine = create_engine(
    f"{os.environ.get('AIRFLOW_VAR_RIALTO_POSTGRES')}/{db_name}", echo=True
)

# a sessionmaker(), also in the same scope as the engine
Session = sessionmaker(engine)


def load_authors_table() -> str:
    """
    Load the authors data from the authors CSV into the database
    """
    authors_file = rialto_authors_file(data_dir)
    check_headers(authors_file)

    logging.info(f"Loading authors from {authors_file} into database {db_name}")
    with Session.begin() as session:
        with open(authors_file, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                author = Author(
                    sunet=row["sunetid"],
                    orcid=row["orcidid"] or None,
                    first_name=row["first_name"],
                    last_name=row["last_name"],
                    status=to_boolean(row["active"]),
                    academic_council=to_boolean(row["academic_council"]),
                    primary_role=row["role"],
                    schools=to_array(row["all_schools"]),
                    departments=to_array(row["all_departments"]),
                    primary_school=row["primary_school"],
                    primary_dept=row["primary_department"],
                    primary_division=row["primary_division"],
                )
                session.add(author)
        logging.info(f"Loaded {session.query(Author).count()} authors")
        # commits the transaction, closes the session

    return authors_file


def check_headers(authors_file: str) -> None:
    with open(authors_file, "r") as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        required_headers = [
            "sunetid",
            "first_name",
            "last_name",
            "orcidid",
            "role",
            "academic_council",
            "primary_school",
            "primary_department",
            "primary_division",
            "all_schools",
            "all_departments",
            "active",
        ]
        if not set(required_headers).issubset(set(headers)):
            raise ValueError(
                f"Headers in {authors_file} are {headers}, expected to include {required_headers}"
            )
        logging.info(f"Headers in {authors_file}: {headers}")


def to_boolean(value: str) -> bool:
    bool_map = {"true": True, "false": False}

    # will raise KeyError if unexpected value
    return bool_map.get(value.strip().lower())


def to_array(value: str) -> list:
    return value.split("|") if value else []
