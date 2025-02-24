import csv
import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from rialto_airflow.database import get_engine, Author
from rialto_airflow.utils import rialto_authors_file


def load_authors_table(snapshot) -> str:
    """
    Load the authors data from the authors CSV into the database
    """
    engine = get_engine(snapshot.database_name)
    Session = sessionmaker(engine)

    authors_file = rialto_authors_file(snapshot.path)
    check_headers(authors_file)

    logging.info(
        f"Loading authors from {authors_file} into database {snapshot.database_name}"
    )
    with open(authors_file, "r") as file:
        errors = []
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            try:
                with Session.begin() as session:
                    author = Author(
                        sunet=row["sunetid"],
                        cap_profile_id=row["cap_profile_id"],
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
            except IntegrityError as e:
                message = f"Skipping author: {row['sunetid'], row['orcidid']}: {e}"
                logging.warning(message)
                errors.append(message)
                continue

    logging.info(f"Loaded {session.query(Author).count()} authors")
    if errors:
        # show all errors at the end of the log
        logging.warning(f"Errors: {errors}")


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
    return bool_map[value.strip().lower()]


def to_array(value: str) -> list:
    return value.split("|") if value else []
