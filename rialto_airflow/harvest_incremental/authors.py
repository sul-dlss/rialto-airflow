import csv
import logging

from psycopg2 import Error as Psycopg2Error
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from rialto_airflow.database import get_session
from rialto_airflow.schema.rialto import RIALTO_DB_NAME, Author
from rialto_airflow.utils import rialto_authors_file


def load_authors_table(data_dir) -> None:
    """
    Load the authors data from the authors CSV into the database
    """
    authors_file = rialto_authors_file(data_dir)
    check_headers(authors_file)

    session_maker = get_session(RIALTO_DB_NAME)

    # we need to control commit and rollback directly in order to handle
    # the different constraint violations that can be encountered
    session = session_maker()

    logging.debug(f"Loading authors from {authors_file} into database {RIALTO_DB_NAME}")

    new_authors = updated_authors = processed_authors = ignored_authors = 0

    with open(authors_file, "r") as file:
        for row in csv.DictReader(file):
            processed_authors += 1
            row_dict = row_to_dict(row)
            try:
                match upsert_author(session, row_dict):
                    case "inserted":
                        new_authors += 1
                    case "updated":
                        updated_authors += 1
                    case "noop":
                        ignored_authors += 1

            except IntegrityError as e:
                assert isinstance(e.orig, Psycopg2Error)  # needed for type checking
                session.rollback()
                constraint = e.orig.diag.constraint_name

                if constraint == "author_orcid_key":
                    ignored_authors += 1
                    logging.warning(
                        f"Ignored author sunet={row_dict['sunet']} because there's another author with ORCID {row_dict['orcid']}"
                    )

                elif constraint == "author_cap_profile_id_key":
                    result = handle_duplicate_cap_profile_id(session, row, row_dict)
                    if result is True:
                        updated_authors += 1
                    else:
                        ignored_authors += 1

        logging.info(
            f"processed={processed_authors} new={new_authors} updated={updated_authors} ignored={ignored_authors}"
        )


def row_to_dict(row: dict) -> dict:
    return dict(
        sunet=row["sunetid"],
        cap_profile_id=row["cap_profile_id"] or None,
        orcid=row["orcidid"] or None,
        first_name=row["first_name"],
        last_name=row["last_name"],
        status=to_boolean(row["active"]),
        academic_council=to_boolean(row["academic_council"]),
        role=row["role"],
        schools=to_array(row["all_schools"]),
        departments=to_array(row["all_departments"]),
        primary_school=row["primary_school"],
        primary_dept=row["primary_department"],
        primary_division=row["primary_division"],
    )


def upsert_author(session: Session, row_dict: dict) -> str:
    """
    Insert author, or update if the sunet already exists and values have changed.
    If there is a constraint violation related to the orcid or cap_profile_id an
    IntegrityError exception will be raised.

    Returns "inserted", "updated", or "noop" depending on what it did.
    """
    author = session.query(Author).where(Author.sunet == row_dict["sunet"]).first()

    if author is None:
        session.add(Author(**row_dict))
        session.commit()
        return "inserted"

    # if no values are changing on the author no update is needed
    # this prevents changing updated_at when there are no changes
    if not any(getattr(author, col) != row_dict[col] for col in row_dict):
        return "noop"

    # update the author using the new data, and commit
    for col, val in row_dict.items():
        setattr(author, col, val)
    session.add(author)
    session.commit()
    return "updated"


def handle_duplicate_cap_profile_id(
    session: Session, row: dict, row_dict: dict
) -> bool:
    """
    Handle a cap_profile_id uniqueness conflict and return the number of errors
    encountered.

    If the author being added is inactive it is ignored.

    If the author being added is active, and there is an inactive author with the same
    cap_profile_id in the database already, the inactive author will be replaced
    with the active one.

    True will be returned if an author was updated, otherwise False.
    """

    cap_profile_id = row["cap_profile_id"]

    if to_boolean(row["active"]) is False:
        logging.warning(
            f"Ignored inactive author sunet={row_dict['sunet']} with pre-existing cap_profile_id {cap_profile_id} and a different sunet"
        )
        return False

    inactive_author = (
        session.query(Author)
        .where(Author.cap_profile_id == cap_profile_id)
        .where(Author.status.is_(False))
        .first()
    )

    if inactive_author is None:
        logging.warning(
            f"Unable to insert active author when there is already an active author with cap_profile_id {cap_profile_id}"
        )
        return False

    old_sunet = inactive_author.sunet

    session.execute(
        update(Author).where(Author.id == inactive_author.id).values(row_dict)
    )
    session.commit()

    logging.info(
        f"Updated inactive author sunet={old_sunet} with active author sunet={row_dict['sunet']} for cap_profile_id={cap_profile_id}"
    )

    return True


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
        logging.debug(f"Headers in {authors_file}: {headers}")


def to_boolean(value: str) -> bool:
    bool_map = {"true": True, "false": False}
    return bool_map[value.strip().lower()]


def to_array(value: str) -> list:
    return value.split("|") if value else []
