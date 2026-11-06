import csv
import logging

from psycopg2 import Error as Psycopg2Error
from sqlalchemy import CursorResult, delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from rialto_airflow.database import get_session
from rialto_airflow.schema.rialto import (
    RIALTO_DB_NAME,
    Author,
    Harvest,
    pub_author_association,
)
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

    # rows that could not be loaded; these are reported together at the end so
    # that one bad row doesn't prevent the rest of the file from loading
    failures: list[str] = []

    with open(authors_file, "r") as file:
        # line 1 is the header
        for line_num, row in enumerate(csv.DictReader(file), start=2):
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
                session.rollback()
                constraint = _constraint_name(e)

                if constraint == "author_orcid_key":
                    ignored_authors += 1
                    logging.warning(
                        f"Ignored author sunet={row_dict['sunet']} because there's another author with ORCID {row_dict['orcid']}"
                    )

                elif constraint == "author_cap_profile_id_key":
                    try:
                        handle_duplicate_cap_profile_id(session, row_dict)
                        updated_authors += 1
                    except IntegrityError as e2:
                        session.rollback()
                        failures.append(_describe_failure(line_num, row_dict, e2))

                else:
                    failures.append(_describe_failure(line_num, row_dict, e))

        logging.info(
            f"processed={processed_authors} new={new_authors} updated={updated_authors} ignored={ignored_authors} failed={len(failures)}"
        )

    if failures:
        for failure in failures:
            logging.error(failure)
        raise AuthorLoadError(
            f"{len(failures)} author row(s) in {authors_file} could not be loaded:\n"
            + "\n".join(failures)
        )


class AuthorLoadError(Exception):
    pass


def _constraint_name(e: IntegrityError) -> str | None:
    assert isinstance(e.orig, Psycopg2Error)  # needed for type checking
    return e.orig.diag.constraint_name


def _describe_failure(line_num: int, row_dict: dict, e: IntegrityError) -> str:
    assert isinstance(e.orig, Psycopg2Error)  # needed for type checking
    return (
        f"line {line_num}: sunet={row_dict['sunet']} cap_profile_id={row_dict['cap_profile_id']} "
        f"orcid={row_dict['orcid']} violates {e.orig.diag.constraint_name}: {e.orig.diag.message_detail}"
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


def handle_duplicate_cap_profile_id(session: Session, row_dict: dict) -> None:
    """
    Resolve a cap_profile_id uniqueness conflict between the incoming row and
    an existing author that already has that cap_profile_id. Overwrite the
    existing row with the incoming values, which preserve the existing
    author/publication relations.

    If the incoming sunet already belongs to a different author row, the same
    person was loaded twice under different identifiers and upstream has since
    merged them. That row is merged into the one holding the cap_profile_id.

    Everything happens in one transaction. If an IntegrityError is raised (e.g.
    the incoming ORCID belongs to a third author) nothing is changed.
    """

    cap_profile_id = row_dict["cap_profile_id"]

    existing_author = (
        session.query(Author).where(Author.cap_profile_id == cap_profile_id).one()
    )

    # save the old sunet before it gets updated, so we can log it below
    old_sunet = existing_author.sunet

    duplicate_author = (
        session.query(Author)
        .where(Author.sunet == row_dict["sunet"], Author.id != existing_author.id)
        .one_or_none()
    )
    if duplicate_author is not None:
        merge_author(session, duplicate_author, into=existing_author)

    session.execute(
        update(Author).where(Author.id == existing_author.id).values(row_dict)
    )
    session.commit()

    logging.info(
        f"Updated author sunet={old_sunet} with author sunet={row_dict['sunet']} for cap_profile_id={cap_profile_id}"
    )


def merge_author(session: Session, author: Author, into: Author) -> None:
    """
    Move the publication links from author onto into, and delete author. This
    does not commit, so that the caller can make it part of a larger transaction.
    """
    links = pub_author_association

    # save these before the author is deleted, so we can log them below
    merged = (
        f"id={author.id} sunet={author.sunet} cap_profile_id={author.cap_profile_id}"
    )

    # skip publications that are already linked to the surviving author, since
    # moving those would violate the association's primary key
    already_linked = select(links.c.publication_id).where(links.c.author_id == into.id)
    moved = session.execute(
        update(links)
        .where(links.c.author_id == author.id)
        .where(links.c.publication_id.not_in(already_linked))
        .values(author_id=into.id)
    )
    assert isinstance(moved, CursorResult)  # needed for type checking

    # any links that weren't moved are removed by the ON DELETE CASCADE
    session.execute(delete(Author).where(Author.id == author.id))

    logging.info(
        f"Merged author {merged} into author id={into.id} sunet={into.sunet} cap_profile_id={into.cap_profile_id}, "
        f"moved {moved.rowcount} publication link(s)"
    )


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


def clear_pub_author_links(harvest_id: int) -> None:
    """
    Remove links between authors and publications, which is useful when doing a
    full harvest of the metadata, and wanting to pull in corrected links.
    """
    harvest = Harvest.get_by_id(harvest_id)
    if not harvest.is_full:
        return
    with get_session(RIALTO_DB_NAME).begin() as session:
        result = session.execute(delete(pub_author_association))
        logging.info(
            f"Cleared {result.rowcount} author/publication links for full harvest."
        )
