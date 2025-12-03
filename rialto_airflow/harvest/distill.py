import logging
from typing import Optional

from sqlalchemy import select, update

from rialto_airflow.database import get_session
from rialto_airflow.distiller import (
    title,
    pub_year,
    open_access,
    types,
    publisher,
    journal_name,
    apc,
)
from rialto_airflow.schema.harvest import Publication
from rialto_airflow.snapshot import Snapshot


def distill(snapshot: Snapshot) -> int:
    """
    Walk through all publications in the database and set the title, pub_year,
    open_access columns using the harvested metadata.
    """
    with get_session(snapshot.database_name).begin() as select_session:
        # iterate through publictions 100 at a time
        count = 0
        stmt = select(Publication).execution_options(yield_per=100)  # type: ignore

        for row in select_session.execute(stmt):
            count += 1
            if count % 50000 == 0:
                logging.debug(f"processed {count} publications")

            pub = row[0]

            # populate new publication columns
            cols = {
                "title": title(pub),
                "pub_year": pub_year(pub),
                "open_access": open_access(pub),
                "types": types(pub),
                "publisher": publisher(pub),
                "journal_name": journal_name(pub),
                "academic_council_authored": _academic_council(pub),
                "faculty_authored": _faculty_authored(pub),
            }

            # pub_year and open_access in cols is needed to determine the apc
            cols["apc"] = apc(pub, cols)

            # update the publication with the new columns
            with get_session(snapshot.database_name).begin() as update_session:
                update_stmt = (
                    update(Publication)  # type: ignore
                    .where(Publication.id == pub.id)
                    .values(cols)
                )
                update_session.execute(update_stmt)

    return count


# these helper functions aren't in the rialto_airflow.distiller modules because
# they simply look at the database models and not the JSON publication metadata


def _academic_council(pub) -> Optional[bool]:
    """
    Get the academic_council value for all authors and return True if any are academic_council
    """
    authors = pub.authors

    for author in authors:
        if author.academic_council:
            return True

    return False


def _faculty_authored(pub) -> Optional[bool]:
    return any(author.role == "faculty" for author in pub.authors)
