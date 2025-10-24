import logging
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import get_session
from rialto_airflow.schema.harvest import Publication, pub_author_association
from rialto_airflow.snapshot import Snapshot


def remove_duplicates(snapshot: Snapshot) -> int:
    """
    Remove duplicate publications from the database.
    Returns the number of duplicates found (not the number of rows deleted).
    """
    logging.debug("Removing duplicate publications from each source.")
    wos_dupes = remove_wos_duplicates(snapshot)
    openalex_dupes = remove_openalex_duplicates(snapshot)
    total_deleted = wos_dupes + openalex_dupes
    logging.info(
        f"Deleted a total of {total_deleted} duplicate publication rows from all sources."
    )
    return total_deleted


def remove_wos_duplicates(snapshot: Snapshot) -> int:
    logging.debug("Removing any duplicate WOS publications.")
    with get_session(snapshot.database_name).begin() as session:
        # Find all duplicate WOS publications in the snapshot
        duplicates = session.execute(
            select(func.count(), Publication.wos_json["UID"])
            .where(Publication.doi.is_(None))
            .where(Publication.wos_json["UID"].is_not(None))
            .group_by(Publication.wos_json["UID"])
            .having(func.count() > 1)
        ).all()
        num_dupes = len(duplicates)
        logging.info(f"Found {num_dupes} WOS publications with duplicates.")
        count_deleted = 0
        wos_uids = [row[1] for row in duplicates]
        for wos_uid in wos_uids:
            pubs = (
                session.execute(
                    select(Publication).where(
                        Publication.wos_json["UID"].astext == wos_uid
                    )
                )
                .scalars()
                .all()
            )
            deleted = merge_pubs(pubs=pubs, session=session)
            count_deleted += deleted
        logging.info(f"Deleted {count_deleted} publication rows from WOS.")
    return num_dupes


def remove_openalex_duplicates(snapshot: Snapshot) -> int:
    logging.debug("Removing any duplicate OpenAlex publications.")
    with get_session(snapshot.database_name).begin() as session:
        # Find all duplicate OpenAlex publications in the snapshot
        duplicates = session.execute(
            select(func.count(), Publication.openalex_json["id"])
            .where(Publication.doi.is_(None))
            .where(Publication.openalex_json["id"].is_not(None))
            .group_by(Publication.openalex_json["id"])
            .having(func.count() > 1)
        ).all()
        num_dupes = len(duplicates)
        logging.info(f"Found {num_dupes} OpenAlex publications with duplicates.")
        count_deleted = 0
        openalex_ids = [row[1] for row in duplicates]
        for openalex_id in openalex_ids:
            pubs = (
                session.execute(
                    select(Publication).where(
                        Publication.openalex_json["id"].astext == openalex_id
                    )
                )
                .scalars()
                .all()
            )
            deleted = merge_pubs(pubs=pubs, session=session)
            count_deleted += deleted
        logging.info(f"Deleted {count_deleted} publication rows from OpenAlex.")
    return num_dupes


def merge_pubs(*, pubs, session) -> int:
    """
    Given a set of publications that are duplicates, merge them into one,
    moving author relationships to the first instance and deleting the duplicates.
    Returns the number of deleted publications.
    """
    count_deleted = 0
    main_pub = pubs[0].id
    for pub in pubs[1:]:
        # Move author relationships to the first instance
        for author in pub.authors:
            session.execute(
                insert(pub_author_association)
                .values(publication_id=main_pub, author_id=author.id)
                .on_conflict_do_nothing()
            )

        # Delete the duplicate
        session.execute(delete(Publication).where(Publication.id == pub.id))  # type: ignore
        count_deleted += 1
    return count_deleted
