import logging
from sqlalchemy import delete, func, insert, select

from rialto_airflow.database import Publication, get_session, pub_author_association
from rialto_airflow.snapshot import Snapshot


def remove_wos_duplicates(snapshot: Snapshot) -> int:
    logging.info("Removing any duplicate publications.")
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
        logging.info(f"Found {num_dupes} publications with duplicates.")
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
            # pubs contains all Publications with this WOS UID
            # keep the first one and merge authors
            main_pub = pubs[0].id
            for pub in pubs[1:]:
                # Move author relationships to the first instance
                for author in pub.authors:
                    session.execute(
                        insert(pub_author_association).values(
                            publication_id=main_pub, author_id=author.id
                        )
                    )
                # Delete the duplicate
                session.execute(delete(Publication).where(Publication.id == pub.id))  # type: ignore
                count_deleted += 1
        logging.info(f"Deleted a total of {count_deleted} publication rows.")
    return num_dupes
