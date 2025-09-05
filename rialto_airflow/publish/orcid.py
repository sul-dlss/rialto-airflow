import csv
import logging

from sqlalchemy.dialects.postgresql import insert

from rialto_airflow.database import RIALTO_REPORTS_DB_NAME, get_session
from rialto_airflow.publish.reports_database import AuthorOrcids, OrcidIntegrationStats  # type: ignore
from rialto_airflow.mais import current_orcid_users, get_orcid_stats
from rialto_airflow.utils import rialto_active_authors_file


def export_author_orcids(data_dir):
    """
    Write the relevant authors data to the table
    """
    # insert rows from the authors CSV into the author_orcids table
    authors_file = rialto_active_authors_file(data_dir)
    logging.info(f"Loading authors from {authors_file} into the author_orcids table")
    with open(authors_file, "r") as file:
        csv_reader = csv.DictReader(file)
        with get_session(RIALTO_REPORTS_DB_NAME).begin() as insert_session:
            insert_session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"}
            )
            insert_session.connection().execute(
                f"TRUNCATE {AuthorOrcids.__tablename__}"
            )

            for row in csv_reader:
                row_values = {
                    "sunetid": row["sunetid"],
                    "full_name": row["full_name"],
                    "orcidid": row["orcidid"],
                    "orcid_update_scope": row["orcid_update_scope"].lower() == "true",
                    "role": row["role"],
                    "primary_affiliation": row["primary_affiliation"],
                    "primary_school": row["primary_school"],
                    "primary_department": row["primary_department"],
                    "primary_division": row["primary_division"],
                }
                insert_session.execute(
                    insert(AuthorOrcids).values(**row_values).on_conflict_do_nothing()
                )

        logging.info("finished writing author_orcids table")
    return True


def export_orcid_integration_stats(
    mais_client_id, mais_client_secret, mais_token_url, mais_base_url
):
    """
    Get current ORCID integration stats from the MAIS ORCID integration API and write to orcid_integrations reports table.
    """
    current_users = current_orcid_users(
        mais_client_id, mais_client_secret, mais_token_url, mais_base_url
    )
    orcid_stats = get_orcid_stats(current_users)
    # TODO: How to handle historic data? Will do as a migration once available
    with get_session(RIALTO_REPORTS_DB_NAME).begin() as insert_session:
        insert_session.connection(execution_options={"isolation_level": "SERIALIZABLE"})

        print(f"ORCID STATS ARE: {orcid_stats}")
        row_values = {
            "date_label": orcid_stats[0],
            "read_only_scope": orcid_stats[1],
            "read_write_scope": orcid_stats[2],
        }
        insert_session.execute(insert(OrcidIntegrationStats).values(**row_values))
    return orcid_stats
