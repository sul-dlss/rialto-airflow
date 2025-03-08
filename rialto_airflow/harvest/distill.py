import logging

from sqlalchemy import Integer, case, func, update
from sqlalchemy.sql.elements import Case

from rialto_airflow.database import Publication, get_session
from rialto_airflow.snapshot import Snapshot


def distill(snapshot: Snapshot) -> None:
    _update_cols(snapshot.database_name)


def _update_cols(db_name) -> None:
    logging.info("starting distill update cols")

    with get_session(db_name).begin() as update_session:
        update_session.execute(
            update(Publication).values(  # type: ignore
                title=_title(),
                pub_year=_pub_year(),
                open_access=_open_access(),
            )
        )
    logging.info("finished distill update cols")


def _title() -> Case:
    # title preference: sulpub, dimensions, openalex, wos

    return case(  # type: ignore
        (
            Publication.sulpub_json["title"].isnot(None),
            Publication.sulpub_json["title"].astext,
        ),
        (
            Publication.dim_json["title"].isnot(None),
            Publication.dim_json["title"].astext,
        ),
        (
            Publication.openalex_json["title"].isnot(None),
            Publication.openalex_json["title"].astext,
        ),
        (
            func.jsonb_path_exists(
                Publication.wos_json,
                '$.static_data.summary.titles[0].title ? (@.type == "item")',
            ),
            func.jsonb_extract_path_text(
                func.jsonb_path_query_first(
                    Publication.wos_json,
                    '$.static_data.summary.titles[0].title ? (@.type == "item")',
                ),
                "content",
            ),
        ),
        else_=None,
    )


def _pub_year() -> Case:
    # pub_year preference: sulpub, dimensions, openalex, wos

    return case(  # type: ignore
        (
            Publication.sulpub_json["year"].isnot(None),
            Publication.sulpub_json["year"].astext.cast(Integer),
        ),
        (
            Publication.dim_json["year"].isnot(None),
            Publication.dim_json["year"].astext.cast(Integer),
        ),
        (
            Publication.openalex_json["publication_year"].isnot(None),
            Publication.openalex_json["publication_year"].astext.cast(Integer),
        ),
        (
            func.jsonb_path_exists(
                Publication.wos_json,
                "$.static_data.summary.pub_info.pubyear",
            ),
            func.jsonb_extract_path_text(
                func.jsonb_path_query_first(
                    Publication.wos_json, "$.static_data.summary.pub_info"
                ),
                "pubyear",
            ).cast(Integer),
        ),
        else_=None,
    )


def _open_access():
    return case(  # type: ignore
        (
            func.jsonb_path_exists(
                Publication.openalex_json, "$.open_access.oa_status"
            ),
            Publication.openalex_json["open_access"]["oa_status"].astext,
        ),
        (
            Publication.dim_json["open_access"].is_not(None),
            func.jsonb_path_query_first(
                Publication.dim_json,
                '$.open_access[*] ? (@ != "oa_all")',  # filter out oa_all
            ).op("#>>")("{}"),
            # the op here turns the JSONB value into text, since astext doesn't work
            # https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-PROCESSING
        ),
        else_=None,
    )


# type

# TODO: We need to distill other values but this will involve additional work to
# get appropriate data:
#
# - apc
# - funders
# - federally funded
#
# See: https://docs.google.com/document/d/1WojtunkzNtidF2JW4ZLcSClbxajMkH36eUdRIl9tk0E/edit?tab=t.0#heading=h.8lc1fr3onylw
