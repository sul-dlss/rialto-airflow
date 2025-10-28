import logging
import requests

from rialto_airflow.schema.harvest import Publication, Funder
from rialto_airflow.funders import linker
from test.utils import num_log_record_matches


def test_link_publications(test_session, snapshot):
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={
                    "funders": [
                        {
                            "id": "grid.419635.c",
                            "name": "National Institute of Diabetes and Digestive and Kidney Diseases",
                        }
                    ]
                },
                openalex_json={
                    "grants": [
                        {
                            "funder": "https://openalex.org/F4320306076",
                            "funder_display_name": "National Science Foundation",
                        }
                    ]
                },
            )
        )
    assert linker.link_publications(snapshot) == 2


def test_dimensions_funders_linking(test_session, snapshot, caplog):
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={
                    "funders": [
                        {
                            "id": "grid.419635.c",
                            "name": "National Institute of Diabetes and Digestive and Kidney Diseases",
                        },
                        {
                            "id": "grid.453405.3",
                            "name": "Andrew W. Mellon Foundation",
                        },
                        {"name": "Organization A"},
                    ]
                },
            )
        )

    linker.link_dim_publications(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 2

    assert (
        pub.funders[0].name
        == "National Institute of Diabetes and Digestive and Kidney Diseases"
    )

    assert pub.funders[0].grid_id == "grid.419635.c"
    assert pub.funders[0].federal is True

    assert pub.funders[1].name == "Andrew W. Mellon Foundation"
    assert pub.funders[1].grid_id == "grid.453405.3"
    assert pub.funders[1].federal is False

    assert "missing GRID ID in {'name': 'Organization A'}" in caplog.text


def test_funders_is_none(test_session, snapshot):
    with test_session.begin() as session:
        session.add(
            Publication(doi="10.1515/9781503624153", dim_json={"funders": None})
        )

    linker.link_dim_publications(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 0


def test_openalex_funders_linking(test_session, snapshot, caplog):
    caplog.set_level(logging.DEBUG)
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={"funders": None},
                openalex_json={
                    "grants": [
                        {
                            "funder": "https://openalex.org/F4320306076",
                            "funder_display_name": "National Science Foundation",
                        },
                        {
                            "funder": "https://openalex.org/F4320306146",
                            "funder_display_name": "Andrew W. Mellon Foundation",
                        },
                    ]
                },
            )
        )

    linker.link_openalex_publications(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 2, "two funders added"

    assert pub.funders[0].name == "National Science Foundation"
    assert pub.funders[0].grid_id == "grid.431093.c"
    assert pub.funders[0].ror_id == "https://ror.org/021nxhr62"
    assert pub.funders[0].openalex_id == "https://openalex.org/F4320306076"
    assert pub.funders[0].federal is True

    assert pub.funders[1].name == "Andrew W. Mellon Foundation"
    assert pub.funders[1].grid_id == "grid.453405.3"
    assert pub.funders[1].ror_id == "https://ror.org/04jsh2530"
    assert pub.funders[1].openalex_id == "https://openalex.org/F4320306146"
    assert pub.funders[1].federal is False

    assert (
        num_log_record_matches(
            caplog.records, logging.DEBUG, "processed 1 publications from OpenAlex"
        )
        == 1
    )
    assert (
        num_log_record_matches(
            caplog.records,
            logging.DEBUG,
            "found funder data in openalex for https://openalex.org/F4320306076",
        )
        == 1
    )


def test_openalex_funders_is_none(test_session, snapshot):
    with test_session.begin() as session:
        session.add(
            Publication(doi="10.1515/9781503624153", openalex_json={"grants": []})
        )

    linker.link_openalex_publications(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 0


def test_openalex_no_ror(test_session, snapshot, caplog, monkeypatch):
    """
    Test handling a funder with no ROR ID in OpenAlex
    """

    # mock Funder to not have a ROR
    def mock_get(*args, **kwargs):
        return {
            "https://openalex.org/F00000": {
                "id": "https://openalex.org/F00000",
                "display_name": "A Small Foundation",
                "ror": None,
                "ids": {
                    "ror": None,
                },
            }
        }

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={"funders": None},
                openalex_json={
                    "grants": [
                        {
                            "funder": "https://openalex.org/F00000",
                            "funder_display_name": "A Small Foundation",
                        },
                    ]
                },
            )
        )

    linker.link_openalex_publications(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 0, "no funder added"
    assert "no ROR ID for https://openalex.org/F00000" in caplog.text


def test_no_ror_in_mapping(test_session, snapshot, caplog, monkeypatch):
    # mock Funder to have an unknown ROR
    def mock_get(*args, **kwargs):
        return {
            "https://openalex.org/F00000": {
                "id": "https://openalex.org/F00000",
                "display_name": "A Small Foundation",
                "ids": {
                    "ror": "https://ror.org/0000000",
                },
            }
        }

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={"funders": None},
                openalex_json={
                    "grants": [
                        {
                            "funder": "https://openalex.org/F00000",
                            "funder_display_name": "A Small Foundation",
                        },
                    ]
                },
            )
        )

    linker.link_openalex_publications(snapshot)
    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 0, "no funder added"
    assert (
        "no GRID ID could be determined for https://openalex.org/F00000" in caplog.text
    )


def test_openalex_http_exception(test_session, snapshot, caplog, monkeypatch):
    # mock Funder to have an unknown ROR
    def mock_get(*args, **kwargs):
        raise requests.exceptions.HTTPError()

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={"funders": None},
                openalex_json={
                    "grants": [
                        {
                            "funder": "https://openalex.org/F00000",
                            "funder_display_name": "A Small Foundation",
                        },
                    ]
                },
            )
        )

    linker.link_openalex_publications(snapshot)
    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 0, "no funder added"
    assert "OpenAlex API returned error for https://" in caplog.text


def test_skip_openalex_lookup(test_session, snapshot, monkeypatch):
    """
    When there is a Funder already in the database with a given openalex_id it
    is fetched from the database rather than from OpenAlex API.
    """

    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    dim_json={
                        "funders": [
                            {
                                "id": "grid.419635.c",
                                "name": "National Institute of Diabetes and Digestive and Kidney Diseases",
                            }
                        ]
                    },
                    openalex_json={
                        "grants": [
                            {
                                "funder": "https://openalex.org/F4320306076",
                                "funder_display_name": "National Science Foundation",
                            }
                        ]
                    },
                ),
                Funder(
                    name="National Science Foundation",
                    openalex_id="https://openalex.org/F4320306076",
                    ror_id="https://ror.org/021nxhr62",
                ),
            ]
        )

    def explode(*args, **kwargs):
        raise Exception("💥")

    monkeypatch.setattr(linker, "Funders", explode)

    # this will raise an exception if
    assert linker.link_publications(snapshot) == 2
