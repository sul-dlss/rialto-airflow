import logging

import pytest
import requests

from rialto_airflow.funders import linker
from rialto_airflow.schema.harvest import Funder as HarvestFunder
from rialto_airflow.schema.harvest import Publication as HarvestPublication
from rialto_airflow.schema.rialto import Funder as RialtoFunder
from rialto_airflow.schema.rialto import Publication as RialtoPublication
from test.utils import num_log_record_matches


RIALTO_TEST_DB_NAME = "rialto_test"
RIALTO_INCREMENTAL_TEST_DB_NAME = "rialto_incremental_test"


@pytest.fixture
def harvest_database_name():
    return RIALTO_TEST_DB_NAME


@pytest.fixture
def incremental_database_name():
    return RIALTO_INCREMENTAL_TEST_DB_NAME


def test_link_publications_harvest(test_session, harvest_database_name):
    with test_session.begin() as session:
        session.add(
            HarvestPublication(
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

    assert linker.link_publications(harvest_database_name) == 2


def test_link_publications_incremental(
    test_incremental_session, incremental_database_name
):
    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(
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

    assert linker.link_publications(incremental_database_name) == 2


def test_dimensions_funders_linking_harvest(
    test_session, harvest_database_name, caplog
):
    with test_session.begin() as session:
        session.add(
            HarvestPublication(
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

    linker.link_dim_publications(harvest_database_name)

    with test_session.begin() as session:
        pub = (
            session.query(HarvestPublication)
            .where(HarvestPublication.doi == "10.1515/9781503624153")
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


def test_dimensions_funders_linking_incremental(
    test_incremental_session, incremental_database_name, caplog
):
    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(
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

    linker.link_dim_publications(incremental_database_name)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(RialtoPublication)
            .where(RialtoPublication.doi == "10.1515/9781503624153")
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


def test_funders_is_none_harvest(test_session, harvest_database_name):
    with test_session.begin() as session:
        session.add(
            HarvestPublication(doi="10.1515/9781503624153", dim_json={"funders": None})
        )

    linker.link_dim_publications(harvest_database_name)

    with test_session.begin() as session:
        pub = (
            session.query(HarvestPublication)
            .where(HarvestPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0


def test_funders_is_none_incremental(
    test_incremental_session, incremental_database_name
):
    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(doi="10.1515/9781503624153", dim_json={"funders": None})
        )

    linker.link_dim_publications(incremental_database_name)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(RialtoPublication)
            .where(RialtoPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0


def test_openalex_funders_linking_harvest(test_session, harvest_database_name, caplog):
    caplog.set_level(logging.DEBUG)

    with test_session.begin() as session:
        session.add(
            HarvestPublication(
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

    linker.link_openalex_publications(harvest_database_name)

    with test_session.begin() as session:
        pub = (
            session.query(HarvestPublication)
            .where(HarvestPublication.doi == "10.1515/9781503624153")
            .first()
        )

        assert len(pub.funders) == 2
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


def test_openalex_funders_linking_incremental(
    test_incremental_session, incremental_database_name, caplog
):
    caplog.set_level(logging.DEBUG)

    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(
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

    linker.link_openalex_publications(incremental_database_name)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(RialtoPublication)
            .where(RialtoPublication.doi == "10.1515/9781503624153")
            .first()
        )

        assert len(pub.funders) == 2
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


def test_openalex_funders_is_none_harvest(test_session, harvest_database_name):
    with test_session.begin() as session:
        session.add(
            HarvestPublication(
                doi="10.1515/9781503624153", openalex_json={"grants": []}
            )
        )

    linker.link_openalex_publications(harvest_database_name)

    with test_session.begin() as session:
        pub = (
            session.query(HarvestPublication)
            .where(HarvestPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0


def test_openalex_funders_is_none_incremental(
    test_incremental_session, incremental_database_name
):
    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(doi="10.1515/9781503624153", openalex_json={"grants": []})
        )

    linker.link_openalex_publications(incremental_database_name)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(RialtoPublication)
            .where(RialtoPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0


def test_openalex_no_ror_harvest(
    test_session, harvest_database_name, caplog, monkeypatch
):
    def mock_get(*args, **kwargs):
        return {
            "https://openalex.org/F00000": {
                "id": "https://openalex.org/F00000",
                "display_name": "A Small Foundation",
                "ror": None,
                "ids": {"ror": None},
            }
        }

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_session.begin() as session:
        session.add(
            HarvestPublication(
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

    linker.link_openalex_publications(harvest_database_name)

    with test_session.begin() as session:
        pub = (
            session.query(HarvestPublication)
            .where(HarvestPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0
    assert "no ROR ID for https://openalex.org/F00000" in caplog.text


def test_openalex_no_ror_incremental(
    test_incremental_session, incremental_database_name, caplog, monkeypatch
):
    def mock_get(*args, **kwargs):
        return {
            "https://openalex.org/F00000": {
                "id": "https://openalex.org/F00000",
                "display_name": "A Small Foundation",
                "ror": None,
                "ids": {"ror": None},
            }
        }

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(
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

    linker.link_openalex_publications(incremental_database_name)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(RialtoPublication)
            .where(RialtoPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0
    assert "no ROR ID for https://openalex.org/F00000" in caplog.text


def test_no_ror_in_mapping_harvest(
    test_session, harvest_database_name, caplog, monkeypatch
):
    def mock_get(*args, **kwargs):
        return {
            "https://openalex.org/F00000": {
                "id": "https://openalex.org/F00000",
                "display_name": "A Small Foundation",
                "ids": {"ror": "https://ror.org/0000000"},
            }
        }

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_session.begin() as session:
        session.add(
            HarvestPublication(
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

    linker.link_openalex_publications(harvest_database_name)

    with test_session.begin() as session:
        pub = (
            session.query(HarvestPublication)
            .where(HarvestPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0
    assert (
        "no GRID ID could be determined for https://openalex.org/F00000" in caplog.text
    )


def test_no_ror_in_mapping_incremental(
    test_incremental_session, incremental_database_name, caplog, monkeypatch
):
    def mock_get(*args, **kwargs):
        return {
            "https://openalex.org/F00000": {
                "id": "https://openalex.org/F00000",
                "display_name": "A Small Foundation",
                "ids": {"ror": "https://ror.org/0000000"},
            }
        }

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(
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

    linker.link_openalex_publications(incremental_database_name)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(RialtoPublication)
            .where(RialtoPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0
    assert (
        "no GRID ID could be determined for https://openalex.org/F00000" in caplog.text
    )


def test_openalex_http_exception_harvest(
    test_session, harvest_database_name, caplog, monkeypatch
):
    def mock_get(*args, **kwargs):
        raise requests.exceptions.HTTPError()

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_session.begin() as session:
        session.add(
            HarvestPublication(
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

    linker.link_openalex_publications(harvest_database_name)

    with test_session.begin() as session:
        pub = (
            session.query(HarvestPublication)
            .where(HarvestPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0
    assert "OpenAlex API returned error for https://" in caplog.text


def test_openalex_http_exception_incremental(
    test_incremental_session, incremental_database_name, caplog, monkeypatch
):
    def mock_get(*args, **kwargs):
        raise requests.exceptions.HTTPError()

    monkeypatch.setattr(linker, "Funders", mock_get)

    with test_incremental_session.begin() as session:
        session.add(
            RialtoPublication(
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

    linker.link_openalex_publications(incremental_database_name)

    with test_incremental_session.begin() as session:
        pub = (
            session.query(RialtoPublication)
            .where(RialtoPublication.doi == "10.1515/9781503624153")
            .first()
        )
        assert len(pub.funders) == 0
    assert "OpenAlex API returned error for https://" in caplog.text


def test_skip_openalex_lookup_harvest(test_session, harvest_database_name, monkeypatch):
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                HarvestPublication(
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
                HarvestFunder(
                    name="National Science Foundation",
                    openalex_id="https://openalex.org/F4320306076",
                    ror_id="https://ror.org/021nxhr62",
                ),
            ]
        )

    def explode(*args, **kwargs):
        raise Exception("boom")

    monkeypatch.setattr(linker, "Funders", explode)
    assert linker.link_publications(harvest_database_name) == 2


def test_skip_openalex_lookup_incremental(
    test_incremental_session, incremental_database_name, monkeypatch
):
    with test_incremental_session.begin() as session:
        session.bulk_save_objects(
            [
                RialtoPublication(
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
                RialtoFunder(
                    name="National Science Foundation",
                    openalex_id="https://openalex.org/F4320306076",
                    ror_id="https://ror.org/021nxhr62",
                ),
            ]
        )

    def explode(*args, **kwargs):
        raise Exception("boom")

    monkeypatch.setattr(linker, "Funders", explode)
    assert linker.link_publications(incremental_database_name) == 2
