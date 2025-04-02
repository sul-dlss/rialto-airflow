from rialto_airflow.database import Publication
from rialto_airflow.funders import linker


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
    assert pub.funders[0].federal is True

    assert pub.funders[1].name == "Andrew W. Mellon Foundation"
    assert pub.funders[1].grid_id == "grid.453405.3"
    assert pub.funders[1].federal is False

    assert "processed 1 publications from OpenAlex" in caplog.text
    assert "found funder data in openalex for grid.431093.c" in caplog.text, (
        "funder logged"
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
    Test that hadnling a funder with no ROR ID in OpenAlex
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
    assert (
        "no ROR ID for {'funder': 'https://openalex.org/F00000', 'funder_display_name': 'A Small Foundation'}"
        in caplog.text
    )


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
        "missing GRID ID for {'funder': 'https://openalex.org/F00000', 'funder_display_name': 'A Small Foundation'}"
        in caplog.text
    )
