from rialto_airflow.database import Publication
from rialto_airflow.funders.linker import link_publications


def test_funders_linking(test_session, snapshot, caplog):
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

    link_publications(snapshot)

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

    link_publications(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert len(pub.funders) == 0
