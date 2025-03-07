from rialto_airflow.database import Publication
from rialto_airflow.harvest.distill import distill
from rialto_airflow.snapshot import Snapshot

# setup some JSON data that looks like what we get from the respective APIs

sulpub_json = {"title": "On the dangers of stochastic parrots (sulpub)", "year": "2020"}

dim_json = {"title": "On the dangers of stochastic parrots (dim)", "year": 2021}

openalex_json = {
    "title": "On the dangers of stochastic parrots (openalex)",
    "publication_year": 2022,
}

wos_json = {
    "static_data": {
        "summary": {
            "pub_info": {"pubyear": 2023},
            "titles": {
                "count": 6,
                "title": [
                    {
                        "type": "source",
                        "content": "FAccT '21: Proceedings of the 2021 ACM Conference on Fairness, Accountability, and Transparency",
                    },
                    {"type": "source_abbrev", "content": "FAACT"},
                    {"type": "abbrev_iso", "content": "FAccT J."},
                    {
                        "type": "item",
                        "content": "On the dangers of stochastic parrots (wos)",
                    },
                ],
            },
        }
    }
}


# test the title preferences


def test_title_sulpub(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json,
                dim_json=dim_json,
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert pub.title == "On the dangers of stochastic parrots (sulpub)"


def test_title_dim(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json=dim_json,
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )
    assert pub.title == "On the dangers of stochastic parrots (dim)"


def test_title_openalex(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )
    assert pub.title == "On the dangers of stochastic parrots (openalex)"


def test_title_wos(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )
    assert pub.title == "On the dangers of stochastic parrots (wos)"


def test_title_none(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(Publication(doi="10.1515/9781503624153"))

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )
    assert pub.title is None


# test the pub_year preferences


def test_pub_year_sulpub(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json,
                dim_json=dim_json,
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert pub.pub_year == 2020


def test_pub_year_dim(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json=dim_json,
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert pub.pub_year == 2021


def test_pub_year_openalex(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert pub.pub_year == 2022


def test_pub_year_wos(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert pub.pub_year == 2023


def test_pub_year_none(test_session, tmp_path):
    snapshot = Snapshot(tmp_path, "rialto_test")

    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
            )
        )

    distill(snapshot)

    pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )

    assert pub.pub_year is None
