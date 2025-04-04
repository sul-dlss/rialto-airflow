from rialto_airflow.database import Publication
from rialto_airflow.harvest.distill import distill

# Set up JSON data that mirrors (in part) what we get from the respective APIs

sulpub_json = {"title": "On the dangers of stochastic parrots (sulpub)", "year": "2020"}

sulpub_json_future_year = {
    "title": "On the dangers of stochastic parrots (sulpub)",
    "year": "2105",
}

dim_json = {
    "title": "On the dangers of stochastic parrots (dim)",
    "year": 2021,
    "open_access": ["oa_all", "green"],
}

openalex_json = {
    "title": "On the dangers of stochastic parrots (openalex)",
    "publication_year": 2022,
    "open_access": {"oa_status": "gold"},
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


def test_title_sulpub(test_session, snapshot):
    """
    title should come from sulpub before dimensions, openalex and wos_json
    """
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

    assert _pub(session).title == "On the dangers of stochastic parrots (sulpub)"


def test_title_dim(test_session, snapshot):
    """
    title should come from dimensions before openalex and wos
    """
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

    assert _pub(session).title == "On the dangers of stochastic parrots (dim)"


def test_title_openalex(test_session, snapshot):
    """
    title should come from openalex before wos
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    assert _pub(session).title == "On the dangers of stochastic parrots (openalex)"


def test_title_wos(test_session, snapshot):
    """
    title should come from wos if all the others are unavaialable
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    assert _pub(session).title == "On the dangers of stochastic parrots (wos)"


def test_title_none(test_session, snapshot):
    """
    Ensure that no title doesn't cause a problem.
    """
    with test_session.begin() as session:
        session.add(Publication(doi="10.1515/9781503624153"))

    distill(snapshot)

    assert _pub(session).title is None


# test the pub_year preferences


def test_pub_year_sulpub(test_session, snapshot):
    """
    pub_year should come from sulpub before dimensions, openalex and wos
    """
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

    assert _pub(session).pub_year == 2020


def test_pub_year_sulpub_future(test_session, snapshot):
    """
    pub_year should not come from sulpub since it is in the future (get from another source)
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json_future_year,
                dim_json=dim_json,
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    assert (
        _pub(session).pub_year == 2021
    )  # comes from dimensions, not the 2105 from sul-pub!


def test_pub_year_dim(test_session, snapshot):
    """
    pub_year should come from dimensions before openalex and wos
    """
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

    assert _pub(session).pub_year == 2021


def test_pub_year_openalex(test_session, snapshot):
    """
    pub_year should come from openalex before wos
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    assert _pub(session).pub_year == 2022


def test_pub_year_wos(test_session, snapshot):
    """
    pub_year should come from wos if all others are unavailable
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    assert _pub(session).pub_year == 2023


def test_pub_year_none(test_session, snapshot):
    """
    no pub_year shouldn't be a problem
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
            )
        )

    distill(snapshot)

    assert _pub(session).pub_year is None


# test open-access


def test_open_access_openalex(test_session, snapshot):
    """
    open_access should come from openalex first
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json=openalex_json,
                dim_json=dim_json,
            )
        )

    distill(snapshot)

    assert _pub(session).open_access == "gold"


def test_open_access_dim(test_session, snapshot):
    """
    open_access should come from dimensions if it is unavailable in openalex
    """
    with test_session.begin() as session:
        session.add(Publication(doi="10.1515/9781503624153", dim_json=dim_json))

    distill(snapshot)

    assert _pub(session).open_access == "green"


def test_open_access_null(test_session, snapshot):
    """
    dimensions is still used when there is an empty in openalex open_access
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json={"open_access": []},
                dim_json=dim_json,
            )
        )

    distill(snapshot)

    assert _pub(session).open_access == "green"


def test_multiple(test_session, snapshot):
    """
    dimensions is still used when there is an empty in openalex open_access
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    sulpub_json=sulpub_json,
                    dim_json=dim_json,
                    openalex_json=openalex_json,
                    wos_json=wos_json,
                ),
                Publication(
                    doi="10.1515/9781503624153-2",
                    sulpub_json=sulpub_json,
                    dim_json=dim_json,
                    wos_json=wos_json,
                ),
            ]
        )

    distill(snapshot)

    assert _pub(session, "10.1515/9781503624153").open_access == "gold", (
        "prefer open alex"
    )
    assert _pub(session, "10.1515/9781503624153-2").open_access == "green", (
        "fell back to dimensions"
    )


# test apc costs


def test_apc_openalex(test_session, snapshot):
    """
    use openalex apc_paid.value_usd to find initial APC cost
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    openalex_json={
                        "apc_paid": {"value_usd": 123},
                        "apc_list": {"value_usd": 1234},
                    },
                ),
            ]
        )

    distill(snapshot)

    assert _pub(session).apc == 123


def test_apc_openalex_fallback(test_session, snapshot):
    """
    fallback to openalex.apc_list if openalex.apc_paid isn't there
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    openalex_json={"apc_list": {"value_usd": 1234}},
                ),
            ]
        )

    distill(snapshot)

    assert _pub(session).apc == 1234


def test_apc_negative(test_session, snapshot):
    """
    negative apc values are not returned
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    openalex_json={
                        "apc_paid": {"value_usd": -123},
                    },
                ),
            ]
        )

    distill(snapshot)

    assert _pub(session).apc is None


def test_apc_not_a_number(test_session, snapshot):
    """
    non numeric apc values are not returned
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    openalex_json={
                        "apc_paid": {"value_usd": "junk"},
                    },
                ),
            ]
        )

    distill(snapshot)

    assert _pub(session).apc is None


def test_apc_dataset(test_session, snapshot):
    """
    Use APC 2024 dataset to get APC cost when openalex apc_paid isn't there.
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    dim_json={
                        "year": 2022,
                        "apc_list": {
                            # the dataset should be preferred to this value
                            "value_usd": 123
                        },
                        "issn": ["1234-5678", "2376-0605"],
                    },
                ),
            ]
        )

    distill(snapshot)

    # ignore the apc_list and use the value in the dataset
    assert _pub(session).apc == 400


def test_apc_closed_oa(test_session, snapshot):
    """
    pubs with a closed open access status should not have an APC
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={
                    "year": 2021,
                    "open_access": ["closed"],
                    "issn": None,
                },
                openalex_json={
                    "apc_paid": {"value_usd": 123},
                },
            ),
        )

    distill(snapshot)

    assert _pub(session).apc == 0


def test_missing_dim_issn(test_session, snapshot):
    """
    Use APC 2024 dataset to get APC cost when openalex apc_paid isn't there.
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    dim_json={
                        "year": 2022,
                        "apc_list": {
                            # the dataset should be preferred to this value
                            "value_usd": 123
                        },
                        "issn": None,
                    },
                ),
            ]
        )

    distill(snapshot)

    # issn list of None is ignored
    assert _pub(session).apc is None


def test_non_int_year(test_session, snapshot, caplog):
    """
    Test that non-integer years don't cause a problem.
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    sulpub_json={"year": "nope"},
                    dim_json={"year": None},
                ),
            ]
        )

    distill(snapshot)
    assert _pub(session).pub_year is None


def test_non_int_year_fallback(test_session, snapshot, caplog):
    """
    Test that sulpub non-integer year doesn't prevent a year coming from
    dimensions.
    """
    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    sulpub_json={"year": "nope"},
                    dim_json={"year": 2022},
                ),
            ]
        )

    distill(snapshot)
    assert _pub(session).pub_year == 2022
    assert 'got "nope" instead of int' in caplog.text


def _pub(session, doi="10.1515/9781503624153"):
    return session.query(Publication).where(Publication.doi == doi).first()
