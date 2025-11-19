from rialto_airflow.distiller import apc
from rialto_airflow.schema.harvest import Publication


def test_openalex():
    """
    use openalex apc_paid.value_usd to find initial APC cost
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json={
            "apc_paid": {"value_usd": 123},
            "apc_list": {"value_usd": 1234},
        },
    )

    assert apc(pub, {}) == 123


def test_openalex_fallback():
    """
    fallback to openalex.apc_list if openalex.apc_paid isn't there
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json={"apc_list": {"value_usd": 1234}},
    )

    apc(pub, {}) == 1234


def test_negative():
    """
    negative apc values are not returned
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json={
            "apc_paid": {"value_usd": -123},
        },
    )

    assert apc(pub, {}) is None


def test_not_a_number():
    """
    non numeric apc values are not returned
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        openalex_json={
            "apc_paid": {"value_usd": "junk"},
        },
    )

    assert apc(pub, {}) is None


def test_dataset():
    """
    Use APC 2024 dataset to get APC cost when openalex apc_paid isn't there.
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={
            "year": 2022,
            "apc_list": {
                # the dataset should be preferred to this value
                "value_usd": 123
            },
            "issn": ["1234-5678", "2376-0605"],
        },
    )

    # ignore the apc_list and use the value in the dataset
    assert apc(pub, {"pub_year": 2022}) == 400


def test_closed_oa():
    """
    pubs with a closed open access status should not have an APC
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={
            "year": 2021,
            "open_access": ["closed"],
            "issn": None,
        },
        openalex_json={
            "apc_paid": {"value_usd": 123},
        },
    )

    assert apc(pub, {"pub_year": 2021, "open_access": "closed"}) == 0


def test_diamond_apc():
    """
    diamond openaccess publications with no apc are assigned $0
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={
            "year": 2021,
            "open_access": ["diamond"],
            "issn": None,
        },
    )

    assert apc(pub, {"pub_year": 2021, "open_access": "diamond"}) == 0


def test_hybrid_apc():
    """
    hybrid openaccess publications with no apc are assigned $3600
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={
            "year": 2021,
            "open_access": ["hybrid"],
            "issn": None,
        },
    )

    assert apc(pub, {"pub_year": 2021, "open_access": "hybrid"}) == 3600


def test_gold_apc():
    """
    gold openaccess publications with no apc are assigned $2450
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={
            "year": 2021,
            "open_access": ["gold"],
            "issn": None,
        },
    )

    assert apc(pub, {"pub_year": 2021, "open_access": "gold"}) == 2450


def test_missing_dim_issn():
    """
    Use APC 2024 dataset to get APC cost when openalex apc_paid isn't there.
    """
    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={
            "year": 2022,
            "apc_list": {
                # the dataset should be preferred to this value
                "value_usd": 123
            },
            "issn": None,
        },
    )

    # issn list of None is ignored
    assert apc(pub, {"pub_year": 2022}) is None
