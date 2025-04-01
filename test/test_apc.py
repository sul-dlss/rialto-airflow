from rialto_airflow import apc


def test_apc():
    usd = apc.get_apc(issn="2376-0605", year=2022)
    assert usd == 400


def test_missing():
    usd = apc.get_apc(issn="foo", year=1999)
    assert usd is None


def test_warning(caplog):
    assert apc.get_apc(issn="1440-1703", year=2019) == 3140
    assert "more than one APC match for 1440-1703 and 2019" in caplog.text


def test_negative(caplog):
    assert (
        apc.get_apc(issn="1234-5678", year=2022) is None
    )  # fake test entry set to a negative number


def test_nan():
    assert apc.get_apc(issn="2173-5735", year=2023) is None
