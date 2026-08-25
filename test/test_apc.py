from io import StringIO

import pandas

from rialto_airflow import apc


def test_apc():
    usd = apc.get_apc(issn="2376-0605", year=2022)
    assert usd == 400


def test_missing():
    usd = apc.get_apc(issn="foo", year=1999)
    assert usd is None


def test_issn_l():
    """An ISSN that only appears in the dataset's issn_l column still matches."""
    assert apc.get_apc(issn="0002-7189", year=2021) == 3000


def test_truncates_converted_value():
    """Converted APCs carry decimals in the dataset and are truncated to int."""
    assert apc.get_apc(issn="2173-5735", year=2023) == 2712


def test_warning(caplog):
    # 0957-5820 is the issn_l of one journal and the issn1 of another, so it
    # matches two rows for 2019 with differing APCs
    assert apc.get_apc(issn="0957-5820", year=2019) == 3300
    assert "more than one APC match for 0957-5820 and 2019" in caplog.text


def test_negative(monkeypatch):
    def mock_dataset():
        mock_file_content = """issn1,issn2,issn_l,apc_year,apc_usd
2813-0324,,2813-0324,2023,
0000-0000,,0000-0000,2022,-100
"""
        mock_csv_data = StringIO(mock_file_content)
        mock_df = pandas.read_csv(mock_csv_data)
        return mock_df

    monkeypatch.setattr(apc, "df", mock_dataset())
    apc.get_apc.cache_clear()

    assert apc.get_apc(issn="0000-0000", year=2022) is None


def test_nan():
    # this journal-year is present in the dataset with type_of_fee "unknown"
    # and no apc_usd value
    assert apc.get_apc(issn="1044-0305", year=2019) is None
