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


from io import StringIO
import pandas


def test_negative(monkeypatch):
    def mock_dataset():
        mock_file_content = """unique_id	Publisher	ISSN_1	ISSN_2	Journal	OA_status	APC_provided	APC_order	APC_USD	APC_USD-originalORconverted	APC_EUR	APC_EUR-originalORconverted	APC_GBP	APC_GBP-originalORconverted	APC_JPY	APC_JPY-originalORconverted	APC_CHF	APC_CHF-originalORconverted	APC_CAD	APC_CAD-originalORconverted	APC_date	APC_year	APC_source	Collector	Comment
9009	MDPI	2813-0324		Computer Sciences & Mathematics Forum	Gold	no			_no_APC_provided		_no_APC_provided		_no_APC_provided		_no_APC_provided		_no_APC_provided		_no_APC_provided	2023-06-24	2023	Wayback Machine	E. Schares
9010	MDPI	0000-0000		Entomology	Gold	yes	1	-100	converted from CHF	995.6660	converted from CHF	849.3570	converted from CHF	137535.1240	converted from CHF	1000.0000	original	1363.2520	converted from CHF	2022-07-22	2022	Wayback Machine	E. Schares
"""
        mock_csv_data = StringIO(mock_file_content)
        mock_df = pandas.read_csv(mock_csv_data, delimiter="\t", encoding="ISO-8859-1")
        return mock_df

    monkeypatch.setattr(apc, "df", mock_dataset())

    assert apc.get_apc(issn="0000-0000", year=2022) is None


    assert (
        apc.get_apc(issn="1234-5678", year=2022) is None
    )  # fake test entry set to a negative number


def test_nan():
    assert apc.get_apc(issn="2173-5735", year=2023) is None
