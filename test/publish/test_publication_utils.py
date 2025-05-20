import test.publish.data as test_data
import rialto_airflow.publish.publication_utils as pub_utils
from test.test_utils import TestRow


def test_journal_with_dimensions_title():
    row = TestRow(
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    assert pub_utils._journal(row) == "Delicious Limes Journal of Science"


def test_journal_missing_dimensions_title():
    row = TestRow(
        dim_json=test_data.dim_json_no_title(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._journal(row) == "Ok Limes Journal of Science"


def test_journal_without_dimensions_title():
    row = TestRow(
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._journal(row) == "Ok Limes Journal of Science"


def test_journal_without_dimensions_and_missing_open_alex_title():
    row = TestRow(
        openalex_json=test_data.openalex_no_title_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from wos
    assert pub_utils._journal(row) == "Meh Limes Journal of Science"


def test_journal_without_dimensions_or_open_alex_title():
    row = TestRow(
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from wos
    assert pub_utils._journal(row) == "Meh Limes Journal of Science"


def test_journal_without_dimensions_or_open_alex_or_wos_title():
    row = TestRow(
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from sulpub
    assert pub_utils._journal(row) == "Bad Limes Journal of Science"


def test_journal_with_dimensions_issue():
    row = TestRow(
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    assert pub_utils._issue(row) == "12"


def test_journal_without_dimensions_issue():
    row = TestRow(
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._issue(row) == "11"


def test_journal_without_dimensions_or_openalex_issue():
    row = TestRow(
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._issue(row) == "10"


def test_journal_without_dimensions_or_openalex_or_wos_issue():
    row = TestRow(
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from sulpub
    assert pub_utils._issue(row) == "9"


def test_journal_with_dimensions_mesh():
    row = TestRow(
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from dimensions
    assert pub_utils._mesh(row) == "Delicions|Limes"


def test_journal_without_dimensions_mesh():
    row = TestRow(
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._mesh(row) == "Ok|Lemons"


def test_journal_with_dimensions_pages():
    row = TestRow(
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from dimensions
    assert pub_utils._pages(row) == "1-10"


def test_journal_without_dimensions_pages():
    row = TestRow(
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._pages(row) == "1-9"


def test_journal_without_dimensions_or_openalex_pages():
    row = TestRow(
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from wos
    assert pub_utils._pages(row) == "1-8"


def test_journal_missing_wos_page_info():
    row = TestRow(
        wos_json=test_data.wos_json_no_page_info(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from sulpub
    assert pub_utils._pages(row) == "1-7"


def test_journal_without_dimensions_or_openalex_or_wos_pages():
    row = TestRow(
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from sulpub
    assert pub_utils._pages(row) == "1-7"


def test_journal_with_pubmed_pmid():
    row = TestRow(
        pubmed_json=test_data.pubmed_json(),
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from pubmed
    assert pub_utils._pmid(row) == "36857419"


def test_journal_without_pubmed_pmid():
    row = TestRow(
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from dimensions
    assert pub_utils._pmid(row) == "123"


def test_journal_without_pubmed_or_dimensions_pmid():
    row = TestRow(
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._pmid(row) == "1234"


def test_journal_without_pubmed_or_dimensions_or_openalex_pmid():
    row = TestRow(
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from sulpub
    assert pub_utils._pmid(row) == "123456"


def test_journal_without_pubmed_or_dimensions_or_openalex_or_sulpub_pmid():
    row = TestRow(
        wos_json=test_data.wos_json(),
    )
    # comes from wos
    assert pub_utils._pmid(row) == "1234567"


def test_journal_with_dimensions_url():
    row = TestRow(
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from dimensions
    assert pub_utils._url(row) == "https://example_dim.com"


def test_journal_without_dimensions_url():
    row = TestRow(
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._url(row) == "https://example_openalex_pdf.com"


def test_journal_without_dimensions_or_openalex_url():
    row = TestRow(
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # no url
    assert pub_utils._url(row) is None


def test_journal_with_dimensions_volume():
    row = TestRow(
        dim_json=test_data.dim_json(),
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from dimensions
    assert pub_utils._volume(row) == "1"


def test_journal_without_dimensions_volume():
    row = TestRow(
        openalex_json=test_data.openalex_json(),
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from openalex
    assert pub_utils._volume(row) == "2"


def test_journal_without_dimensions_or_openalex_volume():
    row = TestRow(
        wos_json=test_data.wos_json(),
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from wos
    assert pub_utils._volume(row) == "3"


def test_journal_without_dimensions_or_openalex_or_wos_volume():
    row = TestRow(
        sulpub_json=test_data.sulpub_json(),
    )
    # comes from sulpub
    assert pub_utils._volume(row) == "4"
