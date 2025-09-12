# import test.publish.data as test_data
import rialto_airflow.publish.publication_utils as pub_utils
from test.utils import TestRow


def test_journal_with_dimensions_title(dim_json, openalex_json, wos_json, sulpub_json):
    row = TestRow(
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    assert pub_utils._journal(row) == "Delicious Limes Journal of Science"


def test_journal_missing_dimensions_title(
    dim_json_no_title, openalex_json, wos_json, sulpub_json
):
    row = TestRow(
        dim_json=dim_json_no_title,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._journal(row) == "Ok Limes Journal of Science"


def test_journal_without_dimensions_title(openalex_json, wos_json, sulpub_json):
    row = TestRow(
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._journal(row) == "Ok Limes Journal of Science"


def test_journal_without_dimensions_and_missing_open_alex_title(
    openalex_no_title_json, wos_json, sulpub_json
):
    row = TestRow(
        openalex_json=openalex_no_title_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from wos
    assert pub_utils._journal(row) == "Meh Limes Journal of Science"


def test_journal_without_dimensions_or_open_alex_title(wos_json, sulpub_json):
    row = TestRow(
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from wos
    assert pub_utils._journal(row) == "Meh Limes Journal of Science"


def test_journal_without_dimensions_or_open_alex_or_wos_title(sulpub_json):
    row = TestRow(
        sulpub_json=sulpub_json,
    )
    # comes from sulpub
    assert pub_utils._journal(row) == "Bad Limes Journal of Science"


def test_journal_with_dimensions_issue(dim_json, openalex_json, wos_json, sulpub_json):
    row = TestRow(
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    assert pub_utils._issue(row) == "12"


def test_journal_without_dimensions_issue(openalex_json, wos_json, sulpub_json):
    row = TestRow(
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._issue(row) == "11"


def test_journal_without_dimensions_or_openalex_issue(wos_json, sulpub_json):
    row = TestRow(
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._issue(row) == "10"


def test_journal_without_dimensions_or_openalex_or_wos_issue(sulpub_json):
    row = TestRow(
        sulpub_json=sulpub_json,
    )
    # comes from sulpub
    assert pub_utils._issue(row) == "9"


def test_journal_with_dimensions_mesh(dim_json, openalex_json, wos_json, sulpub_json):
    row = TestRow(
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from dimensions
    assert pub_utils._mesh(row) == "Delicions|Limes"


def test_journal_without_dimensions_mesh(openalex_json, wos_json, sulpub_json):
    row = TestRow(
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._mesh(row) == "Ok|Lemons"


def test_journal_with_dimensions_pages(dim_json, openalex_json, wos_json, sulpub_json):
    row = TestRow(
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from dimensions
    assert pub_utils._pages(row) == "1-10"


def test_journal_without_dimensions_pages(openalex_json, wos_json, sulpub_json):
    row = TestRow(
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._pages(row) == "1-9"


def test_journal_without_dimensions_or_openalex_pages(wos_json, sulpub_json):
    row = TestRow(
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from wos
    assert pub_utils._pages(row) == "1-8"


def test_journal_missing_wos_page_info(wos_json_no_page_info, sulpub_json):
    row = TestRow(
        wos_json=wos_json_no_page_info,
        sulpub_json=sulpub_json,
    )
    # comes from sulpub
    assert pub_utils._pages(row) == "1-7"


def test_journal_without_dimensions_or_openalex_or_wos_pages(sulpub_json):
    row = TestRow(
        sulpub_json=sulpub_json,
    )
    # comes from sulpub
    assert pub_utils._pages(row) == "1-7"


def test_journal_with_pubmed_pmid(
    pubmed_json, dim_json, openalex_json, wos_json, sulpub_json
):
    row = TestRow(
        pubmed_json=pubmed_json,
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from pubmed
    assert pub_utils._pmid(row) == "36857419"


def test_wos_identifier_as_string():
    """
    Sometimes WoS uses a string as the identifier instead of a dictionary. We should handle that.
    """
    row = TestRow(
        wos_json={
            "dynamic_data": {
                "cluster_related": {"identifiers": {"identifier": ["nope"]}}
            }
        }
    )
    assert pub_utils._pmid(row) is None


def test_journal_without_pubmed_pmid(dim_json, openalex_json, wos_json, sulpub_json):
    row = TestRow(
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from dimensions
    assert pub_utils._pmid(row) == "123"


def test_journal_without_pubmed_or_dimensions_pmid(
    openalex_json, wos_json, sulpub_json
):
    row = TestRow(
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._pmid(row) == "1234"


def test_journal_without_pubmed_or_dimensions_or_openalex_pmid(wos_json, sulpub_json):
    row = TestRow(
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from sulpub
    assert pub_utils._pmid(row) == "123456"


def test_journal_without_pubmed_or_dimensions_or_openalex_or_sulpub_pmid(wos_json):
    row = TestRow(
        wos_json=wos_json,
    )
    # comes from wos
    assert pub_utils._pmid(row) == "1234567"


def test_journal_with_dimensions_url(dim_json, openalex_json, wos_json, sulpub_json):
    row = TestRow(
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from dimensions
    assert pub_utils._url(row) == "https://example_dim.com"


def test_journal_without_dimensions_url(openalex_json, wos_json, sulpub_json):
    row = TestRow(
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._url(row) == "https://example_openalex_pdf.com"


def test_journal_without_dimensions_or_openalex_url(wos_json, sulpub_json):
    row = TestRow(
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # no url
    assert pub_utils._url(row) is None


def test_journal_with_dimensions_volume(dim_json, openalex_json, wos_json, sulpub_json):
    row = TestRow(
        dim_json=dim_json,
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from dimensions
    assert pub_utils._volume(row) == "1"


def test_journal_without_dimensions_volume(openalex_json, wos_json, sulpub_json):
    row = TestRow(
        openalex_json=openalex_json,
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from openalex
    assert pub_utils._volume(row) == "2"


def test_journal_without_dimensions_or_openalex_volume(wos_json, sulpub_json):
    row = TestRow(
        wos_json=wos_json,
        sulpub_json=sulpub_json,
    )
    # comes from wos
    assert pub_utils._volume(row) == "3"


def test_journal_without_dimensions_or_openalex_or_wos_volume(sulpub_json):
    row = TestRow(
        sulpub_json=sulpub_json,
    )
    # comes from sulpub
    assert pub_utils._volume(row) == "4"
