from rialto_airflow.distiller.abstract import (
    abstract,
    _pubmed_abstract,
    _rebuild_abstract,
    _crossref_abstract,
)
from rialto_airflow.schema.harvest import Publication


def test_pubmed_abstract(pubmed_json):
    abstract = _pubmed_abstract(pubmed_json)
    assert (
        abstract
        == "Comorbid insomnia with obstructive sleep apnea (COMISA) is associated with worse daytime function and more medical/psychiatric comorbidities vs either condition alone. E2006-G000-304 was a phase 3, one-month polysomnography trial in adults aged ≥55 years with insomnia."
    )


def test_pubmed():
    metadata = {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
                },
                "Abstract": {
                    "AbstractText": [
                        "This is the abstract.",
                        "It provides a summary of the article.",
                    ]
                },
            }
        },
        "PubmedData": {
            "ArticleIdList": {
                "ArticleId": [
                    {"@IdType": "pubmed", "#text": "36857419"},
                    {"@IdType": "doi", "#text": "10.1182/bloodadvances.2022008893"},
                ]
            },
        },
    }

    abstract = _pubmed_abstract(metadata)
    assert abstract == "This is the abstract. It provides a summary of the article."


def test_pubmed_fields_no_abstract():
    pubmed_no_abstract = {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
                },
            }
        },
    }

    abstract = _pubmed_abstract(pubmed_no_abstract)
    assert abstract is None


def test_dimensions_fields(test_session, dim_json_fields):
    # Add a publication with fields sourced from Dimensions
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My Dimensions Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=dim_json_fields,
            openalex_json=None,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
            publisher="Dimensions Publisher",
            journal_name="Delicious Limes Journal of Science",
        )
    session.add(pub)

    row = session.query(Publication).filter_by(doi="10.000/000003").first()
    assert abstract(row) == "This is a sample Dimensions abstract."


def test_openalex(test_session, openalex_json):
    # Add a publication with fields only sourced from OpenAlex
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/000003",
            title="My OpenAlex Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            openalex_json=openalex_json,
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
        session.add(pub)
        pub_row = session.query(Publication).filter_by(doi="10.000/000003").first()
        assert abstract(pub_row) == "This is an abstract which is inverted."


def test_rebuild_empty_abstract():
    openalex_json = {
        "id": "https://openalex.org/W123456789",
        "biblio": {"issue": "11", "first_page": "1", "last_page": "9", "volume": "2"},
        "abstract_inverted_index": None,
    }
    abstract = _rebuild_abstract(openalex_json)
    assert abstract is None


def test_crossref_abstract_with_jats_single_paragraph():
    """Test extracting plain text from JATS XML with single <jats:p>"""
    crossref_json = {
        "abstract": "<jats:title>Abstract</jats:title><jats:p>A generic search is presented for the associated production of a Z boson or a photon with an additional unspecified massive particle X.</jats:p>"
    }
    result = _crossref_abstract(crossref_json)
    assert (
        result
        == "A generic search is presented for the associated production of a Z boson or a photon with an additional unspecified massive particle X."
    )


def test_crossref_abstract_with_jats_multiple_paragraphs():
    """Test extracting plain text from JATS XML with multiple <jats:p> elements"""
    crossref_json = {
        "abstract": "<jats:title>Abstract</jats:title><jats:p>First paragraph of abstract.</jats:p><jats:p>Second paragraph continues here.</jats:p>"
    }
    result = _crossref_abstract(crossref_json)
    # BeautifulSoup's get_text(strip=True) removes whitespace but doesn't add spaces between tags
    assert result == "First paragraph of abstract.Second paragraph continues here."


def test_crossref_abstract_with_nested_tags():
    """Test extracting text when <p> contains nested HTML tags like bold, italic"""
    crossref_json = {
        "abstract": "<p>Text with <b>bold</b> and <i>italic</i> formatting.</p>"
    }
    result = _crossref_abstract(crossref_json)
    # BeautifulSoup strips tags but doesn't add spaces around inline elements
    assert result == "Text with bold and italic formatting."


def test_crossref_abstract_plain_text():
    """Test that plain text abstracts are returned as-is"""
    crossref_json = {"abstract": "This is a plain abstract without any markup."}
    result = _crossref_abstract(crossref_json)
    assert result == "This is a plain abstract without any markup."


def test_crossref_abstract_with_angle_brackets_in_text():
    """Test that plain text with angle brackets (e.g., math expressions) is preserved"""
    crossref_json = {
        "abstract": "We compare values where 2 < 3 and X > Y in our analysis."
    }
    result = _crossref_abstract(crossref_json)
    assert result == "We compare values where 2 < 3 and X > Y in our analysis."


def test_crossref_abstract_none_json():
    """Test handling when crossref_json is {}"""
    result = _crossref_abstract({})
    assert result is None


def test_crossref_abstract_missing_abstract_key():
    """Test handling when 'abstract' key is missing"""
    crossref_json = {"title": "Some Title"}
    result = _crossref_abstract(crossref_json)
    assert result is None


def test_crossref_abstract_empty_string():
    """Test handling empty abstract string"""
    crossref_json = {"abstract": ""}
    result = _crossref_abstract(crossref_json)
    # Empty string returns empty string (not None)
    assert result == ""


def test_crossref_abstract_with_jats_title_only():
    """Test behavior when only jats:title exists without jats:p"""
    crossref_json = {"abstract": "<jats:title>Abstract</jats:title>"}
    result = _crossref_abstract(crossref_json)
    # Should return empty/None since there's no actual content after title is stripped
    assert result is None or result == ""
