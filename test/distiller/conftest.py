import pytest

from rialto_airflow.schema.harvest import Publication

# Note: the sulpub_json, dim_json, openalex_json and wos_json fixtures here
# override the similarly named fixtures available in test/conftest.py


@pytest.fixture
def sulpub_json():
    return {
        "title": "On the dangers of stochastic parrots (sulpub)",
        "year": "2020",
        "issn": "3333-3333",
        "journal": {"pages": "1-7"},
    }


@pytest.fixture
def dim_json():
    return {
        "title": "On the dangers of stochastic parrots (dim)",
        "year": 2021,
        "open_access": ["oa_all", "green"],
        "type": "article",
        "issn": "1111-1111",
    }


@pytest.fixture
def openalex_json():
    return {
        "title": "On the dangers of stochastic parrots (openalex)",
        "publication_year": 2022,
        "open_access": {"oa_status": "gold"},
        "type": "preprint",
        "awards": [
            {
                "funder_award_id": "ABI 1661218",
            },
            {
                "funder_award_id": None,
            },
        ],
        "abstract_inverted_index": {
            "This": [0],
            "is": [1, 5],
            "an": [2],
            "abstract": [3],
            "which": [
                4,
            ],
            "inverted.": [6],
        },
        "primary_location": {
            "source": {
                "id": "https://openalex.org/S2764375719",
                "display_name": "Choice Reviews Online",
                "issn_l": "0009-4978",
                "issn": ["0009-4978", "1523-8253", "1943-5975"],
                "host_organization": "https://openalex.org/P4310316146",
                "host_organization_name": "Association of College and Research Libraries",
                "host_organization_lineage": [
                    "https://openalex.org/P4310315903",
                    "https://openalex.org/P4310316146",
                ],
                "host_organization_lineage_names": [
                    "American Library Association",
                    "Association of College and Research Libraries",
                ],
                "type": "journal",
            }
        },
        "biblio": {"issue": "11", "first_page": "1", "last_page": "9", "volume": "2"},
    }


@pytest.fixture
def wos_json():
    return {
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
            },
        }
    }


@pytest.fixture
def dim_json_fields():
    return {
        "type": "article",
        "doi": "10.000/000003",
        "issue": "12",
        "pages": "1-10",
        "volume": "1",
        "mesh_terms": ["Delicions", "Limes"],
        "pmid": "123",
        "linkout": "https://example_dim.com",
        "abstract": "This is a sample Dimensions abstract.",
    }


@pytest.fixture
def pub_with_authors():
    return Publication(
        doi="10.000/example",
        title="I'm a Book",
        apc=None,
        open_access="gold",
        pub_year=2023,
        openalex_json={
            "authorships": [
                {
                    "author": {
                        "display_name": "Jane Open Alex",
                        "orcid": "jane-open-alex",
                    }
                },
                {
                    "author": {
                        "display_name": "Mike Open Alex",
                        "orcid": "mike-open-alex",
                    }
                },
                {
                    "author": {
                        "display_name": "Leland Open Alex",
                        "orcid": "leland-open-alex",
                    }
                },
            ]
        },
        dim_json={
            "authors": [
                {
                    "first_name": "Jane",
                    "last_name": "Dimensions",
                    "orcid": ["jane-dimensions"],
                },
                {
                    "first_name": "Mike",
                    "last_name": "Dimensions",
                    "orcid": ["mike-dimensions"],
                },
                {
                    "first_name": "Leland",
                    "last_name": "Dimensions",
                    "orcid": ["leland-dimensions"],
                },
            ]
        },
        pubmed_json={
            "MedlineCitation": {
                "Article": {
                    "AuthorList": {
                        "Author": [
                            {
                                "ForeName": "Jane",
                                "LastName": "Pubmed",
                                "Identifier": {
                                    "@Source": "ORCID",
                                    "#text": "jane-pubmed",
                                },
                            },
                            {
                                "ForeName": "Mike",
                                "LastName": "Pubmed",
                                "Identifier": {
                                    "@Source": "ORCID",
                                    "#text": "mike-pubmed",
                                },
                            },
                            {
                                "ForeName": "Leland",
                                "LastName": "Pubmed",
                                "Identifier": {
                                    "@Source": "ORCID",
                                    "#text": "leland-pubmed",
                                },
                            },
                        ]
                    }
                }
            }
        },
        wos_json={
            "static_data": {
                "summary": {
                    "names": {
                        "name": [
                            {"display_name": "Jane Wos", "orcid_id": "jane-wos"},
                            {"display_name": "Mike Wos", "orcid_id": "mike-wos"},
                            {"display_name": "Leland Wos", "orcid_id": "leland-wos"},
                        ]
                    }
                }
            }
        },
        crossref_json={
            "author": [
                {
                    "given": "Jane",
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/jane-crossref",
                },
                {
                    "given": "Mike",
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/mike-crossref",
                },
                {
                    "given": "Leland",
                    "family": "Crossref",
                    "ORCID": "https://orcid.org/leland-crossref",
                },
            ]
        },
        sulpub_json={
            "author": [
                {"name": "Sulpub, Jane Elizabeth Lathrop"},
                {"name": "Sulpub, Mike"},
                {"name": "Sulpub, Leland DeWitt"},
            ]
        },
    )


@pytest.fixture
def one_author():
    return Publication(
        doi="10.000/example",
        title="I'm a Book",
        apc=None,
        open_access="gold",
        pub_year=2023,
        pubmed_json={
            "MedlineCitation": {
                "Article": {
                    "AuthorList": {
                        "Author": {
                            "ForeName": "Jane",
                            "LastName": "Pubmed",
                            "Identifier": {
                                "@Source": "ORCID",
                                "#text": "jane-pubmed",
                            },
                        }
                    }
                }
            }
        },
        wos_json={
            "static_data": {
                "summary": {
                    "names": {
                        "name": {"display_name": "Jane Wos", "orcid_id": "jane-wos"}
                    }
                }
            }
        },
    )
