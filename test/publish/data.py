# Example data for testing the publish module


def dim_json():
    return {
        "type": "article",
        "doi": "10.000/000001",
        "journal": {"title": "Delicious Limes Journal of Science"},
        "issue": "12",
        "pages": "1-10",
        "volume": "1",
        "mesh_terms": ["Delicions", "Limes"],
        "pmid": "123",
        "linkout": "https://example_dim.com",
    }


def dim_json_no_title():
    return {"other_data": {"bogus": "no journal title here"}}


def openalex_json():
    return {
        "type": "preprint",
        "biblio": {"issue": "11", "first_page": "1", "last_page": "9", "volume": "2"},
        "primary_location": {
            "source": {
                "type": "journal",
                "display_name": "Ok Limes Journal of Science",
            }
        },
        "locations": [
            {"pdf_url": "https://example_openalex_pdf.com"},
        ],
        "mesh": [
            {"descriptor_name": "Ok"},
            {"descriptor_name": "Lemons"},
        ],
        "ids": {
            "doi": "10.000/000001",
            "pmid": "1234",
        },
    }


def openalex_no_title_json():
    return {
        "locations": [
            {
                "bogus": {
                    "nope": "not here",
                }
            },
            {"source": {"type": "geography", "display_name": "New York"}},
        ],
    }


def wos_json():
    return {
        "UID": "WOS:000123456789",
        "fullrecord_metadata": {
            "normalized_doctypes": {"doctype": ["Article", "Abstract"]}
        },
        "static_data": {
            "summary": {
                "pub_info": {
                    "pubyear": "2023",
                    "issue": "10",
                    "vol": "3",
                    "page": {"begin": "1", "end": "8"},
                },
                "titles": {
                    "count": 2,
                    "title": [
                        {"type": "source", "content": "Meh Limes Journal of Science"},
                        {"type": "bogus", "content": "Bogus"},
                    ],
                },
            }
        },
        "dynamic_data": {
            "cluster_related": {
                "identifiers": {
                    "identifier": [
                        {
                            "type": "pmid",
                            "value": "1234567",
                        }
                    ],
                },
            }
        },
    }


def wos_json_no_page_info():
    return {
        "static_data": {
            "summary": {
                "pub_info": {
                    "page": {"page_count": "7"},
                },
            }
        }
    }


def sulpub_json():
    return {
        "journal": {
            "name": "Bad Limes Journal of Science",
            "issue": "9",
            "pages": "1-7",
            "volume": "4",
        },
        "pmid": "123456",
    }


def pubmed_json():
    return {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Title",
            },
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
