import logging
import pytest

from rialto_airflow.schema.harvest import Publication, Author
from rialto_airflow.harvest.distill import distill, _normalize_type, _journal_issn

# Set up JSON data that mirrors (in part) what we get from the respective APIs

sulpub_json = {
    "title": "On the dangers of stochastic parrots (sulpub)",
    "year": "2020",
    "issn": "3333-3333",
}

sulpub_json_journal_year = {
    "title": "On the dangers of stochastic parrots (sulpub journal year)",
    "issn": "3333-3333",
    "journal": {
        "name": "Journal of Smart Things That People Write",
        "year": "2013",
        "issue": "1",
    },
}

dim_json_future_year = {
    "title": "On the dangers of stochastic parrots (dim future)",
    "year": "2105",
    "type": "article",
}

dim_json = {
    "title": "On the dangers of stochastic parrots (dim)",
    "year": 2021,
    "open_access": ["oa_all", "green"],
    "type": "article",
    "issn": "1111-1111",
}

openalex_json = {
    "title": "On the dangers of stochastic parrots (openalex)",
    "publication_year": 2022,
    "open_access": {"oa_status": "gold"},
    "type": "preprint",
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
        },
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
    title should come from wos if all the others are unavailable
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


def test_title_booktitle(test_session, snapshot):
    """
    title should come from sulpub's booktitle if it's not available elsewhere
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json={"booktitle": "Gravity's Rainbow"},
            )
        )

    distill(snapshot)

    assert _pub(session).title == "Gravity's Rainbow"


# test the pub_year preferences


def test_pub_year_sulpub(test_session, snapshot):
    """
    pub_year should come from sul_pub if all others are unavailable
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json,
            )
        )

    distill(snapshot)

    assert _pub(session).pub_year == 2020


def test_pub_year_from_journal_sulpub(test_session, snapshot):
    """
    pub_year should come from sul_pub journal if all others are unavailable and year is not at top level
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json_journal_year,
            )
        )

    distill(snapshot)

    assert _pub(session).pub_year == 2013


def test_pub_year_dim_future(test_session, snapshot):
    """
    pub_year should not come from dimensions since it is in the future (get from another source)
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json,
                dim_json=dim_json_future_year,
                openalex_json=openalex_json,
                wos_json=wos_json,
            )
        )

    distill(snapshot)

    assert (
        _pub(session).pub_year == 2022
    )  # comes from openalex, not the 2105 from dimensions!


def test_pub_year_dim(test_session, snapshot):
    """
    pub_year should come from dimensions before openalex, wos, and sulpub
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json=dim_json,
                openalex_json=openalex_json,
                wos_json=wos_json,
                sulpub_json=sulpub_json,
            )
        )

    distill(snapshot)

    assert _pub(session).pub_year == 2021


def test_pub_year_openalex(test_session, snapshot):
    """
    pub_year should come from openalex before wos and sulpub
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json=openalex_json,
                wos_json=wos_json,
                sulpub_json=sulpub_json,
            )
        )

    distill(snapshot)

    assert _pub(session).pub_year == 2022


def test_pub_year_wos(test_session, snapshot):
    """
    pub_year should come from wos before sulpub
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                wos_json=wos_json,
                sulpub_json=sulpub_json,
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


def test_diamond_apc(test_session, snapshot):
    """
    diamond openaccess publications with no apc are assigned $0
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={
                    "year": 2021,
                    "open_access": ["diamond"],
                    "issn": None,
                },
            ),
        )

    distill(snapshot)

    assert _pub(session).apc == 0


def test_hybrid_apc(test_session, snapshot):
    """
    hybrid openaccess publications with no apc are assigned $3600
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={
                    "year": 2021,
                    "open_access": ["hybrid"],
                    "issn": None,
                },
            ),
        )

    distill(snapshot)

    assert _pub(session).apc == 3600


def test_gold_apc(test_session, snapshot):
    """
    gold openaccess publications with no apc are assigned $2450
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                dim_json={
                    "year": 2021,
                    "open_access": ["gold"],
                    "issn": None,
                },
            ),
        )

    distill(snapshot)

    assert _pub(session).apc == 2450


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
    Test that higher priority non-integer year doesn't prevent a year coming from another source.
    """
    caplog.set_level(logging.DEBUG)

    with test_session.begin() as session:
        session.bulk_save_objects(
            [
                Publication(
                    doi="10.1515/9781503624153",
                    dim_json={"year": "nope"},
                    openalex_json={"publication_year": 2022},
                ),
            ]
        )

    distill(snapshot)
    assert _pub(session).pub_year == 2022
    assert (
        "could not cast 'nope' to a non-future year; jpath=year; data={'year': 'nope'}"
        in caplog.text
    )


def test_types(test_session, snapshot, caplog):
    """
    Test that only the types from the first match are returned.
    """

    # set up a publication with some initial type metadata where we would expect
    # to find it, and slowly pare the different platform metadata away to make
    # sure the rules are matching correctly.

    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624153",
            dim_json={"type": "Book"},
            openalex_json={"type": "Chapter"},
            sulpub_json={"type": "Dissertation"},
            crossref_json={"type": "Dataset"},
            wos_json={
                "static_data": {
                    "fullrecord_metadata": {
                        "normalized_doctypes": {"doctype": "Article"}
                    }
                }
            },
            pubmed_json={
                "MedlineCitation": {
                    "Article": {
                        "PublicationTypeList": {
                            "PublicationType": [
                                {"#text": "Article"},
                                {"#text": "Preprint"},
                                {"#text": "Article"},
                            ]
                        }
                    }
                }
            },
        )
        session.add(pub)

    # dimensions takes priority
    distill(snapshot)
    assert _pub(session).types == ["Book"]

    # openalex next
    with test_session.begin() as session:
        pub = _pub(session)
        pub.dim_json = None
        session.add(pub)

    distill(snapshot)
    assert _pub(session).types == ["Chapter"]

    # pubmed next
    with test_session.begin() as session:
        pub = _pub(session)
        pub.openalex_json = None
        session.add(pub)

    distill(snapshot)
    assert _pub(session).types == ["Article", "Preprint"]

    # wos next
    with test_session.begin() as session:
        pub = _pub(session)
        pub.pubmed_json = None
        session.add(pub)

    distill(snapshot)
    assert _pub(session).types == ["Article"]

    # crossref next
    with test_session.begin() as session:
        pub = _pub(session)
        pub.wos_json = None
        session.add(pub)

    distill(snapshot)
    assert _pub(session).types == ["Dataset"]

    # sulpub next
    with test_session.begin() as session:
        pub = _pub(session)
        pub.crossref_json = None
        session.add(pub)

    distill(snapshot)
    assert _pub(session).types == ["Dissertation"]

    # unepected json shouldn't cause a problem
    with test_session.begin() as session:
        pub = _pub(session)
        pub.sulpub_json = {"foo": "bar"}
        session.add(pub)

    distill(snapshot)
    assert _pub(session).types == []

    # no json would be weird, but shouldn't cause a problem w/ distill
    with test_session.begin() as session:
        pub = _pub(session)
        pub.sulpub_json = None
        session.add(pub)

    distill(snapshot)
    assert _pub(session).types == []

    # unexpected json throws an distiller exception
    with test_session.begin() as session:
        pub = _pub(session)
        pub.sulpub_json = {"type": {"foo": "bar"}}
        session.add(pub)

    with pytest.raises(Exception) as e:
        distill(snapshot)

    assert (
        str(e.value)
        == "types distill rules generated unexpected result: <class 'dict'>"
    )


def test_normalize_type():
    assert _normalize_type("book") == "Book"
    assert _normalize_type("book-chapter") == "Chapter"
    assert _normalize_type("book-part") == "Chapter"
    assert _normalize_type("book-section") == "Chapter"
    assert _normalize_type("book-series") == "Other"
    assert _normalize_type("book-set") == "Other"
    assert _normalize_type("component") == "Other"
    assert _normalize_type("database") == "Other"
    assert _normalize_type("dataset") == "Dataset"
    assert _normalize_type("dissertation") == "Dissertation"
    assert _normalize_type("edited-book") == "Book"
    assert _normalize_type("journal") == "Other"
    assert _normalize_type("journal article") == "Article"
    assert _normalize_type("journal-article") == "Article"
    assert _normalize_type("journal-issue") == "Other"
    assert _normalize_type("monograph") == "Book"
    assert _normalize_type("other") == "Other"
    assert _normalize_type("posted-content") == "Other"
    assert _normalize_type("proceedings") == "Other"
    assert _normalize_type("proceedings-article") == "Article"
    assert _normalize_type("reference-book") == "Other"
    assert _normalize_type("reference-entry") == "Other"
    assert _normalize_type("report") == "Other"
    assert _normalize_type("report-component") == "Other"
    assert _normalize_type("report-series") == "Other"
    assert _normalize_type("standard") == "Other"
    assert _normalize_type("abstract") == "Other"
    assert _normalize_type("address") == "Other"
    assert _normalize_type("art and literature") == "Other"
    assert _normalize_type("article") == "Article"
    assert _normalize_type("bibliography") == "Other"
    assert _normalize_type("biography") == "Book"
    assert _normalize_type("case reports") == "Other"
    assert _normalize_type("caseStudy") == "Other"
    assert _normalize_type("chapter") == "Chapter"
    assert _normalize_type("congress") == "Other"
    assert _normalize_type("correction") == "Correction/Retraction"
    assert _normalize_type("data paper") == "Article"
    assert _normalize_type("data set") == "Dataset"
    assert _normalize_type("data study") == "Other"
    assert _normalize_type("dictionary") == "Other"
    assert _normalize_type("early access") == "Article"
    assert _normalize_type("editorial") == "Editorial Material "
    assert _normalize_type("editorial material") == "Editorial Material "
    assert _normalize_type("erratum") == "Correction/Retraction"
    assert _normalize_type("expression of concern") == "Correction/Retraction"
    assert _normalize_type("festschrift") == "Book"
    assert _normalize_type("inbook") == "Chapter"
    assert _normalize_type("inproceedings") == "Article"
    assert _normalize_type("interview") == "Other"
    assert _normalize_type("introductory journal article") == "Other"
    assert _normalize_type("item withdrawal") == "Correction/Retraction"
    assert _normalize_type("lecture") == "Other"
    assert _normalize_type("letter") == "Other"
    assert _normalize_type("libguides") == "Other"
    assert _normalize_type("meeting") == "Other"
    assert _normalize_type("news") == "Other"
    assert _normalize_type("otherPaper") == "Other"
    assert _normalize_type("paratext") == "Other"
    assert _normalize_type("patient education handout") == "Other"
    assert _normalize_type("peer-review") == "Other"
    assert _normalize_type("personal narrative") == "Other"
    assert _normalize_type("preprint") == "Preprint"
    assert _normalize_type("proceeding") == "Article"
    assert (
        _normalize_type("publication with expression of concern")
        == "Correction/Retraction"
    )
    assert _normalize_type("published erratum") == "Correction/Retraction"
    assert _normalize_type("retracted publication") == "Correction/Retraction"
    assert _normalize_type("retraction") == "Correction/Retraction"
    assert _normalize_type("retraction notice") == "Correction/Retraction"
    assert _normalize_type("review") == "Article"
    assert _normalize_type("seminar") == "Other"
    assert _normalize_type("supplementary-materials") == "Other"
    assert _normalize_type("technicalReport") == "Other"
    assert _normalize_type("withdrawn publication") == "Correction/Retraction"
    assert _normalize_type("workingPaper") == "Other"
    assert _normalize_type("autobiography") == "Book"
    assert _normalize_type("clinical conference") == "Other"
    assert _normalize_type("clinical study") == "Other"
    assert _normalize_type("clinical trial") == "Other"
    assert _normalize_type("clinical trial protocol") == "Other"
    assert _normalize_type("clinical trial, phase i") == "Other"
    assert _normalize_type("clinical trial, phase ii") == "Other"
    assert _normalize_type("clinical trial, phase iii") == "Other"
    assert _normalize_type("clinical trial, phase iv") == "Other"
    assert _normalize_type("comment") == "Other"
    assert _normalize_type("comparative study") == "Other"
    assert _normalize_type("consensus development conference") == "Other"
    assert _normalize_type("consensus development conference, nih") == "Other"
    assert _normalize_type("controlled clinical trial") == "Other"
    assert _normalize_type("english abstract") == "Other"
    assert _normalize_type("equivalence trial") == "Other"
    assert _normalize_type("evaluation study") == "Other"
    assert _normalize_type("guideline") == "Other"
    assert _normalize_type("historical article") == "Article"
    assert _normalize_type("interactive tutorial") == "Other"
    assert _normalize_type("legal case") == "Other"
    assert _normalize_type("meta-analysis") == "Article"
    assert _normalize_type("multicenter study") == "Other"
    assert _normalize_type("network meta-analysis") == "Article"
    assert _normalize_type("observational study") == "Other"
    assert _normalize_type("overall") == "Other"
    assert _normalize_type("portrait") == "Other"
    assert _normalize_type("practice guideline") == "Other"
    assert _normalize_type("pragmatic clinical trial") == "Other"
    assert _normalize_type("randomized controlled trial") == "Other"
    assert (
        _normalize_type("research support, american recovery and reinvestment act")
        == "Other"
    )
    assert _normalize_type("research support, n.i.h., extramural") == "Other"
    assert _normalize_type("research support, n.i.h., intramural") == "Other"
    assert _normalize_type("research support, non-u.s. gov't") == "Other"
    assert _normalize_type("research support, u.s. gov't, non-p.h.s.") == "Other"
    assert _normalize_type("research support, u.s. gov't, p.h.s.") == "Other"
    assert _normalize_type("scoping review") == "Article"
    assert _normalize_type("systematic review") == "Article"
    assert _normalize_type("technical report") == "Other"
    assert _normalize_type("twin study") == "Other"
    assert _normalize_type("validation study") == "Other"
    assert _normalize_type("video-audio media") == "Other"
    assert _normalize_type("webcast") == "Other"

    # edge cases
    assert _normalize_type("awesome") == "Awesome", "no mapping"


def _pub(session, doi="10.1515/9781503624153"):
    return session.query(Publication).where(Publication.doi == doi).first()


def test_openalex_publisher_journal(test_session, snapshot):
    """
    Test that publisher is distilled from OpenAlex JSON first if available.
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json,
                dim_json=dim_json,
                openalex_json={
                    "title": "On the dangers of stochastic parrots (openalex)",
                    "primary_location": {
                        "source": {
                            "id": "https://openalex.org/S2764375719",
                            "display_name": "Not the journal name to use",
                            "issn_l": "0009-4978",
                            "issn": ["0009-4978", "1523-8253", "1943-5975"],
                            "host_organization": "https://openalex.org/P4310316146",
                            "host_organization_name": "Some Publisher",
                            "type": "journal",
                        }
                    },
                    "locations": [
                        {
                            "id": "doi:10.5860/choice.51-7042",
                            "source": {
                                "id": "https://openalex.org/S2764375719",
                                "display_name": "Real Journal Name",
                                "issn_l": "9999-9999",
                                "type": "journal",
                            },
                        },
                        {
                            "id": "pmh:oai:archive.org:I",
                            "source": {
                                "id": "https://openalex.org/S4377196541",
                                "display_name": "Internet Archive (Internet Archive)",
                                "issn_l": "8888-8888",
                                "type": "repository",
                            },
                        },
                    ],
                },
            )
        )

    distill(snapshot)

    assert _pub(session).publisher == "Some Publisher"
    assert _pub(session).journal_name == "Real Journal Name"


@pytest.fixture
def pubmed_json():
    return {
        "MedlineCitation": {
            "Article": {
                "ArticleTitle": "Example Article Title",
                "PublicationTypeList": {
                    "PublicationType": {"@UI": "D016428", "#text": "Journal Article"}
                },
                "Journal": {
                    "Title": "The Medical Journal",
                    "ISSN": {"#text": "1873-2054", "@IssnType": "Electronic"},
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


def test_pubmed_publisher_journal(test_session, snapshot, pubmed_json):
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                pubmed_json=pubmed_json,
                dim_json=None,
                openalex_json=None,
            )
        )

    distill(snapshot)
    pub = _pub(session)
    assert _journal_issn(pub) == "1873-2054"
    # will do live lookup in OpenAlex Sources API
    assert _pub(session).journal_name == "Health & Place"
    assert _pub(session).publisher == "Elsevier"


def test_dimensions_publisher_journal(test_session, snapshot):
    """
    Test that publisher is distilled from Dimensions JSON.
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                openalex_json=None,
                dim_json={
                    "type": "article",
                    "issn": "1476-4687",  # Nature
                },
            )
        )

    distill(snapshot)

    pub = _pub(session)
    assert _journal_issn(pub) == "1476-4687"
    # will do live lookup in OpenAlex Sources API
    assert pub.publisher == "Springer Nature"
    assert pub.journal_name == "Nature"


def test_author_based_fields(test_session, snapshot):
    """
    academic_council_authored should be true if any authors are academic council
    """
    with test_session.begin() as session:
        pub = Publication(
            doi="10.1515/9781503624153",
            openalex_json=openalex_json,
            dim_json=dim_json,
        )
        pub2 = Publication(
            doi="10.1515/0003",
            openalex_json=openalex_json,
            dim_json=dim_json,
        )
        author1 = Author(
            first_name="Jane",
            last_name="Stanford",
            sunet="janes",
            cap_profile_id="1234",
            orcid="0298098343",
            primary_school="School of Humanities and Sciences",
            primary_dept="Social Sciences",
            primary_role="faculty",
            schools=[
                "Vice Provost for Undergraduate Education",
                "School of Humanities and Sciences",
            ],
            departments=["Inter-Departmental Programs", "Social Sciences"],
            academic_council=True,
        )
        author2 = Author(
            first_name="Leland",
            last_name="Stanford",
            sunet="lelands",
            cap_profile_id="12345",
            orcid="02980983434",
            primary_school="School of Humanities and Sciences",
            primary_dept="Social Sciences",
            primary_role="staff",
            schools=[
                "School of Humanities and Sciences",
            ],
            departments=["Social Sciences"],
            academic_council=False,
        )
        pub.authors.append(author1)
        pub.authors.append(author2)
        pub2.authors.append(author2)
        session.add(pub)
        session.add(pub2)

    distill(snapshot)

    academic_pub = (
        session.query(Publication)
        .where(Publication.doi == "10.1515/9781503624153")
        .first()
    )
    assert academic_pub.academic_council_authored
    assert academic_pub.faculty_authored

    non_academic_pub = (
        session.query(Publication).where(Publication.doi == "10.1515/0003").first()
    )
    assert non_academic_pub.academic_council_authored is False
    assert non_academic_pub.faculty_authored is False


def test_journal_issn(test_session, snapshot):
    """
    Test that journal ISSN is extracted from multiple sources
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json=sulpub_json,  # 3333-3333
                dim_json=dim_json,  # 1111-1111
                openalex_json=openalex_json,  # 0009-4978, 1523-8253, 1943-5975
                crossref_json={
                    "ISSN": ["0000-0000", "1111-1111"],
                },
                pubmed_json={
                    "MedlineCitation": {
                        "Article": {
                            "Journal": {
                                "Title": "The Medical Journal",
                                "ISSN": {
                                    "#text": "4444-4444",
                                    "@IssnType": "Electronic",
                                },
                            },
                        }
                    },
                },
            )
        )

    distill(snapshot)

    # ISSNs extracted from multiple sources
    assert (
        _journal_issn(_pub(session))
        == "0000-0000|0009-4978|1111-1111|1523-8253|1943-5975|3333-3333|4444-4444"
    )


def test_null_issn(test_session):
    # Add a publication with fields only sourced from OpenAlex
    with test_session.begin() as session:
        pub = Publication(
            doi="10.000/some_doi",
            title="My OpenAlex Life",
            apc=123,
            open_access="gold",
            pub_year=2023,
            dim_json=None,
            openalex_json={
                "id": "https://openalex.org/W123456789",
                "biblio": {
                    "issue": "11",
                    "first_page": "1",
                    "last_page": "9",
                    "volume": "2",
                },
                "primary_location": {
                    "source": {
                        "type": "journal",
                        "display_name": "Ok Limes Journal of Science",
                        "host_organization_name": "Science Publisher Inc.",
                        "issn_l": None,
                        "issn": None,
                    }
                },
            },
            wos_json=None,
            sulpub_json=None,
            pubmed_json=None,
            crossref_json=None,
            types=["Article", "Preprint"],
        )
        session.add(pub)

        selected_pub = (
            session.query(Publication).filter_by(doi="10.000/some_doi").first()
        )
        assert _journal_issn(selected_pub) is None


def test_invalid_issn(test_session, snapshot):
    """
    Test that invalid ISSNs are ignored.
    """
    with test_session.begin() as session:
        session.add(
            Publication(
                doi="10.1515/9781503624153",
                sulpub_json={
                    "journal_issn": "",
                },
                dim_json={
                    "issn": 1,
                },
                crossref_json={
                    "ISSN": ["", "abcd-efgh", "12345678", "1234-0000"],
                },
            )
        )

    distill(snapshot)

    # no valid ISSNs
    assert _journal_issn(_pub(session)) == "1234-0000"
