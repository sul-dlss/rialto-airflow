import pytest

from rialto_airflow.schema.harvest import Publication
from rialto_airflow.distiller.types import types, _normalize_type


def test_types(caplog):
    """
    Test that only the types from the first match are returned.
    """

    # set up a publication with some initial type metadata where we would expect
    # to find it, and slowly pare the different platform metadata away to make
    # sure the rules are matching correctly.

    pub = Publication(
        doi="10.1515/9781503624153",
        dim_json={"type": "Book"},
        openalex_json={"type": "Chapter"},
        sulpub_json={"type": "Dissertation"},
        crossref_json={"type": "Dataset"},
        wos_json={
            "static_data": {
                "fullrecord_metadata": {"normalized_doctypes": {"doctype": "Article"}}
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

    # dimensions takes priority
    assert types(pub) == ["Book"]

    # openalex next
    pub.dim_json = {}
    assert types(pub) == ["Chapter"]

    # pubmed next
    pub.openalex_json = {}
    assert types(pub) == ["Article", "Preprint"]

    # web of science next
    pub.pubmed_json = {}
    assert types(pub) == ["Article"]

    # crossref next
    pub.wos_json = {}
    assert types(pub) == ["Dataset"]

    # sulpub next
    pub.crossref_json = {}
    assert types(pub) == ["Dissertation"]

    # unepected json shouldn't cause a problem
    pub.sulpub_json = {"foo": "bar"}
    assert types(pub) == []

    # no json would be weird, but shouldn't cause a problem w/ distill
    pub.sulpub_json = {}
    assert types(pub) == []

    # unexpected json throws an distiller exception
    pub.sulpub_json = {"type": {"foo": "bar"}}

    with pytest.raises(Exception) as e:
        types(pub)

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
