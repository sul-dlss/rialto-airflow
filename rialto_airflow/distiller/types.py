from .utils import JsonPathRule, FuncRule, first, json_path


def types(pub) -> list[str]:
    types = first(
        pub,
        rules=[
            JsonPathRule("dim_json", "type"),
            JsonPathRule("openalex_json", "type"),
            FuncRule("pubmed_json", _pubmed_type),
            JsonPathRule(
                "wos_json",
                "static_data.fullrecord_metadata.normalized_doctypes.doctype",
            ),
            JsonPathRule("crossref_json", "type"),
            JsonPathRule("sulpub_json", "type"),
        ],
    )

    if types is None:
        return []

    if isinstance(types, str):
        types = [types]

    elif not isinstance(types, list):
        raise Exception(
            f"types distill rules generated unexpected result: {type(types)}"
        )

    return sorted(set([_normalize_type(s) for s in types if s is not None]))


def _pubmed_type(pubmed_json: dict) -> list[str]:
    """
    PubMed publication types can point to a dictionary or a list of dictionaries.
    """
    types = []
    jsonp = json_path("MedlineCitation.Article.PublicationTypeList.PublicationType[*]")
    for pub_type in jsonp.find(pubmed_json):
        types.append(pub_type.value.get("#text"))

    return types


def _normalize_type(s: str) -> str:
    return type_mapping.get(s.lower(), s.capitalize())


type_mapping = {
    "autobiography": "Book",
    "book": "Book",
    "book-chapter": "Chapter",
    "book-part": "Chapter",
    "book-section": "Chapter",
    "book-series": "Other",
    "book-set": "Other",
    "clinical conference": "Other",
    "clinical study": "Other",
    "clinical trial": "Other",
    "clinical trial protocol": "Other",
    "clinical trial, phase i": "Other",
    "clinical trial, phase ii": "Other",
    "clinical trial, phase iii": "Other",
    "clinical trial, phase iv": "Other",
    "comment": "Other",
    "component": "Other",
    "comparative study": "Other",
    "consensus development conference": "Other",
    "consensus development conference, nih": "Other",
    "controlled clinical trial": "Other",
    "database": "Other",
    "dataset": "Dataset",
    "dissertation": "Dissertation",
    "edited-book": "Book",
    "english abstract": "Other",
    "equivalence trial": "Other",
    "evaluation study": "Other",
    "guideline": "Other",
    "historical article": "Article",
    "interactive tutorial": "Other",
    "journal": "Other",
    "journal article": "Article",
    "journal-article": "Article",
    "journal-issue": "Other",
    "legal case": "Other",
    "meta-analysis": "Article",
    "monograph": "Book",
    "multicenter study": "Other",
    "network meta-analysis": "Article",
    "observational study": "Other",
    "other": "Other",
    "overall": "Other",
    "portrait": "Other",
    "posted-content": "Other",
    "practice guideline": "Other",
    "pragmatic clinical trial": "Other",
    "proceedings": "Other",
    "proceedings-article": "Article",
    "randomized controlled trial": "Other",
    "reference-book": "Other",
    "reference-entry": "Other",
    "report": "Other",
    "report-component": "Other",
    "report-series": "Other",
    "research support, american recovery and reinvestment act": "Other",
    "research support, n.i.h., extramural": "Other",
    "research support, n.i.h., intramural": "Other",
    "research support, non-u.s. gov't": "Other",
    "research support, u.s. gov't, non-p.h.s.": "Other",
    "research support, u.s. gov't, p.h.s.": "Other",
    "scoping review": "Article",
    "standard": "Other",
    "systematic review": "Article",
    "technical report": "Other",
    "twin study": "Other",
    "validation study": "Other",
    "video-audio media": "Other",
    "webcast": "Other",
    "abstract": "Other",
    "address": "Other",
    "art and literature": "Other",
    "article": "Article",
    "bibliography": "Other",
    "biography": "Book",
    "case reports": "Other",
    "casestudy": "Other",
    "chapter": "Chapter",
    "congress": "Other",
    "correction": "Correction/Retraction",
    "data paper": "Article",
    "data set": "Dataset",
    "data study": "Other",
    "dictionary": "Other",
    "early access": "Article",
    "editorial": "Editorial Material ",
    "editorial material": "Editorial Material ",
    "erratum": "Correction/Retraction",
    "expression of concern": "Correction/Retraction",
    "festschrift": "Book",
    "inbook": "Chapter",
    "inproceedings": "Article",
    "interview": "Other",
    "introductory journal article": "Other",
    "item withdrawal": "Correction/Retraction",
    "lecture": "Other",
    "letter": "Other",
    "libguides": "Other",
    "meeting": "Other",
    "news": "Other",
    "otherpaper": "Other",
    "paratext": "Other",
    "patient education handout": "Other",
    "peer-review": "Other",
    "personal narrative": "Other",
    "preprint": "Preprint",
    "proceeding": "Article",
    "publication with expression of concern": "Correction/Retraction",
    "published erratum": "Correction/Retraction",
    "retracted publication": "Correction/Retraction",
    "retraction": "Correction/Retraction",
    "retraction notice": "Correction/Retraction",
    "review": "Article",
    "seminar": "Other",
    "supplementary-materials": "Other",
    "technicalreport": "Other",
    "withdrawn publication": "Correction/Retraction",
    "workingpaper": "Other",
}
