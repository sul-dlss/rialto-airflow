from .utils import JsonPathRule, first


def issue(row) -> str | None:
    value = first(
        row,
        rules=[
            JsonPathRule("openalex_json", "biblio.issue"),
            JsonPathRule("dim_json", "issue"),
            JsonPathRule(
                "pubmed_json", "MedlineCitation.Article.Journal.JournalIssue.Issue"
            ),
            JsonPathRule("sulpub_json", "journal.issue"),
        ],
    )

    match value:
        case list():
            return value[0] if len(value) > 0 else None
        case str():
            return value
        case _:
            return None
