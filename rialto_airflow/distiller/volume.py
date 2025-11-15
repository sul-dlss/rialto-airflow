from .utils import JsonPathRule, first


def volume(row) -> str | None:
    vol = first(
        row,
        rules=[
            JsonPathRule("openalex_json", "biblio.volume"),
            JsonPathRule("dim_json", "volume"),
            JsonPathRule(
                "pubmed_json", "MedlineCitation.Article.Journal.JournalIssue.Volume"
            ),
            JsonPathRule("sulpub_json", "journal.volume"),
        ],
    )

    match vol:
        case list():
            return vol[0] if len(vol) > 0 else None
        case str():
            return vol
        case _:
            return None
