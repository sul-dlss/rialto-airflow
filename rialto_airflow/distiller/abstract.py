from .utils import JsonPathRule, FuncRule, first, json_path


def abstract(row):
    """
    Get the abstract from openalex, dimensions, pubmed then crossref.
    """
    return first(
        row,
        rules=[
            FuncRule("openalex_json", _rebuild_abstract),
            JsonPathRule("dim_json", "abstract"),
            FuncRule("pubmed_json", _pubmed_abstract),
            JsonPathRule("crossref_json", "abstract"),
        ],
    )


def _pubmed_abstract(pubmed_json: dict) -> str | None:
    """
    Get the abstract from PubMed JSON.
    """
    if pubmed_json is None:
        return None

    full_abstract = []
    jsonp = json_path("MedlineCitation.Article.Abstract.AbstractText[*]")
    abstract_text = jsonp.find(pubmed_json)
    if abstract_text:
        for abstract in abstract_text:
            # sometimes the abstract is a string and not a dict of text segments
            if isinstance(abstract.value, str):
                full_abstract.append(abstract.value)
            else:
                full_abstract.append(abstract.value.get("#text", None))
        # remove any None or empty-string segments before joining
        full_abstract = [
            text
            for text in full_abstract
            if text is not None and str(text).strip() != ""
        ]
        return " ".join(full_abstract)
    return None


def _rebuild_abstract(openalex_json: dict) -> str | None:
    """
    Rebuilds an abstract from a positional inverted index.
    """

    # openalex metadata isn't always defined
    if openalex_json is None:
        return None

    # guard against the abstract data being missing
    inverted_index = openalex_json.get("abstract_inverted_index")
    if inverted_index is None:
        return None

    # Create a list to hold the words in their correct positions.
    # We find the max position to make sure the list is large enough.
    max_position = 0
    for positions in inverted_index.values():
        if positions:
            max_position = max(max_position, max(positions))

    # The list is zero-indexed, so we need max_position + 1 length.
    abstract_words = [""] * (max_position + 1)

    # Place each word in its correct position.
    for word, positions in inverted_index.items():
        for position in positions:
            abstract_words[position] = word

    # Join the words to form the final abstract.
    return " ".join(abstract_words)
