import json
from dataclasses import dataclass


def num_jsonl_objects(jsonl_path):
    assert jsonl_path.is_file()
    pubs = [json.loads(line) for line in jsonl_path.open("r")]
    return len(pubs)


def load_jsonl_file(path):
    """
    Load a jsonl file into memory and return it as a list of JSON objects.
    """
    result = []
    with open(path, "r") as f:
        for line in f:
            result.append(json.loads(line))
    return result


@dataclass
class TestRow:
    """
    An object that simulates a Publication database row.
    """

    dim_json: dict | None = None
    openalex_json: dict | None = None
    sulpub_json: dict | None = None
    wos_json: dict | None = None
    pubmed_json: dict | None = None
