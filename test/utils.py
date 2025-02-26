import json


def num_jsonl_objects(jsonl_path):
    assert jsonl_path.is_file()
    pubs = [json.loads(line) for line in jsonl_path.open("r")]
    return len(pubs)
