import json
import logging


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


def is_matching_log_record(
    r: logging.LogRecord, levelno: int, renderd_msg_fragment: str
) -> bool:
    return r.levelno == levelno and renderd_msg_fragment in r.getMessage()


def num_log_record_matches(
    records: list[logging.LogRecord], levelno: int, renderd_msg_fragment: str
) -> int:
    return len(
        [r for r in records if is_matching_log_record(r, levelno, renderd_msg_fragment)]
    )
