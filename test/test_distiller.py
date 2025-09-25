from dataclasses import dataclass

import pytest

from rialto_airflow.distiller import FuncRule, JsonPathRule, first


@dataclass
class Publication:
    """
    An object that simulates a Publication database row.
    """

    dim_json: dict | None = None
    openalex_json: dict | None = None
    sulpub_json: dict | None = None
    wos_json: dict | None = None
    pubmed_json: dict | None = None


def test_jsonpath_rule():
    pub = Publication(sulpub_json={"authors": [{"name": "Leland"}]})

    result = first(pub, rules=[JsonPathRule("sulpub_json", "authors[*].name")])

    assert result == "Leland"


def test_func_rule():
    pub = Publication(sulpub_json={"a": 1})

    result = first(pub, rules=[FuncRule("sulpub_json", lambda d: d["a"] + 1)])

    assert result == 2


def test_exception():
    pub = Publication(sulpub_json={"a": 1})

    with pytest.raises(Exception) as e:
        first(pub, rules=["x"])  # type: ignore

    assert str(e.value) == "Rule must be JsonPathRule or FuncRule"
