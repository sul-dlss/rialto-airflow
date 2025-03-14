import re
from pathlib import Path
from functools import cache

import pandas


@cache
def is_federal(name: str) -> bool:
    name = _normalize_name(name)
    return name_to_grid_id.get(name) is not None


@cache
def is_federal_grid_id(grid_id: str) -> bool:
    return grid_id_to_name.get(grid_id) is not None


@cache
def _normalize_name(name: str) -> str:
    # remove parenthetical acronym
    name = re.sub(r" ?\(.+\)$", "", name)
    return name.lower()


# read in the federal funders dataset
dataset_path = Path(__file__).parent / "239_US_Federal_Funders_for_filter_with_ror.csv"
df = pandas.read_csv(dataset_path, encoding="ISO-8859-1")

# remove parenthethical acronyms
df["Name"] = df["Name"].apply(_normalize_name)

# create a dictionary mapping of name to grid id
name_to_grid_id = df.set_index("Name").to_dict()["ID"]

# create a dictionary mapping of grid_id to name
grid_id_to_name = df.set_index("ID").to_dict()["Name"]
