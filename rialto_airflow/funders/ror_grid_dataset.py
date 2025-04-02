from pathlib import Path
from functools import cache

import pandas


@cache
def convert_grid_to_ror(grid: str) -> str:
    return grid_to_ror.get(grid, None)


@cache
def convert_ror_to_grid(ror: str) -> str:
    return ror_to_grid.get(ror, None)


# read in the ror-grid mapping dataset
dataset_path = Path(__file__).parent / "2021-09-23-ror-grid-equivalents.csv"
df = pandas.read_csv(dataset_path)

# create a dictionary mapping of ror to grid id
ror_to_grid = df.set_index("rorId").to_dict()["gridId"]

# create a dictionary mapping of grid_id to ror
grid_to_ror = df.set_index("gridId").to_dict()["rorId"]
