import logging
from functools import cache
from pathlib import Path

import pandas

# load the dataset into memory for use, limited to the columns get_apc() needs
# since the full 30 column frame is ~85MB in every worker process
dataset_path = Path(__file__).parent / "scholcommlab_apc_dataset_2019_2025.csv"
df = pandas.read_csv(
    dataset_path, usecols=["issn1", "issn2", "issn_l", "apc_year", "apc_usd"]
)


@cache
def get_apc(issn: str, year: int) -> int | None:
    matches = df[
        ((df.issn1 == issn) | (df.issn2 == issn) | (df.issn_l == issn))
        & (df.apc_year == year)
        & df.apc_usd.notna()
    ]
    if len(matches) >= 1:
        if len(matches) > 1:
            logging.warning(f"more than one APC match for {issn} and {year}")
        apc_value = int(matches.iloc[0].apc_usd)
        return apc_value if apc_value >= 0 else None
    else:
        return None
