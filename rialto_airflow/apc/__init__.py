import logging
from functools import cache
from pathlib import Path
from typing import Optional

import pandas


# load the dataset into memory for use
dataset_path = Path(__file__).parent / "APCdataset-annualAPCs_Published-v1.txt"
df = pandas.read_csv(dataset_path, delimiter="\t", encoding="ISO-8859-1")


@cache
def get_apc(issn: str, year: int) -> Optional[int]:
    matches = df[
        ((df.ISSN_1 == issn) | (df.ISSN_2 == issn))
        & (df.APC_year == year)
        & df.APC_USD.notna()
    ]
    if len(matches) >= 1:
        if len(matches) > 1:
            logging.warning(f"more than one APC match for {issn} and {year}")
        apc_value = int(matches.iloc[0].APC_USD)
        return apc_value if apc_value >= 0 else None
    else:
        return None
