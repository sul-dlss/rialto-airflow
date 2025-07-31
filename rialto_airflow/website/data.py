"""
Generate data for Open Access reports.
"""

import json
from pathlib import Path

import pandas

from rialto_airflow.database import get_engine
from rialto_airflow.snapshot import Snapshot


OA_COLORS = {
    "bronze":        "#B96400",
    "gold":          "#FFC800",
    "closed":        "#566271",
    "green":         "#7DC896",
    "hybrid":        "#5064FA",
    "diamond":       "#C2EEEE",
    None: "#C8D0D9",
}


def write_data(snapshot: Snapshot, rialto_site_dir: Path):
    js_dir = rialto_site_dir / "data"
    if not js_dir.is_dir():
        js_dir.mkdir()

    write_json(publication_counts(snapshot), js_dir / "publications.json")

    write_json(openaccess(snapshot), js_dir / "openaccess.json")


def write_json(data, json_path):
    json.dump(data, json_path.open("w"), indent=2)


def publication_counts(snapshot: Snapshot) -> list[dict]:
    engine = get_engine(snapshot.database_name)
    with engine.connect() as conn:
        sql = """
            SELECT
                pub_year,
                COUNT(*) AS "pub_count"
            FROM publication
            WHERE publication.pub_year >= 2018
            GROUP BY pub_year
            ORDER BY pub_year
            """

        df = pandas.read_sql(sql, con=conn.connection)

        return [
            {
                "x": list(df.pub_year), 
                "y": list(df.pub_count), 
                "type": "bar",
                "hovertext": "none"
            }
        ]


def openaccess(snapshot) -> list[dict]:
    engine = get_engine(snapshot.database_name)
    with engine.connect() as conn:
        sql = """
            SELECT
                pub_year,
                open_access,
                COUNT(*) AS "pub_count"
            FROM publication
            WHERE
                publication.pub_year >= 2018
            GROUP BY pub_year, open_access
            ORDER BY pub_year
            """

        df = pandas.read_sql(sql, con=conn.connection)

        years = list(map(int, sorted(df.pub_year.unique())))

        data = []
        for oa in [None, 'closed', 'bronze', 'green', 'hybrid', 'gold', 'diamond']:

            # get counts by year for the open_access type
            if oa is not None:
                counts = [
                    df.loc[(df.pub_year == y) & (df.open_access == oa), "pub_count"]
                    for y in years
                ]
            else:
                # when open_access is None we need to look up the counts with isnull()
                counts = [
                    df.loc[(df.pub_year == y) & (df.open_access.isnull()), "pub_count"]
                    for y in years
                ]

            # extract the first row in series if it's there otherwise there were no results
            counts = [int(c.values[0]) if len(c) > 0 else 0 for c in counts]

            # convert open_access None to "not available" for the graph
            oa_label = oa if oa is not None else "not available"

            data.append(
                {
                    "x": years,
                    "y": counts,
                    "name": oa_label,
                    "type": "bar",
                    "text": [str(count) for count in counts],
                    "textposition": "bottom",
                    "hovertemplate": (
                        f"Open Access Category: <b>{oa_label}</b><br>"
                        "Publication Count:          <b>%{y}</b><br>"
                        "Publication Year:            <b>%{x}</b>"
                    ),
                    "marker": {"color": OA_COLORS[oa]},
                }
            )

        return data
