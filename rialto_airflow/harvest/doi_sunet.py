import logging
import pickle
from collections import defaultdict

# TODO: use polars instead?
import pandas as pd

from rialto_airflow.utils import normalize_doi, normalize_orcid


def create_doi_sunet_pickle(
    dimensions: str, openalex: str, sul_pub_csv: str, authors_csv: str, output_path
) -> dict:
    """
    Get DOIs from each source and determine their SUNETID(s) using the authors
    csv file. Write the resulting mapping as a pickle to the output_path.
    """
    # use the authors csv to generate two dictionaries for looking up the sunet
    # based on an orcid or a cap_profile
    orcid_sunet, cap_profile_sunet = get_author_maps(authors_csv)

    # dimensions and openalex pickle files map doi -> [orcid] and use the
    # orcid_sunet mapping to turn that into doi -> [sunet]
    dimensions_map = doi_sunetids(dimensions, orcid_sunet)
    openalex_map = doi_sunetids(openalex, orcid_sunet)

    # sulpub csv has doi and authorship columns the latter of which contains the cap_profile_id so
    # the cap_profile_sunet mapping can be used to return a mapping of doi -> [sunet]
    sulpub_map = sulpub_doi_sunetids(sul_pub_csv, cap_profile_sunet)

    doi_sunet = combine_maps(dimensions_map, openalex_map, sulpub_map)

    with open(output_path, "wb") as handle:
        pickle.dump(doi_sunet, handle, protocol=pickle.HIGHEST_PROTOCOL)

    logging.info(f"Found {len(doi_sunet)} DOIs")


def doi_sunetids(pickle_file: str, orcid_sunet: dict) -> dict:
    """
    Convert a mapping of doi -> [orcid] to a mapping of doi -> [sunet].
    """
    doi_orcids = pickle.load(open(pickle_file, "rb"))

    mapping = {}
    for doi, orcids in doi_orcids.items():
        doi = normalize_doi(doi)
        mapping[doi] = [orcid_sunet[orcid] for orcid in orcids]

    return mapping


def sulpub_doi_sunetids(sul_pub_csv, cap_profile_sunet):
    # create a dataframe for sul_pub which has a column for cap_profile_id
    # extracted from the authorship column
    df = pd.read_csv(sul_pub_csv, usecols=["doi", "authorship"])
    df = df[df["doi"].notna()]
    df["doi"] = df["doi"].apply(lambda doi: normalize_doi(doi))

    def extract_cap_ids(authors):
        return [a["cap_profile_id"] for a in eval(authors) if a["status"] == "approved"]

    df["cap_profile_id"] = df["authorship"].apply(extract_cap_ids)

    df = df.explode("cap_profile_id")

    # create a column for sunet using the cap_profile_sunet dictionary
    df["sunet"] = df["cap_profile_id"].apply(
        lambda cap_id: cap_profile_sunet.get(cap_id)
    )

    # NOTE: the sunet could be None if the cap_profile id isn't in the authors.csv
    # log these so we get a sense of how much that happens
    missing = df[df["sunet"].isna()]["doi"].values
    if len(missing) > 0:
        logging.warn(
            f"found {len(missing)} DOI that have cap_profile_id missing from authors.csv: {','.join(missing)}."
        )

    return df.groupby("doi")["sunet"].apply(list).to_dict()


def get_author_maps(authors):
    """
    Reads the authors csv and returns two dictionary mappings: orcid -> sunet,
    cap_profile_id -> sunet.
    """
    df = pd.read_csv(authors, usecols=["sunetid", "orcidid", "cap_profile_id"])
    df["orcidid"] = df["orcidid"].apply(orcid_id)

    # orcid -> sunet
    orcid = pd.Series(df["sunetid"].values, index=df["orcidid"]).to_dict()

    # cap_profile_id -> sunet
    cap_profile_id = pd.Series(
        df["sunetid"].values, index=df["cap_profile_id"]
    ).to_dict()

    return orcid, cap_profile_id


def combine_maps(m1, m2, m3):
    m = defaultdict(set)

    # fold values from dictionary d2 into dictionary d1
    def combine(d1, d2):
        for doi, sunets in d2.items():
            for sunet in sunets:
                d1[doi].add(sunet)

    combine(m, m1)
    combine(m, m2)
    combine(m, m3)

    # return the mapping with the sets turned into lists
    return {k: list(v) for k, v in m.items()}


def orcid_id(orcid):
    if pd.isna(orcid):
        return None
    else:
        return normalize_orcid(orcid)
