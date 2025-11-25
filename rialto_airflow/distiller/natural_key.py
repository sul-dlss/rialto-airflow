import re

from rialto_airflow.distiller.title import title
from rialto_airflow.distiller.author_names import first_author_name
from rialto_airflow.distiller.pub_year import pub_year


def natural_key(pub):
    title_str = title(pub)
    first_author_str = first_author_name(pub)
    pub_year_int = pub_year(pub)

    # we need these to generate a natural key for the publication
    if title_str is None or first_author_str is None or pub_year_int is None:
        return None

    parts = [title_str, first_author_str, str(pub_year_int)]

    parts = list(map(_normalize, parts))

    return ":".join(parts)


def _normalize(s):
    """
    Remove punctuation, whitespace, and lowercase.
    """
    if s is None:
        return None
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r" ", "", s)
    s = s.lower()
    return s
