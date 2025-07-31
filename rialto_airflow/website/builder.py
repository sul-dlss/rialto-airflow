"""
This module builds a static website for the current database using the 11ty
static site builder and the https://github.com/sul-dlss-labs/rialto-site
repository.
"""

from pathlib import Path
from subprocess import run

from typing import Optional

from rialto_airflow.website.data import write_data
from rialto_airflow.snapshot import Snapshot


def build(snapshot: Snapshot, rialto_site_dir: Optional[Path] = None) -> Path:
    if rialto_site_dir is None:
        rialto_site_dir = Path("rialto-site").absolute()
    else:
        rialto_site_dir = rialto_site_dir.absolute()

    # get or update the rialto-site git repo
    if not rialto_site_dir.is_dir():
        _checkout(rialto_site_dir)
    else:
        _pull(rialto_site_dir)

    # write data from the database to the static site
    write_data(snapshot, rialto_site_dir)

    # build the site and return the directory where it was written
    return _run_build(snapshot, rialto_site_dir)


def _checkout(rialto_site_dir) -> None:
    run(
        [
            "git",
            "clone",
            "https://github.com/sul-dlss-labs/rialto-site",
            rialto_site_dir,
        ],
        check=True,
    )


def _pull(rialto_site_dir) -> None:
    run(["git", "reset", "--hard"], cwd=rialto_site_dir, check=True)
    run(
        [
            "git",
            "pull",
        ],
        cwd=rialto_site_dir,
        check=True,
    )


def _run_build(snapshot, rialto_site_dir) -> Path:
    website_dir = snapshot.path / "website"
    run(["yarn", "install"], cwd=rialto_site_dir, check=True)
    run(
        ["yarn", "run", "eleventy", "--pathprefix", "site", "--output", website_dir],
        cwd=rialto_site_dir,
        check=True,
    )

    return website_dir
