import os
import pandas as pd
import pytest
import csv

from rialto_airflow import validate


@pytest.fixture
def orcid_integration_csv(tmp_path):
    # Create a fixture ORCID stats CSV file
    fixture_file = tmp_path / "orcid-integration-stats.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Date", "Read Only Scope", "Read/Write Scope"])
        writer.writerow(["01/30/2022", "455", "4110"])
        writer.writerow(["02/28/2023", "775", "4121"])
        writer.writerow(["04/23/2024", "806", "6009"])
        writer.writerow(["08/30/2024", "856", "4121"])
    return fixture_file


def test_validation_report(orcid_integration_csv):
    base_dir = os.path.dirname(__file__)
    data_dir = os.path.join(base_dir, "data")

    authors_path = os.path.join(data_dir, "authors-data-quality.csv")

    authors_df = pd.read_csv(authors_path)
    orcid_integration_sheet_df = pd.read_csv(orcid_integration_csv)

    assert (
        " ".join(
            validate.validation_report(
                "ORCID Validation Report",
                validate.validate_orcid_tableau(authors_df, orcid_integration_sheet_df),
            ).split()
        )
        == """<!DOCTYPE html> <html> <head> <title>ORCID Validation Report</title> <style> body { font-family: sans-serif; margin: 3em 1em; } h1 { text-align: center; } .column { flex: 50%; } .report { border: 1px solid black; margin: 10px 25px 10px; padding: 5px 10px 5px; } .row { display: flex; } </style> </head> <body> <div id="header"> <ol> <li>The ORCID Integration count for April 23, 2024 should be 806.</li> <li>100.0% of faculty members have ORCIDs.</li> <li>0.0% of masters students do not have ORCIDs.</li> <li>100.0% of faculty in the School of Engineering have an ORCID.</li> <li>0 PhD students at SLAC have an ORCID; there should be no PhD students at SLAC.</li> <li>100.0% of postdocs in the Dermatology department at the Medical School have an ORCID.</li> </ol> </div> </body> </html>"""
    )
