from dominate import document
from dominate.tags import style, div, ol, li
import pandas as pd


def validate_orcid_tableau(authors_df, orcid_integration_sheet_df):
    # Check the ORCID integration count for 04/23/2024
    orcid_integration_sheet_df["Date"] = pd.to_datetime(
        orcid_integration_sheet_df["Date"]
    )
    orcid_integration_count = orcid_integration_sheet_df.loc[
        orcid_integration_sheet_df["Date"]
        == pd.to_datetime("04/23/2024", format="%m/%d/%Y"),
        "Read Only Scope",
    ].squeeze()
    orcid_integration_count_str = f"The ORCID Integration count for April 23, 2024 should be {orcid_integration_count}."
    # Check the percent of faculty members with ORCIDs
    faculty_orcid_percent = round(
        (
            len(authors_df[authors_df.role == "faculty"].dropna(subset="orcidid"))
            / len(authors_df[authors_df.role == "faculty"])
            * 100
        ),
        1,
    )
    faculty_orcid_percent_str = (
        f"{faculty_orcid_percent}% of faculty members have ORCIDs."
    )
    # Check the percent of masters students without ORCIDs
    ms_students = authors_df[authors_df.role == "msstudent"]
    ms_student_no_orcid_percent = round(
        (ms_students["orcidid"].isna().sum() / len(ms_students) * 100), 1
    )
    ms_student_no_orcid_percent_str = (
        f"{ms_student_no_orcid_percent}% of masters students do not have ORCIDs."
    )
    # Check the percent of faculty members in the School of Engineering with an ORCID
    faculty = authors_df[authors_df.role == "faculty"]
    engineering_faculty_orcid_percent = round(
        (
            len(
                faculty[faculty.primary_school == "School of Engineering"].dropna(
                    subset="orcidid"
                )
            )
            / len(faculty[faculty.primary_school == "School of Engineering"])
            * 100
        ),
        1,
    )
    engineering_faculty_orcid_percent_str = f"{engineering_faculty_orcid_percent}% of faculty in the School of Engineering have an ORCID."
    # Check the percent of PhD students in SLAC, should be 0
    phd_students = authors_df[authors_df.role == "phdstudent"]
    slac_phd_student_orcid_count = len(
        phd_students[
            phd_students.primary_school == "SLAC National Accelerator Laboratory"
        ]
    )
    slac_phd_student_orcid_count_str = f"{slac_phd_student_orcid_count} PhD students at SLAC have an ORCID; there should be no PhD students at SLAC."
    # Check the percent of postdocs in the "Dermatology" department at the Medical School
    postdocs = authors_df[authors_df.role == "postdoc"]
    med_postdocs_student_orcid_percent = postdocs[
        postdocs.primary_school == "School of Medicine"
    ]
    hh_postdocs_student_orcid_percent = round(
        (
            len(
                med_postdocs_student_orcid_percent[
                    med_postdocs_student_orcid_percent.primary_department
                    == "Dermatology"
                ].dropna(subset="orcidid")
            )
            / len(
                med_postdocs_student_orcid_percent[
                    med_postdocs_student_orcid_percent.primary_department
                    == "Dermatology"
                ]
            )
            * 100
        ),
        1,
    )
    hh_postdocs_student_orcid_percent_str = f"{hh_postdocs_student_orcid_percent}% of postdocs in the Dermatology department at the Medical School have an ORCID."

    return [
        orcid_integration_count_str,
        faculty_orcid_percent_str,
        ms_student_no_orcid_percent_str,
        engineering_faculty_orcid_percent_str,
        slac_phd_student_orcid_count_str,
        hh_postdocs_student_orcid_percent_str,
    ]


def validation_report(title: str, expected_calculations: list):
    doc = document(title=title)

    with doc.head:
        style(
            """\
         body {
              font-family: sans-serif;
              margin: 3em 1em;
         }
         h1 {
              text-align: center;
         }
          .column {
              flex: 50%;
          }
         .report {
              border: 1px solid black;
              margin: 10px 25px 10px;
              padding: 5px 10px 5px;
         }
         .row {
              display: flex;
         }
     """
        )

    with doc:
        with div(id="header").add(ol()):
            for i in expected_calculations:
                li(i)

    return doc.render()
