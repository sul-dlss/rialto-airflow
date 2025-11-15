from sqlalchemy import select

from rialto_airflow.publish import publication
from rialto_airflow.schema.reports import PublicationsByAuthor


def test_write_publications_by_author(test_reports_session, snapshot, dataset, caplog):
    result = publication.export_publications_by_author(snapshot)
    assert result == 5

    with test_reports_session.begin() as session:
        rows = session.execute(
            select(PublicationsByAuthor).order_by(
                PublicationsByAuthor.doi, PublicationsByAuthor.sunet
            )
        ).all()
        assert len(rows) == 5

        row = rows[0][0]
        assert bool(row.academic_council) is False
        assert row.abstract == "This is an abstract which is inverted."
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.apc == 123
        assert row.citation_count == 100
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert (
            row.journal_name
            == "Proceedings of the National Academy of Sciences of the United States of America"
        )
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        # note: unlike the other first/last_author_orcid (which come from fixture publication JSON)
        # this orcid value comes from Author database model, which is different
        assert row.orcid == "02980983422"
        assert row.pages == "1-9"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Mechanical Engineering"
        assert row.publisher == "Science Publisher Inc."
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "folms"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[1][0]
        assert bool(row.academic_council) is True
        assert row.apc == 123
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.citation_count == 100
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        assert row.orcid == "029809834222"
        assert row.primary_school == "School of Engineering"
        assert row.primary_department == "Electrical Engineering"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "fterm"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[2][0]
        assert bool(row.academic_council) is True
        assert row.apc == 123
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        assert row.orcid == "0298098343"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "faculty"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "janes"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[3][0]
        assert bool(row.academic_council) is False
        assert row.apc == 123
        assert row.author_list_names == "Jane Stanford|Leland Stanford"
        assert row.author_list_orcids == "0000-0003-1111-2222|0000-0004-3333-4444"
        assert row.doi == "10.000/000001"
        assert bool(row.federally_funded) is True
        assert row.first_author_name == "Jane Stanford"
        assert row.first_author_orcid == "0000-0003-1111-2222"
        assert row.issue == "11"
        assert row.last_author_name == "Leland Stanford"
        assert row.last_author_orcid == "0000-0004-3333-4444"
        assert row.open_access == "gold"
        assert row.orcid == "02980983434"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "staff"
        assert row.pub_year == 2023
        assert row.types == "Article|Preprint"
        assert row.sunet == "lelands"
        assert row.title == "My Life"
        assert row.volume == "2"

        row = rows[4][0]
        assert bool(row.academic_council) is True
        assert row.apc == 500
        assert row.doi == "10.000/000002"
        assert bool(row.federally_funded) is True
        assert row.open_access == "green"
        assert row.orcid == "0298098343"
        assert row.primary_school == "School of Humanities and Sciences"
        assert row.primary_department == "Social Sciences"
        assert row.role == "faculty"
        assert row.pages == "1-10"
        assert row.pub_year == 2024
        assert row.types == "Article|Preprint"
        assert row.sunet == "janes"
        assert row.title == "My Life Part 2"

        assert "started writing publications_by_author table" in caplog.text
        assert (
            "finished writing 5 rows to the publications_by_author table" in caplog.text
        )
