from rialto_airflow.harvest.distill import distill
from rialto_airflow.schema.harvest import Publication

# This simply tests that the distill function is working. For the detailed testing of distill rules see
# the test/distill directory.


def test_distill(test_session, dataset, snapshot):
    with test_session.begin() as session:
        distill(snapshot)

        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        assert pub.title == "Sometimes limes are ok"
        assert pub.pub_year == 2023
        assert pub.apc == 123
        assert pub.open_access == "gold"
        assert pub.types == ["Article"]
        assert pub.publisher == "Science Publisher Inc."
        assert (
            pub.journal_name
            == "Proceedings of the National Academy of Sciences of the United States of America"
        )
        assert pub.academic_council_authored is True
        assert pub.faculty_authored is True
