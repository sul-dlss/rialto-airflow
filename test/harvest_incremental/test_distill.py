import pytest

from rialto_airflow.harvest_incremental import distill as distill_mod
from rialto_airflow.harvest_incremental.distill import distill
from rialto_airflow.schema.rialto import Publication

# This simply tests that the distill function is working. For the detailed testing of distill rules see
# the test/distill directory.


@pytest.fixture
def mock_rialto_db_name(monkeypatch):
    monkeypatch.setattr(distill_mod, "RIALTO_DB_NAME", "rialto_incremental_test")


def test_distill(
    test_incremental_session,
    dataset_incremental,
    snapshot_incremental,
    mock_rialto_db_name,
):
    with test_incremental_session.begin() as session:
        distill(snapshot_incremental)

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
