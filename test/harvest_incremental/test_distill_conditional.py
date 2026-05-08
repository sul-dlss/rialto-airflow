import datetime
import time
from rialto_airflow.harvest_incremental.distill import distill
from rialto_airflow.schema.rialto import Publication


def test_distill_conditional(
    test_incremental_session,
    dataset_incremental,
    mock_rialto_db_name,
):
    with test_incremental_session.begin() as session:
        # 1. Initial distillation
        distilled_count = distill()
        assert distilled_count == 1

        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        assert pub.distilled_at is not None
        first_distilled_at = pub.distilled_at

        # 2. Second distillation without any changes (should skip)
        distilled_count = distill()
        assert distilled_count == 0

        session.refresh(pub)
        assert pub.distilled_at == first_distilled_at

        # 3. Harvest update (set dim_harvested to now)
        # Wait a tiny bit to ensure the timestamp is different if the system is very fast
        time.sleep(0.1)
        pub.dim_harvested = datetime.datetime.now(datetime.timezone.utc)
        session.add(pub)
        session.commit()

    # New session to avoid caching issues
    with test_incremental_session.begin() as session:
        # 4. Third distillation after harvest update (should distill)
        distilled_count = distill()
        assert distilled_count == 1

        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        assert pub.distilled_at > first_distilled_at
        second_distilled_at = pub.distilled_at

        # 5. Reset capability: set distilled_at to NULL
        pub.distilled_at = None
        session.add(pub)
        session.commit()

    with test_incremental_session.begin() as session:
        # 6. Fourth distillation after reset (should distill)
        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        assert pub.distilled_at is None

        distilled_count = distill()
        assert distilled_count == 1

        pub = (
            session.query(Publication).where(Publication.doi == "10.000/000001").first()
        )
        session.refresh(pub)
        assert pub.distilled_at is not None
        assert pub.distilled_at > second_distilled_at
