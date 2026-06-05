import datetime
import time
from rialto_airflow.harvest_incremental.distill import distill
from rialto_airflow.schema.rialto import Publication

# This simply tests that the distill function is working. For the detailed testing of distill rules see
# the test/distill directory.


def test_distill(test_incremental_session, dataset_incremental):
    with test_incremental_session.begin() as session:
        distill()

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
        assert pub.distilled_at is not None


def test_academic_council_authored():
    from rialto_airflow.harvest_incremental.distill import _academic_council
    from rialto_airflow.schema.rialto import Publication, Author

    pub = Publication()
    author1 = Author(first_name="A", last_name="B", academic_council=False)
    author2 = Author(first_name="C", last_name="D", academic_council=False)
    pub.authors = [author1, author2]

    # Test return False
    assert _academic_council(pub) is False

    # Test return True on second author (covers branch 80->79)
    author2.academic_council = True
    assert _academic_council(pub) is True


def test_faculty_authored():
    from rialto_airflow.harvest_incremental.distill import _faculty_authored
    from rialto_airflow.schema.rialto import Publication, Author

    pub = Publication()
    author1 = Author(first_name="A", last_name="B", role="staff")
    pub.authors = [author1]
    assert _faculty_authored(pub) is False

    author2 = Author(first_name="C", last_name="D", role="faculty")
    pub.authors.append(author2)
    assert _faculty_authored(pub) is True


def test_distill_conditional(test_incremental_session, dataset_incremental):
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
