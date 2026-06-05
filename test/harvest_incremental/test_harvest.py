import pytest
from datetime import datetime, timezone

from sqlalchemy import inspect

from rialto_airflow.schema.rialto import Harvest


def test_create_persists_harvest(test_incremental_session):
    harvest = Harvest.create()

    assert harvest.id is not None
    assert inspect(harvest).detached is True

    with test_incremental_session.begin() as session:
        persisted = session.get(Harvest, harvest.id)
        assert persisted is not None
        assert persisted.finished_at is None
        assert persisted.created_at is not None
        assert persisted.is_full is False


def test_get_by_id_returns_detached_harvest(test_incremental_session):
    with test_incremental_session.begin() as session:
        harvest = Harvest()
        session.add(harvest)
        session.flush()
        harvest_id = harvest.id

    fetched_harvest = Harvest.get_by_id(harvest_id)

    assert fetched_harvest is not None
    assert fetched_harvest.id == harvest_id
    assert fetched_harvest.is_full is False
    assert inspect(fetched_harvest).detached is True


def test_get_previous(test_incremental_session):

    # set up three harvests, that started in order
    with test_incremental_session.begin() as session:
        h1 = Harvest(
            created_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
            finished_at=datetime(2024, 1, 1, 4, tzinfo=timezone.utc),
        )
        h2 = Harvest(
            created_at=datetime(2024, 1, 8, 0, tzinfo=timezone.utc),
            finished_at=datetime(2024, 1, 8, 4, tzinfo=timezone.utc),
        )
        h3 = Harvest(
            created_at=datetime(2024, 1, 15, 0, tzinfo=timezone.utc),
        )

        session.add_all([h1, h2, h3])
        session.flush()

        h1_id = h1.id
        h2_id = h2.id
        h3_id = h3.id

    harvest = Harvest.get_by_id(h1_id)
    previous = harvest.get_previous()
    assert previous is None

    harvest = Harvest.get_by_id(h2_id)
    previous = harvest.get_previous()
    assert previous is not None
    assert previous.id == h1_id
    assert inspect(previous).detached is True

    harvest = Harvest.get_by_id(h3_id)
    previous = harvest.get_previous()
    assert previous is not None
    assert previous.id == h2_id
    assert inspect(previous).detached is True


def test_get_previous_for_full_harvest(test_incremental_session):
    """
    Full harvests never have a previous harvest, only incremental ones do. This
    allows harvesting logic to not limit the scope of collected metadata.
    """

    # set up three harvests, that started in order
    with test_incremental_session.begin() as session:
        h1 = Harvest(
            created_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
            finished_at=datetime(2024, 1, 1, 4, tzinfo=timezone.utc),
        )
        h2 = Harvest(
            created_at=datetime(2024, 1, 8, 0, tzinfo=timezone.utc),
            finished_at=datetime(2024, 1, 8, 4, tzinfo=timezone.utc),
            is_full=True,
        )
        session.add_all([h1, h2])
        session.flush()
        harvest_id = h2.id

    harvest = Harvest.get_by_id(harvest_id)
    assert harvest is not None

    previous = harvest.get_previous()
    assert previous is None


def test_complete(test_incremental_session):
    harvest = Harvest.create()

    harvest.complete()

    with test_incremental_session.begin() as session:
        completed = session.get(Harvest, harvest.id)
        assert completed is not None
        assert completed.finished_at is not None


def test_get_by_id_raises_for_missing_harvest(test_incremental_session):
    with pytest.raises(ValueError, match="999999"):
        Harvest.get_by_id(999999)


def test_complete_raises_value_error_for_missing_harvest(test_incremental_session):
    harvest = Harvest(id=999999)
    with pytest.raises(ValueError, match="Harvest 999999 not found"):
        harvest.complete()
