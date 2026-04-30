import datetime

from sqlalchemy import inspect

from rialto_airflow.schema.rialto import Harvest


def test_create_persists_harvest(test_incremental_session, mock_rialto_db_name):
    harvest = Harvest.create()

    assert harvest.id is not None
    assert inspect(harvest).detached is True

    with test_incremental_session.begin() as session:
        persisted = session.get(Harvest, harvest.id)
        assert persisted is not None
        assert persisted.finished_at is None
        assert persisted.created_at is not None


def test_get_by_id_returns_detached_harvest(
    test_incremental_session, mock_rialto_db_name
):
    with test_incremental_session.begin() as session:
        harvest = Harvest()
        session.add(harvest)
        session.flush()
        harvest_id = harvest.id

    fetched_harvest = Harvest.get_by_id(harvest_id)

    assert fetched_harvest is not None
    assert fetched_harvest.id == harvest_id
    assert inspect(fetched_harvest).detached is True


def test_get_previous_returns_latest_finished_harvest(
    test_incremental_session, mock_rialto_db_name
):
    with test_incremental_session.begin() as session:
        session.add(Harvest())
        older_finished = Harvest(
            finished_at=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        )
        newer_finished = Harvest(
            finished_at=datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
        )
        session.add_all([older_finished, newer_finished])
        session.flush()
        newer_finished_id = newer_finished.id

    previous = Harvest.get_previous()

    assert previous is not None
    assert previous.id == newer_finished_id
    assert inspect(previous).detached is True


def test_complete_sets_finished_at(test_incremental_session, mock_rialto_db_name):
    harvest = Harvest.create()

    harvest.complete()

    with test_incremental_session.begin() as session:
        completed = session.get(Harvest, harvest.id)
        assert completed is not None
        assert completed.finished_at is not None
