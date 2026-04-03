import pytest
import re
import time

from pathlib import Path
from types import SimpleNamespace

from rialto_airflow.snapshot import Snapshot
from rialto_airflow.xcom import RialtoXCom


def test_create(tmp_path):
    """
    Make sure we can create a Snapshot given a data directory.
    """
    s = Snapshot.create(tmp_path)
    assert s.timestamp
    assert s.path.is_dir()
    assert s.path.is_relative_to(Path(tmp_path))
    assert s.authors_csv.is_relative_to(Path(tmp_path))
    assert re.match(r"^rialto_\d+$", s.database_name)


def test_complete(tmp_path):
    s = Snapshot.create(tmp_path)
    assert s.is_complete() is False

    s.complete()
    assert s.is_complete() is True

    # can't complete an already completed snapshot
    with pytest.raises(Exception, match=r"can't complete already completed.*"):
        s.complete()


def test_get_latest(tmp_path):
    assert Snapshot.get_latest(tmp_path) is None, "no snapshots"

    s1 = Snapshot.create(tmp_path)
    assert Snapshot.get_latest(tmp_path) is None, "uncompleted snapshot not returned"

    s1.complete()
    assert Snapshot.get_latest(tmp_path) == s1, "completed snapshot returned"

    # prevent duplicate since the timestamp granularity is in seconds
    time.sleep(1)

    s2 = Snapshot.create(tmp_path)
    assert Snapshot.get_latest(tmp_path) == s1, "returns the only completed snapshot"

    s2.complete()
    assert Snapshot.get_latest(tmp_path) == s2, "returns the new latests snaphost"


def test_not_snapshot(tmp_path):
    # create a snapshots directory with an incorrect name
    snapshot_file = tmp_path / "snapshots" / "non-snapshot" / "snapshot.json"
    snapshot_file.parent.mkdir(parents=True)

    # write a snapshot.json file to it so it looks like it is completed
    snapshot_file.open("w").write("{}")

    # ensure that it isn't found
    assert Snapshot.get_latest(tmp_path) is None


def test_serialize(tmp_path):
    """
    Make sure that Snapshot serializes to a dict and deserializes back correctly.
    This is needed to serialize the Path object in Airflow 3 which no longer has
    pickle serialization.
    """
    s1 = Snapshot.create(tmp_path)

    serialized = RialtoXCom.serialize_value(s1)
    s2 = RialtoXCom.deserialize_value(SimpleNamespace(value=serialized))

    assert s1 == s2
