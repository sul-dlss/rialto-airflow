import io
import pickle
import pytest
import re
import time

from pathlib import Path
from rialto_airflow.snapshot import Snapshot


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
    Make sure that Snapshot is serializable as a pickle file since we want to be
    able to use it in XComs with AIRFLOW__CORE__ENABLE_XCOM_PICKLING set.
    Otherwise the Snapshot would need to be JSON serializable.
    """
    s1 = Snapshot.create(tmp_path)

    out = io.BytesIO()
    pickle.dump(s1, out)

    out.seek(0)
    s2 = pickle.load(out)
    assert s1 == s2
