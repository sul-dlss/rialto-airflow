import io
import pickle

from rialto_airflow.snapshot import Snapshot


def test_snapshot(tmpdir):
    """
    Make sure we can create a Snapshot given a data directory.
    """
    s = Snapshot(tmpdir)
    assert s.time
    assert s.path.is_dir()
    assert s.database_name == f"rialto_{s.time}"


def test_serialize(tmpdir):
    """
    Make sure that Snapshot is serializable as a pickle file since we want to be
    able to use it in XComs with AIRFLOW__CORE__ENABLE_XCOM_PICKLING set.
    Otherwise the Snapshot would need to be JSON serializable.
    """
    s1 = Snapshot(tmpdir)

    out = io.BytesIO()
    pickle.dump(s1, out)

    out.seek(0)
    s2 = pickle.load(out)
    assert s1 == s2
