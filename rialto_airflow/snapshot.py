import datetime
import json
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Snapshot:
    """
    A class representing a given snapshot and its associated data directory and
    database name.
    """

    path: Path
    timestamp: str
    database_name: str

    @classmethod
    def create(cls, data_dir: Path, database_name: str | None = None):
        """
        Create a Snapshot in the given root data directory. Optionally you can
        override the database name, which is really only useful in testing.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        path = data_dir / "snapshots" / timestamp
        path.mkdir(parents=True, exist_ok=True)

        database_name = database_name or f"rialto_{timestamp}"

        return cls(path, timestamp, database_name)

    @classmethod
    def get_latest(cls, data_dir: Path):
        """
        Returns the most recent completed Snapshot.
        """
        data_dir = data_dir

        snapshots_dir = data_dir / "snapshots"
        if not snapshots_dir.is_dir():
            return None

        snapshot_names = [p.name for p in snapshots_dir.iterdir()]

        for snapshot_name in reversed(sorted(snapshot_names)):
            # ignore things that might be here that aren't the right format
            if not re.match(r"^\d{14}$", snapshot_name):
                continue

            # if the snapshot.json file is there it is complete
            snapshot_path = snapshots_dir / snapshot_name
            if not (snapshot_path / "snapshot.json").is_file():
                continue

            # TODO: store the database name in snapshot.json?
            database_name = f"rialto_{snapshot_path.name}"

            return Snapshot(snapshot_path, snapshot_name, database_name)

        # no complete snapshot was found
        return None

    def complete(self) -> Path:
        """
        Mark a snapshot as having been completed.
        """
        if self.snapshot_file.is_file():
            raise Exception(f"can't complete already completed snapshot: {self.path}")

        with self.snapshot_file.open("w") as fh:
            # TODO: should we save other things about the harvest run here?
            json.dump({"finished": datetime.datetime.now().isoformat()}, fh)

        return self.snapshot_file

    def is_complete(self) -> bool:
        return self.snapshot_file.is_file()

    @property
    def snapshot_file(self) -> Path:
        """
        The path to the snapshot JSON file that is written when it is finished.
        """
        return self.path / "snapshot.json"

    @property
    def authors_csv(self) -> Path:
        """
        The path to the snapshots authors.csv file.
        """
        return self.path / "authors.csv"
