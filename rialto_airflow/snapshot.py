import datetime
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Snapshot:
    """
    A class representing a given snapshot and its associated data directory and
    database name.
    """

    path: Path
    time: str
    database_name: str

    def __init__(self, path, database_name=None):
        now = datetime.datetime.now()
        self.time = now.strftime("%Y%m%d%H%M%S")

        self.path = Path(path) / "snapshots" / self.time
        self.path.mkdir(parents=True)

        self.database_name = database_name or f"rialto_{self.time}"
