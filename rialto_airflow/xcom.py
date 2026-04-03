from pathlib import Path
from typing import Any

from airflow.sdk.bases.xcom import BaseXCom

from rialto_airflow.snapshot import Snapshot


class RialtoXCom(BaseXCom):
    @staticmethod
    def serialize_value(value, **kwargs):
        if isinstance(value, Snapshot):
            value = {
                "__snapshot__": True,
                "path": str(value.path),
                "timestamp": value.timestamp,
                "database_name": value.database_name,
            }
        return BaseXCom.serialize_value(value, **kwargs)

    @staticmethod
    def deserialize_value(result) -> Any:
        value = BaseXCom.deserialize_value(result)
        if isinstance(value, dict) and value.get("__snapshot__"):
            return Snapshot(
                path=Path(value["path"]),
                timestamp=value["timestamp"],
                database_name=value["database_name"],
            )
        return value
