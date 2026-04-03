from pathlib import Path

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
    def deserialize_value(xcom):
        result = BaseXCom.deserialize_value(xcom)
        if isinstance(result, dict) and result.get("__snapshot__"):
            return Snapshot(
                path=Path(result["path"]),
                timestamp=result["timestamp"],
                database_name=result["database_name"],
            )
        return result
