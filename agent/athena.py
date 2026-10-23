"""Minimal Athena client for the agent tools: parameterized queries, timeout, short-lived cache."""

import os
import time
from dataclasses import dataclass, field
from decimal import Decimal

import boto3

INT_TYPES = {"tinyint", "smallint", "integer", "bigint"}
FLOAT_TYPES = {"double", "float", "real", "decimal"}


class QueryError(RuntimeError):
    pass


def sql_literal(value: str | int) -> str:
    """Athena ExecutionParameters are substituted as SQL literals, so strings must be quoted.
    Values are already validated against allow-lists; quoting is the second line of defence."""
    if isinstance(value, bool):
        raise TypeError("booleans are not supported as parameters")
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


@dataclass
class AthenaRunner:
    workgroup: str
    database: str
    timeout_s: float = 30.0
    cache_ttl_s: float = 60.0
    client: object = field(default_factory=lambda: boto3.client(
        "athena", region_name=os.environ.get("AWS_REGION", "us-east-1")))
    _cache: dict = field(default_factory=dict)

    def run(self, sql: str, params: list[str | int]) -> dict:
        key = (sql, tuple(params))
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < self.cache_ttl_s:
            return hit[1]

        request = {
            "QueryString": sql,
            "QueryExecutionContext": {"Database": self.database},
            "WorkGroup": self.workgroup,
        }
        if params:
            request["ExecutionParameters"] = [sql_literal(p) for p in params]
        query_id = self.client.start_query_execution(**request)["QueryExecutionId"]

        deadline = time.monotonic() + self.timeout_s
        while True:
            execution = self.client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            state = execution["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                raise QueryError(execution["Status"].get("StateChangeReason", state))
            if time.monotonic() > deadline:
                self.client.stop_query_execution(QueryExecutionId=query_id)
                raise QueryError(f"query timed out after {self.timeout_s:.0f} s")
            time.sleep(0.5)

        page = self.client.get_query_results(QueryExecutionId=query_id, MaxResults=100)
        info = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        columns = [c["Name"] for c in info]
        rows = [[self._convert(cell.get("VarCharValue"), col["Type"]) for cell, col in zip(r["Data"], info)]
                for r in page["ResultSet"]["Rows"][1:]]        # first row is the header
        result = {
            "columns": columns,
            "rows": rows,
            "scanned_bytes": execution["Statistics"].get("DataScannedInBytes", 0),
        }
        self._cache[key] = (time.monotonic(), result)
        return result

    @staticmethod
    def _convert(value: str | None, athena_type: str):
        if value is None:
            return None
        if athena_type in INT_TYPES:
            return int(value)
        if athena_type in FLOAT_TYPES:
            return float(Decimal(value))
        return value
