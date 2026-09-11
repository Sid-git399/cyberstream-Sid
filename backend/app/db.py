"""
Data access layer.

Two storage engines, matching Section 38:
  - Postgres  -> operational metadata (alerts, incidents, benchmark runs,
                 pipeline metric snapshots).
  - Parquet   -> large analytical event storage (data/processed/, data/dlq/),
                 queried with DuckDB so the Event Explorer can filter and
                 paginate millions of rows without ever loading them into
                 the API process, let alone the browser (Sections 28, 44).
"""

from __future__ import annotations
import os
import duckdb
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager

PG_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://cyberstream:cyberstream@postgres:5432/cyberstream",
)
PROCESSED_PATH = os.environ.get("PROCESSED_PATH", "/data/processed")
ALERTS_JSON_PATH = os.environ.get("ALERTS_PATH", "/data/alerts")
DLQ_PATH = os.environ.get("DLQ_PATH", "/data/dlq")


@contextmanager
def pg_conn():
    conn = psycopg.connect(PG_DSN, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def duckdb_conn():
    """Fresh in-memory DuckDB connection with the Parquet lake registered."""
    con = duckdb.connect(database=":memory:")
    return con


def processed_events_glob() -> str:
    return f"{PROCESSED_PATH}/**/*.parquet"


def has_processed_data() -> bool:
    import glob
    return len(glob.glob(processed_events_glob(), recursive=True)) > 0
