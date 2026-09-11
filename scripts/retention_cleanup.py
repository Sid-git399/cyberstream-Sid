#!/usr/bin/env python3
"""
Data Retention Enforcement (Section 39).

Deletes files/rows older than the configured retention window for each
storage tier. Configurable via the SAME environment variables documented
in .env.example (RETENTION_RAW_DAYS, RETENTION_PROCESSED_DAYS,
RETENTION_ALERTS_DAYS) — this script and the Settings API
(/api/settings) read the identical variables, so what the dashboard
displays as "configured retention" is exactly what this job enforces.

Run manually:
    python3 scripts/retention_cleanup.py

Or continuously (see the `retention` service in docker-compose.yml),
which just calls this on a loop with a sleep interval.
"""
from __future__ import annotations
import glob
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

RAW_PATH = os.environ.get("RAW_PATH", "/data/raw")
PROCESSED_PATH = os.environ.get("PROCESSED_PATH", "/data/processed")
DLQ_PATH = os.environ.get("DLQ_PATH", "/data/dlq")

RETENTION_RAW_DAYS = int(os.environ.get("RETENTION_RAW_DAYS", 7))
RETENTION_PROCESSED_DAYS = int(os.environ.get("RETENTION_PROCESSED_DAYS", 14))
RETENTION_ALERTS_DAYS = int(os.environ.get("RETENTION_ALERTS_DAYS", 30))

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://cyberstream:cyberstream@postgres:5432/cyberstream"
)


def _delete_files_older_than(path_glob: str, days: int) -> int:
    cutoff = time.time() - days * 86400
    deleted = 0
    for fp in glob.glob(path_glob, recursive=True):
        try:
            if os.path.isfile(fp) and os.path.getmtime(fp) < cutoff:
                os.remove(fp)
                deleted += 1
        except OSError:
            continue
    return deleted


def enforce_raw_retention() -> int:
    return _delete_files_older_than(f"{RAW_PATH}/**/*", RETENTION_RAW_DAYS)


def enforce_processed_retention() -> int:
    n = _delete_files_older_than(f"{PROCESSED_PATH}/**/*.parquet", RETENTION_PROCESSED_DAYS)
    n += _delete_files_older_than(f"{DLQ_PATH}/**/*.json", RETENTION_PROCESSED_DAYS)
    return n


def enforce_alert_retention() -> int:
    """Deletes alerts/incidents older than RETENTION_ALERTS_DAYS from Postgres."""
    try:
        import psycopg
    except ImportError:
        print("[retention] psycopg not installed — skipping Postgres retention "
              "(expected when running this script outside the backend container)")
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_ALERTS_DAYS)
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM alerts WHERE created_at < %s", (cutoff,))
            deleted_alerts = cur.rowcount
            cur.execute("DELETE FROM incidents WHERE created_at < %s", (cutoff,))
            deleted_incidents = cur.rowcount
            conn.commit()
        return deleted_alerts + deleted_incidents
    except Exception as e:
        print(f"[retention] Postgres unreachable, skipping alert retention: {e}")
        return 0


def run_once():
    raw_deleted = enforce_raw_retention()
    processed_deleted = enforce_processed_retention()
    alert_rows_deleted = enforce_alert_retention()
    print(f"[retention] raw files deleted: {raw_deleted} (>{RETENTION_RAW_DAYS}d), "
          f"processed/dlq files deleted: {processed_deleted} (>{RETENTION_PROCESSED_DAYS}d), "
          f"alert/incident rows deleted: {alert_rows_deleted} (>{RETENTION_ALERTS_DAYS}d)")


if __name__ == "__main__":
    if os.environ.get("RETENTION_LOOP", "false").lower() == "true":
        interval = int(os.environ.get("RETENTION_INTERVAL_SECONDS", 3600))
        while True:
            run_once()
            time.sleep(interval)
    else:
        run_once()
