"""
Ingestor — bridges Spark's file-based sinks into Postgres.

streaming/spark_job.py writes alerts and incidents as JSON files (one
append per micro-batch) because driving Postgres from inside a Spark
executor would require bundling a JDBC driver and managing connection
pooling from JVM-side Python workers — needless complexity for a lab
platform. Instead, the backend runs a lightweight background poller that
reads newly-appended JSON files and upserts their rows into Postgres,
using each record's own primary key (alert_id / incident_id) so re-
reading a file twice is a no-op, not a duplicate.

DLQ entries have no natural unique key from Spark's output, so those are
tracked by (file path, byte offset already read) instead, to avoid
double-inserting on restart.
"""

from __future__ import annotations
import glob
import json
import os
import time
from typing import Set

from .db import pg_conn

ALERTS_PATH = os.environ.get("ALERTS_PATH", "/data/alerts")
INCIDENTS_PATH = os.environ.get("INCIDENTS_PATH", "/data/incidents")
DLQ_PATH = os.environ.get("DLQ_PATH", "/data/dlq")
INGEST_INTERVAL_SECONDS = int(os.environ.get("INGEST_INTERVAL_SECONDS", "5"))

_dlq_offsets: dict[str, int] = {}


def _read_json_lines(path_glob: str):
    for fp in sorted(glob.glob(path_glob, recursive=True)):
        try:
            with open(fp) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield fp, json.loads(line)
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            continue


def ingest_alerts() -> int:
    n = 0
    rows = list(_read_json_lines(f"{ALERTS_PATH}/**/*.json"))
    if not rows:
        return 0
    with pg_conn() as conn, conn.cursor() as cur:
        for _, a in rows:
            cur.execute(
                """INSERT INTO alerts
                     (alert_id, title, detection_rule, severity, risk_score,
                      score_breakdown, source_ip, destination_ip, hostname,
                      username, evidence, mitre_technique, occurrences,
                      total_underlying_events, status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (alert_id) DO UPDATE SET
                     occurrences = EXCLUDED.occurrences,
                     total_underlying_events = EXCLUDED.total_underlying_events""",
                (a.get("alert_id"), a.get("title"), a.get("detection_rule"),
                 a.get("severity"), a.get("risk_score"),
                 json.dumps(a.get("score_breakdown")), a.get("source_ip"),
                 a.get("destination_ip"), a.get("hostname"), a.get("username"),
                 json.dumps(a.get("evidence")), json.dumps(a.get("mitre_technique")),
                 a.get("occurrences", 1), a.get("total_underlying_events", 1),
                 a.get("status", "NEW")),
            )
            n += 1
        conn.commit()
    return n


def ingest_incidents() -> int:
    n = 0
    rows = list(_read_json_lines(f"{INCIDENTS_PATH}/**/*.json"))
    if not rows:
        return 0
    with pg_conn() as conn, conn.cursor() as cur:
        for _, inc in rows:
            cur.execute(
                """INSERT INTO incidents (incident_id, entity, stage_count, stages, max_severity)
                   VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (incident_id) DO NOTHING""",
                (inc.get("incident_id"), inc.get("entity"), inc.get("stage_count"),
                 json.dumps(inc.get("stages")), inc.get("max_severity")),
            )
            n += 1
        conn.commit()
    return n


def ingest_dlq() -> int:
    n = 0
    with pg_conn() as conn, conn.cursor() as cur:
        for fp in sorted(glob.glob(f"{DLQ_PATH}/**/*.json", recursive=True)):
            seen = _dlq_offsets.get(fp, 0)
            try:
                with open(fp) as f:
                    f.seek(seen)
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        cur.execute(
                            """INSERT INTO dlq_entries (error_reason, original_event)
                               VALUES (%s, %s)""",
                            (entry.get("error_reason"), json.dumps(entry.get("original_event"))),
                        )
                        n += 1
                    _dlq_offsets[fp] = f.tell()
            except FileNotFoundError:
                continue
        conn.commit()
    return n


def run_ingest_cycle() -> dict:
    return {
        "alerts_ingested": ingest_alerts(),
        "incidents_ingested": ingest_incidents(),
        "dlq_ingested": ingest_dlq(),
        "at": time.time(),
    }
