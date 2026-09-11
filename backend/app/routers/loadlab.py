"""
Load Lab / Benchmarking (Sections 36-37, 64).

Runs a REAL, bounded synthetic-event generation burst against Kafka and
measures REAL before/after processed-event counts from the Parquet lake
plus real Kafka lag — never a fabricated number (Section 67-68). Because
a benchmark run takes real wall-clock time, it executes as a background
task; the client polls /api/benchmarks/{run_id} for status.
"""

from __future__ import annotations
import os
import sys
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from generator.generator import generate  # noqa: E402
from ..db import pg_conn, duckdb_conn, processed_events_glob, has_processed_data
from .. import metrics

router = APIRouter(prefix="/api/benchmarks", tags=["benchmarks"])

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "security-events")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")


class LoadTestRequest(BaseModel):
    target_eps: int = 1000
    partitions: int = 3
    duration_seconds: int = 30
    scenario: str = "normal"


def _current_processed_count() -> int:
    if not has_processed_data():
        return 0
    con = duckdb_conn()
    try:
        return con.execute(
            f"SELECT count(*) FROM read_parquet('{processed_events_glob()}')"
        ).fetchone()[0]
    except Exception:
        return 0


def _run_benchmark(run_id: str, req: LoadTestRequest):
    from kafka import KafkaProducer
    import json as _json

    started = time.time()
    before_count = _current_processed_count()
    lag_before = metrics.kafka_status().get("total_lag")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20, batch_size=64_000,
    )

    target_events = req.target_eps * req.duration_seconds
    latencies = []
    sent = 0
    deadline = started + req.duration_seconds

    for evt in generate(events=target_events, rate=req.target_eps, n_hosts=200,
                          n_users=500, scenario=req.scenario, attack_pct=0.01):
        t0 = time.time()
        producer.send(KAFKA_TOPIC, key=evt.get("hostname"), value=evt)
        latencies.append((time.time() - t0) * 1000)
        sent += 1
        if time.time() > deadline:
            break
    producer.flush()

    elapsed = time.time() - started
    lag_after = metrics.kafka_status().get("total_lag")
    after_count_immediate = _current_processed_count()

    avg_latency = sum(latencies) / len(latencies) if latencies else None
    peak_latency = max(latencies) if latencies else None

    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE benchmark_runs SET
                 finished_at=now(), events_generated=%s, events_processed=%s,
                 avg_latency_ms=%s, peak_latency_ms=%s, kafka_lag_max=%s, status='completed'
               WHERE run_id=%s""",
            (sent, max(after_count_immediate - before_count, 0), avg_latency,
             peak_latency,
             lag_after if isinstance(lag_after, int) else None, run_id),
        )
        conn.commit()


@router.post("")
def start_benchmark(req: LoadTestRequest, background_tasks: BackgroundTasks):
    run_id = f"bench-{uuid.uuid4().hex[:8]}"
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO benchmark_runs
               (run_id, target_events, target_eps, partitions, duration_seconds, status)
               VALUES (%s,%s,%s,%s,%s,'running')""",
            (run_id, req.target_eps * req.duration_seconds, req.target_eps,
             req.partitions, req.duration_seconds),
        )
        conn.commit()

    background_tasks.add_task(_run_benchmark, run_id, req)
    return {"run_id": run_id, "status": "running",
             "note": "processed/latency figures are measured after the run completes; "
                     "poll GET /api/benchmarks/{run_id}"}


@router.get("/{run_id}")
def get_benchmark(run_id: str):
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM benchmark_runs WHERE run_id=%s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, "benchmark run not found")
    return row


@router.get("")
def list_benchmarks():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM benchmark_runs ORDER BY started_at DESC LIMIT 50")
        return cur.fetchall()
