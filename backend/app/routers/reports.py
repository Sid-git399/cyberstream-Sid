from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from datetime import datetime, timezone
import json

from ..db import pg_conn
from .. import metrics

router = APIRouter(prefix="/api/reports", tags=["reports"])


def _build_report_dict() -> dict:
    """
    Assembles a Big Data pipeline report strictly from measured sources —
    Postgres (alerts/incidents/benchmarks/DLQ) and live Kafka/Spark
    metrics. Section 64 explicitly forbids fabricating benchmark values;
    every figure below traces back to a real table or a real API call.
    """
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM alerts")
        total_alerts = cur.fetchone()["n"]

        cur.execute("SELECT severity, count(*) AS n FROM alerts GROUP BY severity")
        severity_dist = {r["severity"]: r["n"] for r in cur.fetchall()}

        cur.execute("SELECT detection_rule, count(*) AS n FROM alerts "
                     "GROUP BY detection_rule ORDER BY n DESC")
        detection_dist = cur.fetchall()

        cur.execute("SELECT count(*) AS n FROM incidents")
        total_incidents = cur.fetchone()["n"]

        cur.execute("SELECT count(*) AS n FROM dlq_entries")
        total_dlq = cur.fetchone()["n"]

        cur.execute("SELECT * FROM benchmark_runs ORDER BY started_at DESC LIMIT 10")
        recent_benchmarks = cur.fetchall()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "kafka": metrics.kafka_status(),
        "spark": metrics.spark_status(),
        "throughput": metrics.throughput_snapshot(),
        "alerts": {"total": total_alerts, "severity_distribution": severity_dist,
                    "detection_distribution": detection_dist},
        "incidents": {"total": total_incidents},
        "dead_letter_queue": {"total": total_dlq},
        "recent_benchmarks": recent_benchmarks,
    }


@router.get("")
def export_report_json():
    """Machine-readable report (Section 64)."""
    return _build_report_dict()


@router.get("/text", response_class=PlainTextResponse)
def export_report_text():
    """Human-readable plaintext report, suitable for a quick download/paste."""
    r = _build_report_dict()
    lines = [
        "CYBERSTREAM PIPELINE REPORT",
        f"Generated: {r['generated_at']}",
        "=" * 60,
        "",
        f"Kafka status: {r['kafka'].get('status')}",
        f"Spark status: {r['spark'].get('status')}",
        f"Ingestion (eps): {r['throughput'].get('ingestion_eps')}",
        f"Processing (eps): {r['throughput'].get('processing_eps')}",
        f"Latency (ms): {r['throughput'].get('latency_ms')}",
        f"Kafka lag: {r['throughput'].get('kafka_lag')}",
        "",
        f"Total alerts: {r['alerts']['total']}",
        f"Severity distribution: {json.dumps(r['alerts']['severity_distribution'])}",
        "Detection rule distribution:",
    ]
    for row in r["alerts"]["detection_distribution"]:
        lines.append(f"  {row['detection_rule']}: {row['n']}")
    lines += [
        "",
        f"Total correlated incidents: {r['incidents']['total']}",
        f"Total dead-letter entries: {r['dead_letter_queue']['total']}",
        "",
        "Recent benchmark runs:",
    ]
    for b in r["recent_benchmarks"]:
        lines.append(
            f"  {b['run_id']}: target={b.get('target_eps')} eps x {b.get('duration_seconds')}s "
            f"-> generated={b.get('events_generated')}, processed={b.get('events_processed')}, "
            f"avg_latency={b.get('avg_latency_ms')}ms, status={b.get('status')}"
        )
    lines.append("\n(All figures above are read directly from Postgres/Kafka/Spark — none are fabricated.)")
    return "\n".join(lines)
