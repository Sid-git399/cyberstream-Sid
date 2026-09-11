from fastapi import APIRouter
from .. import metrics
from ..db import pg_conn, has_processed_data
from shared.mitre import MITRE_TECHNIQUES

router = APIRouter(prefix="/api", tags=["pipeline"])


@router.get("/pipeline/health")
def pipeline_health():
    """Section 32 — real component health, no fake ONLINE status (Section 46)."""
    kafka = metrics.kafka_status()
    spark = metrics.spark_status()

    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            db_status = "ONLINE"
    except Exception as e:
        db_status = "OFFLINE"

    return {
        "kafka": {"status": kafka.get("status"), "detail": kafka},
        "spark": {"status": spark.get("status"), "detail": spark},
        "database": {"status": db_status},
        "storage": {"status": "ONLINE" if has_processed_data() else "STARTING"},
    }


@router.get("/kafka/status")
def kafka_status():
    return metrics.kafka_status()


@router.get("/spark/status")
def spark_status():
    return metrics.spark_status()


@router.get("/pipeline/throughput")
def throughput():
    return metrics.throughput_snapshot()


@router.get("/mitre")
def mitre_matrix():
    return {"techniques": MITRE_TECHNIQUES}


@router.get("/overview")
def overview():
    """Section 72 — answers the main dashboard questions in one call."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT severity, count(*) AS n FROM alerts GROUP BY severity")
        severity_counts = {r["severity"]: r["n"] for r in cur.fetchall()}

        cur.execute(
            """SELECT source_ip, count(*) AS n FROM alerts
               WHERE source_ip IS NOT NULL GROUP BY source_ip
               ORDER BY n DESC LIMIT 5"""
        )
        top_sources = cur.fetchall()

        cur.execute(
            """SELECT hostname, count(*) AS n FROM alerts
               WHERE hostname IS NOT NULL GROUP BY hostname
               ORDER BY n DESC LIMIT 5"""
        )
        top_hosts = cur.fetchall()

        cur.execute(
            """SELECT detection_rule, count(*) AS n FROM alerts
               GROUP BY detection_rule ORDER BY n DESC LIMIT 10"""
        )
        top_rules = cur.fetchall()

        cur.execute(
            """SELECT * FROM alerts ORDER BY risk_score DESC, created_at DESC LIMIT 5"""
        )
        top_incidents = cur.fetchall()

    throughput = metrics.throughput_snapshot()

    return {
        "severity_distribution": severity_counts,
        "top_source_ips": top_sources,
        "top_targeted_hosts": top_hosts,
        "top_detection_rules": top_rules,
        "top_severity_alerts": top_incidents,
        "throughput": throughput,
    }
