import os
from fastapi import APIRouter
from shared.detection import THRESHOLDS, DETECTOR_RULE_NAMES
from shared.windowing import WINDOW_SECONDS, CORRELATION_WINDOW_SECONDS
from shared.scoring import RISK_WEIGHTS, SEVERITY_BANDS

router = APIRouter(prefix="/api", tags=["settings"])


@router.get("/settings")
def get_settings():
    """
    Section 40/56: a read-only view of the environment-driven
    configuration actually in effect for this running stack. This is
    intentionally read-only rather than a live control panel — changing
    Kafka partition counts or retention windows requires restarting the
    affected containers, which this API does not attempt to orchestrate.
    """
    return {
        "kafka": {
            "brokers": os.environ.get("KAFKA_BROKERS", "kafka:29092"),
            "topic": os.environ.get("KAFKA_TOPIC", "security-events"),
            "consumer_group": os.environ.get("KAFKA_CONSUMER_GROUP", "cyberstream-spark"),
        },
        "spark": {
            "shuffle_partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"),
        },
        "retention_days": {
            "raw": int(os.environ.get("RETENTION_RAW_DAYS", 7)),
            "processed": int(os.environ.get("RETENTION_PROCESSED_DAYS", 14)),
            "alerts": int(os.environ.get("RETENTION_ALERTS_DAYS", 30)),
        },
        "storage_paths": {
            "processed": os.environ.get("PROCESSED_PATH", "/data/processed"),
            "alerts": os.environ.get("ALERTS_PATH", "/data/alerts"),
            "incidents": os.environ.get("INCIDENTS_PATH", "/data/incidents"),
            "dlq": os.environ.get("DLQ_PATH", "/data/dlq"),
        },
    }


RULE_THRESHOLD_MAP = {
    "brute_force": THRESHOLDS["brute_force_failed_logins"],
    "password_spraying": THRESHOLDS["password_spray_distinct_accounts"],
    "port_scan": THRESHOLDS["port_scan_distinct_ports"],
    "data_transfer_anomaly": THRESHOLDS["data_transfer_anomaly_bytes"],
    "dns_anomaly": THRESHOLDS["dns_query_burst"],
}


@router.get("/detection-rules")
def get_detection_rules():
    """
    Section 40's 'Detection Rules' page + Section 54's educational panel
    content — the actual thresholds and window sizes in effect, read
    directly from shared/detection.py and shared/windowing.py rather
    than a separate hand-maintained description that could drift from
    the real logic.
    """
    rules = []
    for rule_name in set(DETECTOR_RULE_NAMES.values()):
        rules.append({
            "rule": rule_name,
            "window_seconds": WINDOW_SECONDS.get(rule_name),
            "threshold": RULE_THRESHOLD_MAP.get(rule_name),
            "risk_weight": RISK_WEIGHTS.get(rule_name),
        })
    return {
        "rules": sorted(rules, key=lambda r: r["rule"]),
        "correlation_window_seconds": CORRELATION_WINDOW_SECONDS,
        "severity_bands": [{"min": lo, "max": hi, "label": label}
                            for lo, hi, label in SEVERITY_BANDS],
    }


@router.get("/bigdata-lab/concepts")
def bigdata_concepts():
    """Section 54's educational panel content, served so the frontend
    doesn't hardcode prose that could drift from the actual architecture."""
    return {
        "volume_velocity_variety": {
            "volume": "The raw quantity of security telemetry — CyberStream's "
                       "generator can produce from 100K to 50M+ synthetic events.",
            "velocity": "The rate events arrive and must be processed — the Load "
                        "Lab lets you push this from hundreds to tens of thousands "
                        "of events/sec and watch Kafka lag respond.",
            "variety": "Six structurally different event categories (auth, network, "
                        "endpoint, privilege, cloud, web) collapsed into one "
                        "normalized schema before detection runs.",
        },
        "concepts": {
            "kafka": "Distributed event streaming — decouples producers from consumers.",
            "partition": "The unit of parallelism in Kafka; more partitions allow more "
                          "concurrent consumers within a group.",
            "consumer_group": "A named set of consumers that split a topic's partitions "
                               "among themselves for parallel consumption.",
            "spark": "A distributed processing engine; here, driving Structured Streaming.",
            "structured_streaming": "Spark's DataFrame-based API for continuous, "
                                     "incremental processing of unbounded data.",
            "window": "A time-bounded bucket events are grouped into before aggregation "
                       "or detection — see /api/detection-rules for this system's window sizes.",
            "watermark": "A bound on how late an event can arrive and still be included "
                         "in its window's results, preventing unbounded state growth.",
            "backpressure": "When ingestion outpaces processing capacity, causing a "
                            "growing backlog — observable here as rising Kafka lag.",
            "throughput": "Events processed per second.",
            "latency": "Time between an event's ingestion and its processed result.",
        },
    }
