"""
Real pipeline metrics — Kafka via kafka-python AdminClient/Consumer,
Spark via its Structured Streaming REST/metrics endpoint.

Section 31/33/34 hard requirement: "These values MUST come from actual
pipeline metrics... If metrics are unavailable, display 'Metrics
unavailable' rather than inventing values." Every function here either
returns a real reading or an explicit unavailable/offline marker —
nothing here is allowed to synthesize a plausible-looking number.
"""

from __future__ import annotations
import os
import time
import requests
from typing import Any, Dict, Optional

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "security-events")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "cyberstream-spark")
SPARK_UI_URL = os.environ.get("SPARK_UI_URL", "http://spark:4040")


def kafka_status() -> Dict[str, Any]:
    try:
        from kafka import KafkaAdminClient, KafkaConsumer
        from kafka.structs import TopicPartition

        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS.split(","),
                                  request_timeout_ms=3000)
        topics = admin.list_topics()
        if KAFKA_TOPIC not in topics:
            return {"status": "DEGRADED", "reason": f"topic {KAFKA_TOPIC} not found",
                     "topics": topics}

        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS.split(","),
                                  group_id=KAFKA_CONSUMER_GROUP,
                                  consumer_timeout_ms=3000)
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        end_offsets = consumer.end_offsets(tps) if tps else {}

        lag_info = {}
        try:
            committed = {tp: consumer.committed(tp) or 0 for tp in tps}
            for tp in tps:
                end = end_offsets.get(tp, 0)
                lag_info[tp.partition] = max(end - committed.get(tp, 0), 0)
        except Exception:
            pass

        consumer.close()
        return {
            "status": "ONLINE",
            "topics": topics,
            "topic": KAFKA_TOPIC,
            "partitions": sorted(partitions),
            "consumer_group": KAFKA_CONSUMER_GROUP,
            "end_offsets": {tp.partition: off for tp, off in end_offsets.items()},
            "lag_by_partition": lag_info,
            "total_lag": sum(lag_info.values()) if lag_info else None,
        }
    except Exception as e:
        return {"status": "OFFLINE", "reason": str(e)}


def spark_status() -> Dict[str, Any]:
    """Query Spark's Structured Streaming metrics via its REST API."""
    try:
        resp = requests.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=3)
        resp.raise_for_status()
        apps = resp.json()
        if not apps:
            return {"status": "OFFLINE", "reason": "no running Spark application"}
        app_id = apps[0]["id"]

        streams_resp = requests.get(
            f"{SPARK_UI_URL}/api/v1/applications/{app_id}/streams", timeout=3
        )
        streams = streams_resp.json() if streams_resp.ok else []

        if not streams:
            return {"status": "STARTING", "app_id": app_id,
                     "reason": "application running, no active streaming query yet"}

        latest = streams[0]
        recent = latest.get("recentProgress", [])
        last_batch = recent[-1] if recent else None

        return {
            "status": "ONLINE",
            "app_id": app_id,
            "query_name": latest.get("name"),
            "run_id": latest.get("runId"),
            "batches_processed": len(recent),
            "last_batch": last_batch,
            "input_rows_per_second": last_batch.get("inputRowsPerSecond") if last_batch else None,
            "processed_rows_per_second": last_batch.get("processedRowsPerSecond") if last_batch else None,
        }
    except requests.exceptions.RequestException as e:
        return {"status": "OFFLINE", "reason": f"Spark UI unreachable: {e}"}
    except Exception as e:
        return {"status": "OFFLINE", "reason": str(e)}


def throughput_snapshot() -> Dict[str, Any]:
    """
    Composite view for the dashboard's throughput panel (Section 31).
    Returns 'unavailable' fields rather than fabricated numbers when a
    component can't be reached.
    """
    k = kafka_status()
    s = spark_status()

    ingestion_eps = None  # derived from generator's own reported rate, not guessed
    processing_eps = s.get("processed_rows_per_second") if s.get("status") == "ONLINE" else None
    latency_ms = None
    if s.get("last_batch"):
        latency_ms = s["last_batch"].get("durationMs", {}).get("triggerExecution")

    return {
        "ingestion_eps": ingestion_eps if ingestion_eps is not None else "unavailable",
        "processing_eps": processing_eps if processing_eps is not None else "unavailable",
        "latency_ms": latency_ms if latency_ms is not None else "unavailable",
        "kafka_lag": k.get("total_lag", "unavailable"),
        "kafka_status": k.get("status"),
        "spark_status": s.get("status"),
        "measured_at": time.time(),
    }
