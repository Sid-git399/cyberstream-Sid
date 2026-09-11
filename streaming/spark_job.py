"""
CyberStream Spark Structured Streaming Job
============================================
Real Spark Structured Streaming pipeline (Section 14).

    Kafka topic "security-events"
        -> parse JSON + schema validation
        -> normalize (dead-letter invalid events)     [Section 15]
        -> enrich                                       [Section 16]
        -> windowed detection (foreachBatch)             [Section 17-19]
        -> risk scoring + alert dedup                   [Section 21, 26-27]
        -> sink: Parquet (processed/) + Postgres (alerts) [Section 38]

Run (inside the `spark` container, or via spark-submit locally):

    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      streaming/spark_job.py

Why Structured Streaming instead of raw DStreams (documented further in
docs/spark.md): the DataFrame API gives us watermarking, window(), and
exactly-once-ish sink semantics via checkpointing "for free", instead of
hand-rolling stateful RDD bookkeeping.
"""

from __future__ import annotations
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, MapType,
)

from shared.normalize import normalize_event, enrich_event
from shared.detection import run_detectors
from shared.scoring import score_detection
from shared.correlation import dedupe_alerts, correlate_incidents
from shared.dedup import DuplicateFilter, deduplicate_events
from shared.data_quality import GLOBAL_TRACKER as DQ
from shared.schema import ValidationError

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "security-events")
KAFKA_DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "security-events-dlq")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/data/checkpoints")
PROCESSED_PATH = os.environ.get("PROCESSED_PATH", "/data/processed")
ALERTS_PATH = os.environ.get("ALERTS_PATH", "/data/alerts")
DLQ_PATH = os.environ.get("DLQ_PATH", "/data/dlq")
INCIDENTS_PATH = os.environ.get("INCIDENTS_PATH", "/data/incidents")

# Consumer group is set explicitly (Section 13) rather than left to default,
# so operators can see and reason about it in Kafka monitoring tools.
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "cyberstream-spark")

# Cross-batch duplicate filter (Section 62). foreachBatch always runs
# driver-side, so this single shared instance genuinely dedups across
# micro-batches, not just within one.
_DUP_FILTER = DuplicateFilter()
LATE_EVENT_THRESHOLD_SECONDS = 120  # matches the 2-minute watermark below

RAW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("event_type", StringType()),
    StructField("action", StringType()),
    StructField("status", StringType()),
    StructField("source_ip", StringType()),
    StructField("destination_ip", StringType()),
    StructField("source_port", IntegerType()),
    StructField("destination_port", IntegerType()),
    StructField("protocol", StringType()),
    StructField("username", StringType()),
    StructField("hostname", StringType()),
    StructField("bytes", LongType()),
    StructField("country", StringType()),
    StructField("metadata", MapType(StringType(), StringType())),
    StructField("is_synthetic_attack", StringType()),
    StructField("scenario", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CyberStream")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", CONSUMER_GROUP)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 200_000)  # backpressure control, Section 35
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_split(raw_df: DataFrame):
    """Parse JSON; route unparseable rows straight to the DLQ path."""
    parsed = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("timestamp").alias("kafka_ts"),
        F.from_json(F.col("value").cast("string"), RAW_EVENT_SCHEMA).alias("data"),
        F.col("value").cast("string").alias("raw_value"),
    )
    valid = parsed.filter(F.col("data").isNotNull()).select("data.*", "kafka_ts")
    malformed = parsed.filter(F.col("data").isNull()).select("raw_value", "kafka_ts")
    return valid, malformed


def process_batch(batch_df: DataFrame, batch_id: int):
    """
    foreachBatch handler: this is where normalization, enrichment, dedup,
    detection, scoring, and alert dedup actually execute per micro-batch
    (Section 14). Using foreachBatch (rather than pure streaming
    aggregation expressions) lets us reuse the exact same Python
    detection/scoring modules used by the local fallback runner and unit
    tests — one source of truth for business logic, executed at Spark
    scale.
    """
    if batch_df.rdd.isEmpty():
        return

    rows = [r.asDict(recursive=True) for r in batch_df.collect()]
    DQ.record_seen(len(rows))

    # --- late-event accounting (Section 61, 63) --------------------------
    # An event is "late" if its own event_time trails the time Kafka
    # actually received it by more than the watermark's grace period —
    # i.e. it took unusually long to be shipped/produced.
    for r in rows:
        try:
            evt_dt = datetime.fromisoformat(
                r["timestamp"][:-1] + "+00:00" if r["timestamp"].endswith("Z") else r["timestamp"]
            )
            kafka_dt = r.get("kafka_ts")
            if kafka_dt is not None:
                delay = (kafka_dt - evt_dt).total_seconds()
                if delay > LATE_EVENT_THRESHOLD_SECONDS:
                    DQ.record_late()
        except Exception:
            pass

    # --- duplicate filtering (Section 62) ---------------------------------
    rows, dup_count = deduplicate_events(rows, _DUP_FILTER)
    if dup_count:
        DQ.record_duplicate(dup_count)

    normalized, dlq = [], []
    for r in rows:
        try:
            n = normalize_event(r)
            normalized.append(enrich_event(n))
        except ValidationError as e:
            DQ.record_invalid()
            dlq.append({"original_event": r, "error_reason": str(e),
                        "timestamp": r.get("timestamp")})
        except Exception:
            DQ.record_error()

    if dlq:
        spark = batch_df.sparkSession
        spark.createDataFrame(dlq).write.mode("append").json(DLQ_PATH)

    if not normalized:
        return

    spark = batch_df.sparkSession
    norm_df = spark.createDataFrame(normalized)
    norm_df.write.mode("append").partitionBy("event_type").parquet(PROCESSED_PATH)

    # --- windowed detection over this micro-batch (Section 17-19) --------
    # run_detectors() internally buckets each rule at its own documented
    # window size (1 min / 5 min / 1 hour) via shared/windowing.py.
    from collections import defaultdict
    by_host = defaultdict(list)
    for e in normalized:
        by_host[e.get("hostname") or "unknown"].append(e)

    raw_alerts = []
    hosts_touched = set()
    for host, group in by_host.items():
        for detection in run_detectors(group):
            is_priv = any(e.get("is_privileged_account") for e in group)
            scored = score_detection(detection, is_privileged=is_priv,
                                      cross_host=len(by_host) > 3)
            raw_alerts.append({**detection, **scored, "status": "NEW"})
            hosts_touched.add(host)

    if raw_alerts:
        final_alerts = dedupe_alerts(raw_alerts)
        alerts_df = spark.createDataFrame(final_alerts, samplingRatio=1.0)
        alerts_df.write.mode("append").json(ALERTS_PATH)

        # --- incident correlation (Sections 20, 43) — 15-minute window --
        incidents = correlate_incidents(final_alerts)
        if incidents:
            incidents_df = spark.createDataFrame(incidents, samplingRatio=1.0)
            incidents_df.write.mode("append").json(INCIDENTS_PATH)

        print(f"[batch {batch_id}] {len(normalized)} events ({dup_count} dupes dropped) -> "
              f"{len(final_alerts)} deduped alerts, {len(incidents)} incidents, "
              f"across {len(hosts_touched)} hosts")
    else:
        print(f"[batch {batch_id}] {len(normalized)} events processed "
              f"({dup_count} dupes dropped), 0 alerts")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka_stream(spark)
    valid, malformed = parse_and_split(raw)

    # Event-time watermarking (Section 19): tolerate events arriving up to
    # 2 minutes late before Spark drops them from window state — necessary
    # because security telemetry from many hosts is never perfectly ordered.
    valid_with_time = valid.withColumn(
        "event_time", F.to_timestamp("timestamp")
    ).withWatermark("event_time", "2 minutes")

    query = (
        valid_with_time.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Malformed rows -> DLQ, independent stream so a parse failure never
    # blocks the main pipeline (Section 47).
    dlq_query = (
        malformed.writeStream
        .format("json")
        .option("path", DLQ_PATH)
        .option("checkpointLocation", CHECKPOINT_DIR + "-dlq")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()
    dlq_query.awaitTermination()


if __name__ == "__main__":
    main()
