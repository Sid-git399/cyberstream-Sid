"""
Windowed Analysis (Section 18).

Section 18 specifies FOUR distinct window sizes, each matched to a
detection category:
    1 minute  -> authentication bursts     (brute force)
    5 minutes -> scanning behaviour        (password spraying, port scan, DNS)
    15 minutes -> correlated multi-stage   (incident correlation)
    1 hour    -> data-transfer behaviour   (data transfer anomaly)

Design note (documented honestly in docs/streaming.md): Spark's native
`groupBy(window(col("event_time"), "1 minute"))` is the textbook way to
do this in pure Spark SQL, but CyberStream's detectors are Python
functions with branching logic (shared/detection.py), not aggregate
expressions. Reimplementing every detector as a Spark SQL aggregate
would sacrifice the "one detection codebase shared by Spark, the local
fallback runner, and the unit tests" property that keeps this whole
system testable outside a JVM.

So the actual window *tolerance* (how long Spark waits for late events
before finalizing a batch) is handled by `withWatermark` in
streaming/spark_job.py — that part IS native Spark. This module handles
the *bucket size specific to each rule*: it groups a micro-batch's
already-watermarked events into (window_start, group_key) buckets sized
per the table above, so e.g. the brute-force detector only ever sees
events from the same 1-minute bucket, not an entire 10-minute
micro-batch's worth of events.
"""

from __future__ import annotations
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

WINDOW_SECONDS = {
    "brute_force": 60,
    "password_spraying": 300,
    "port_scan": 300,
    "dns_anomaly": 300,
    "privilege_escalation": None,      # instantaneous — no windowing needed
    "suspicious_process": None,        # instantaneous
    "suspicious_authentication": None, # instantaneous
    "data_transfer_anomaly": 3600,
}

# Correlation across alerts (not raw events) uses a 15-minute window,
# applied separately in shared/correlation.py's caller — see
# streaming/spark_job.py::process_batch.
CORRELATION_WINDOW_SECONDS = 900


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts[:-1] + "+00:00" if ts.endswith("Z") else ts)


def _window_start(dt: datetime, window_seconds: int) -> datetime:
    epoch = dt.timestamp()
    bucket = int(epoch // window_seconds) * window_seconds
    return datetime.fromtimestamp(bucket, tz=dt.tzinfo)


def bucket_events(events: List[Dict[str, Any]], window_seconds: int,
                   group_key: Callable[[Dict[str, Any]], Any] = lambda e: e.get("hostname")
                   ) -> Dict[Tuple[Any, Any], List[Dict[str, Any]]]:
    """
    Group events into (window_start, group_key(event)) buckets of the
    given size, using each event's own event-time timestamp (not
    processing time — Section 18's explicit event-time requirement).
    """
    buckets: Dict[Tuple[Any, Any], List[Dict[str, Any]]] = defaultdict(list)
    for e in events:
        try:
            dt = _parse_ts(e["timestamp"])
        except Exception:
            continue  # unparseable timestamps were already routed to DLQ upstream
        w_start = _window_start(dt, window_seconds)
        buckets[(w_start, group_key(e))].append(e)
    return buckets


def run_windowed_detector(detector_fn: Callable, rule_name: str,
                            events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Run a single detector against events, bucketed at the window size
    documented for that rule. Rules with WINDOW_SECONDS[rule] == None
    run against the whole batch (they don't need time-bucketing —
    e.g. a single privilege_escalation event is meaningful on its own).
    """
    window_seconds = WINDOW_SECONDS.get(rule_name)
    if window_seconds is None:
        result = detector_fn(events)
        return [result] if result else []

    results = []
    buckets = bucket_events(events, window_seconds)
    for (w_start, key), bucket_events_list in buckets.items():
        r = detector_fn(bucket_events_list)
        if r:
            r["window_start"] = w_start.isoformat()
            r["window_size_seconds"] = window_seconds
            results.append(r)
    return results
