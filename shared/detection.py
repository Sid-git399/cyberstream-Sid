"""
Detection Engine (Section 17) + Windowed Analysis (Section 18).

Design note on why this is written as plain aggregation-over-groups rather
than "one function per raw event": nearly every real detection here is a
*rate* or *count* phenomenon (many failed logins, many ports, many
destinations) — it cannot be decided from a single event. So each function
takes a list of already-grouped, already-windowed events (what Spark's
`groupBy(window(...), key)` produces per micro-batch) and decides whether
that group constitutes a detection.

This module is intentionally framework-agnostic: streaming/spark_job.py
calls these functions from a `foreachBatch` handler using Spark DataFrames
collected per group; tests and the local fallback call them directly with
plain Python lists/dicts.
"""

from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime

from .mitre import mitre_for

# ---------------------------------------------------------------------------
# Thresholds — deliberately explicit and documented, not magic numbers.
# In a real deployment these would be tunable per-tenant baselines
# (Section 23); here they are the "detection rule" configuration.
# ---------------------------------------------------------------------------

THRESHOLDS = {
    "brute_force_failed_logins": 8,      # per source+account within window
    "password_spray_distinct_accounts": 6,  # per source within window
    "port_scan_distinct_ports": 15,       # per source within window
    "data_transfer_anomaly_bytes": 50_000_000,  # per host within 1h window
    "dns_query_burst": 200,               # per source within window
}


def _dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts[:-1] + "+00:00" if ts.endswith("Z") else ts)


def _evidence(events: List[Dict[str, Any]], keys: List[str]) -> Dict[str, Any]:
    times = sorted(_dt(e["timestamp"]) for e in events)
    ev = {"event_count": len(events),
          "window_start": times[0].isoformat(),
          "window_end": times[-1].isoformat(),
          "sample_event_ids": [e["event_id"] for e in events[:5]]}
    for k in keys:
        vals = sorted({e.get(k) for e in events if e.get(k) is not None})
        ev[f"distinct_{k}"] = len(vals)
        ev[f"sample_{k}"] = vals[:5]
    return ev


def detect_brute_force(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Many failed logins, same source + same target account (1-min window)."""
    failed = [e for e in events
              if e["event_type"] == "authentication" and e["action"] == "login_failed"]
    if not failed:
        return None
    by_key = defaultdict(list)
    for e in failed:
        by_key[(e.get("source_ip"), e.get("username"))].append(e)

    for (src, user), grp in by_key.items():
        if len(grp) >= THRESHOLDS["brute_force_failed_logins"]:
            return {
                "detection_rule": "brute_force",
                "title": "Brute Force Authentication Activity",
                "source_ip": src, "username": user,
                "hostname": grp[0].get("hostname"),
                "evidence": _evidence(grp, ["hostname"]),
                "mitre_technique": mitre_for("brute_force"),
            }
    return None


def detect_password_spraying(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """One source, many distinct target accounts, failed logins (5-min window)."""
    failed = [e for e in events
              if e["event_type"] == "authentication" and e["action"] == "login_failed"]
    by_src = defaultdict(list)
    for e in failed:
        by_src[e.get("source_ip")].append(e)

    for src, grp in by_src.items():
        distinct_users = {e.get("username") for e in grp}
        if len(distinct_users) >= THRESHOLDS["password_spray_distinct_accounts"]:
            return {
                "detection_rule": "password_spraying",
                "title": "Password Spraying Activity",
                "source_ip": src, "username": None,
                "hostname": None,
                "evidence": _evidence(grp, ["username", "hostname"]),
                "mitre_technique": mitre_for("password_spraying"),
            }
    return None


def detect_port_scan(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """One source hitting many destination ports (5-min window)."""
    net = [e for e in events if e["event_type"] == "network"]
    by_src = defaultdict(list)
    for e in net:
        by_src[e.get("source_ip")].append(e)

    for src, grp in by_src.items():
        ports = {e.get("destination_port") for e in grp if e.get("destination_port")}
        if len(ports) >= THRESHOLDS["port_scan_distinct_ports"]:
            return {
                "detection_rule": "port_scan",
                "title": "Port Scanning Activity",
                "source_ip": src, "username": None,
                "hostname": grp[0].get("hostname"),
                "evidence": _evidence(grp, ["destination_port", "destination_ip"]),
                "mitre_technique": mitre_for("port_scan"),
            }
    return None


def detect_privilege_escalation(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    priv = [e for e in events if e["event_type"] == "privilege"
            and e["action"] in ("privilege_escalation", "group_modified")]
    if not priv:
        return None
    e = priv[0]
    return {
        "detection_rule": "privilege_escalation",
        "title": "Privilege Escalation Detected",
        "source_ip": e.get("source_ip"), "username": e.get("username"),
        "hostname": e.get("hostname"),
        "evidence": _evidence(priv, ["hostname", "username"]),
        "mitre_technique": mitre_for("privilege_escalation"),
    }


def detect_suspicious_process(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    procs = [e for e in events if e["event_type"] == "endpoint"
             and e["action"] == "process_created"
             and e.get("metadata", {}).get("suspicious")]
    if not procs:
        return None
    e = procs[0]
    return {
        "detection_rule": "suspicious_process",
        "title": "Suspicious Process Execution",
        "source_ip": None, "username": e.get("username"),
        "hostname": e.get("hostname"),
        "evidence": _evidence(procs, ["hostname", "username"]),
        "mitre_technique": mitre_for("suspicious_process"),
    }


def detect_data_transfer_anomaly(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Unusually high outbound bytes from one host (1h window)."""
    by_host = defaultdict(int)
    grp_by_host = defaultdict(list)
    for e in events:
        if e["event_type"] == "network" and not e.get("destination_internal", True):
            by_host[e.get("hostname")] += e.get("bytes", 0)
            grp_by_host[e.get("hostname")].append(e)

    for host, total in by_host.items():
        if total >= THRESHOLDS["data_transfer_anomaly_bytes"]:
            grp = grp_by_host[host]
            return {
                "detection_rule": "data_transfer_anomaly",
                "title": "Anomalous Outbound Data Transfer",
                "source_ip": grp[0].get("source_ip"), "username": None,
                "hostname": host,
                "evidence": {**_evidence(grp, ["destination_ip"]),
                             "total_bytes": total},
                "mitre_technique": mitre_for("data_transfer_anomaly"),
            }
    return None


def detect_dns_anomaly(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    dns = [e for e in events if e["event_type"] == "network" and e["action"] == "dns_query"]
    by_src = defaultdict(list)
    for e in dns:
        by_src[e.get("source_ip")].append(e)

    for src, grp in by_src.items():
        if len(grp) >= THRESHOLDS["dns_query_burst"]:
            return {
                "detection_rule": "dns_anomaly",
                "title": "Abnormal DNS Query Frequency",
                "source_ip": src, "username": None,
                "hostname": grp[0].get("hostname"),
                "evidence": _evidence(grp, ["hostname"]),
                "mitre_technique": mitre_for("dns_anomaly"),
            }
    return None


def detect_suspicious_authentication(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Authentication anomalies involving unusual timing or source (Section 17's
    'Suspicious Authentication' category — distinct from brute force/spraying,
    which are volume-based). This rule fires on *successful* logins that are
    individually unusual, not on failure volume:

      - a privileged account authenticating successfully during off-hours, or
      - a successful login whose source IP is external (not RFC1918) for an
        account that is not expected to authenticate from outside the network.

    Both signals rely only on fields enrich_event() already computes
    (is_off_hours, is_privileged_account, source_internal) — no external
    intel required (Section 16).
    """
    logins = [e for e in events if e["event_type"] == "authentication"
              and e["action"] == "login_success"]
    for e in logins:
        off_hours_priv = e.get("is_off_hours") and e.get("is_privileged_account")
        external_source = e.get("source_internal") is False
        if off_hours_priv or external_source:
            reason = []
            if off_hours_priv:
                reason.append("privileged account authenticated off-hours")
            if external_source:
                reason.append("authentication from external source IP")
            return {
                "detection_rule": "suspicious_authentication",
                "title": "Suspicious Authentication Pattern",
                "source_ip": e.get("source_ip"), "username": e.get("username"),
                "hostname": e.get("hostname"),
                "evidence": {**_evidence([e], ["hostname"]), "reasons": reason},
                "mitre_technique": mitre_for("suspicious_authentication"),
            }
    return None


DETECTORS = [
    detect_brute_force,
    detect_password_spraying,
    detect_port_scan,
    detect_privilege_escalation,
    detect_suspicious_process,
    detect_data_transfer_anomaly,
    detect_dns_anomaly,
    detect_suspicious_authentication,
]


DETECTOR_RULE_NAMES = {
    detect_brute_force: "brute_force",
    detect_password_spraying: "password_spraying",
    detect_port_scan: "port_scan",
    detect_privilege_escalation: "privilege_escalation",
    detect_suspicious_process: "suspicious_process",
    detect_data_transfer_anomaly: "data_transfer_anomaly",
    detect_dns_anomaly: "dns_anomaly",
    detect_suspicious_authentication: "suspicious_authentication",
}


def run_detectors(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Run every detector against a micro-batch, each at ITS OWN documented
    window size (Section 18): 1 min for brute force, 5 min for
    spraying/scanning/DNS, 1 hour for data-transfer, and instantaneous
    for single-event rules (privilege escalation, suspicious process,
    suspicious authentication). See shared/windowing.py for the bucketing
    mechanics and the documented reasoning for why this is Python-side
    bucketing on top of Spark's native watermark, not a native Spark
    window() aggregate.
    """
    from .windowing import run_windowed_detector

    results = []
    for fn, rule_name in DETECTOR_RULE_NAMES.items():
        results.extend(run_windowed_detector(fn, rule_name, events))
    return results
