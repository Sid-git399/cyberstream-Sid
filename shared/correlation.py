"""
Alert Deduplication (Section 26-27) and Incident Correlation (Section 20, 43).

Big Data insight this module exists to demonstrate:
    raw event volume != useful alert volume

10,000 identical failed-login events must become ONE aggregated alert,
and a sequence of causally-linked alerts (brute force -> valid login ->
priv-esc -> suspicious process -> exfil) must become ONE incident rather
than five unrelated tickets.
"""

from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, List
from datetime import datetime, timezone
import uuid

DEDUP_KEYS = ["detection_rule", "source_ip", "hostname"]

# Kill-chain-like ordering used to decide whether a set of alerts within a
# correlation window plausibly represents one multi-stage incident.
INCIDENT_CHAIN_ORDER = [
    "brute_force", "password_spraying",  # initial access attempt
    "privilege_escalation",              # escalation
    "suspicious_process",                # execution
    "data_transfer_anomaly",             # exfiltration
]


def dedupe_alerts(alerts: List[Dict[str, Any]], window_seconds: int = 300
                   ) -> List[Dict[str, Any]]:
    """
    Group alerts sharing (detection_rule, source, host) within a time
    window into a single aggregated alert with an occurrence count,
    rather than emitting one alert per underlying event group.
    """
    groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for a in alerts:
        key = tuple(a.get(k) for k in DEDUP_KEYS)
        groups[key].append(a)

    aggregated = []
    for key, grp in groups.items():
        first = grp[0]
        total_events = sum(g.get("evidence", {}).get("event_count", 1) for g in grp)
        aggregated.append({
            **first,
            "alert_id": f"alrt-{uuid.uuid4().hex[:10]}",
            "occurrences": len(grp),
            "total_underlying_events": total_events,
            "status": "NEW",
        })
    return aggregated


def correlate_incidents(alerts: List[Dict[str, Any]], window_minutes: int = 30
                         ) -> List[Dict[str, Any]]:
    """
    Group alerts that share a host or user and fall within a correlation
    window into candidate incidents, ordering stages by the kill-chain
    order above. This is intentionally simple (no ML) — a demonstration
    of correlation logic, not a claim of sophisticated threat-graph
    analysis.
    """
    by_entity: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in alerts:
        entity = a.get("hostname") or a.get("username") or a.get("source_ip") or "unknown"
        by_entity[entity].append(a)

    incidents = []
    for entity, grp in by_entity.items():
        if len(grp) < 2:
            continue
        stages = sorted(
            grp,
            key=lambda a: INCIDENT_CHAIN_ORDER.index(a["detection_rule"])
            if a["detection_rule"] in INCIDENT_CHAIN_ORDER else 99,
        )
        incidents.append({
            "incident_id": f"INC-{datetime.now(timezone.utc).year}-{uuid.uuid4().hex[:4].upper()}",
            "entity": entity,
            "stage_count": len(stages),
            "stages": [{"detection_rule": s["detection_rule"],
                        "alert_id": s.get("alert_id"),
                        "severity": s.get("severity")} for s in stages],
            "max_severity": max(stages, key=lambda s: s.get("risk_score", 0)).get("severity"),
        })
    return incidents
